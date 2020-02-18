// Copyright 2018 The zookeeper-operator Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"errors"
	"fmt"
	"sync"
	api "zookeeper-operator/apis/zookeeper/v1alpha1"
	"zookeeper-operator/util/k8sutil"
	"zookeeper-operator/util/zookeeperutil"

	"k8s.io/api/core/v1"
)

// ErrLostQuorum indicates that the zookeeper cluster lost its quorum.
var ErrLostQuorum = errors.New("lost quorum")

// Reconcile reconciles cluster current state to desired state specified by spec.
// - it tries to Reconcile the cluster to desired size.
// - if the cluster needs for upgrade, it tries to upgrade old member one by one.
func (c *Cluster) Reconcile() error {
	c.logger.Infoln("Start reconciling")
	defer c.logger.Infoln("Finish reconciling")

	defer func() {
		c.cluster.Status.Size = c.runningMembers.Size()
	}()

	// Reconfigure required if running == membership but clusterConfig != membership
	if c.readyMembers.Size() > 0 {
		all := zookeeperutil.MemberSet{}
		all.Update(c.runningMembers)
		all.Update(c.readyMembers)

		c.logger.Infoln("Reconfiguring ZK cluster")
		config, err := zookeeperutil.ReconfigureCluster(all.ClientHostList(), all.ClusterConfig())
		if err != nil {
			c.logger.Infoln("Reconfigure error")
			return err
		}

		c.logger.Infoln(fmt.Sprintf("New ZK config: %s", config))

		wait := sync.WaitGroup{}
		nbPods := c.readyMembers.Size()
		wait.Add(nbPods)
		for _, member := range c.readyMembers {
			go func() {
				delete(member.Annotations, "waiting")
				defer wait.Done()
				pod, err := c.config.KubeCli.CoreV1().Pods(c.cluster.Namespace).Update((*v1.Pod)(member))
				if err != nil {
					c.logger.Warn("Error happended when updating pod %v: %v", member, err)
				} else {
					delete(c.readyMembers, member.Name)
					c.runningMembers[member.Name] = (*zookeeperutil.Member)(pod)
				}
			}()
		}
		wait.Wait()

		return nil
	}
	// If not enough are running or membership size != spec size then maybe resize
	if c.runningMembers.Size() != c.cluster.Spec.Size {
		return c.resize()
	}
	c.cluster.Status.ClearCondition(api.ClusterConditionScaling)

	// TODO: @MDF: Try and upgrade the leader last, that way we don't bounce it around repeatedly
	if c.needUpgrade() {
		c.cluster.Status.UpgradeVersionTo(c.cluster.Spec.Version)

		m := c.pickOneOldMember()
		return c.upgradeOneMember(m.Name)
	}
	c.cluster.Status.ClearCondition(api.ClusterConditionUpgrading)

	c.cluster.Status.SetVersion(c.cluster.Spec.Version)
	c.cluster.Status.SetReadyCondition()

	return nil
}

func (c *Cluster) resize() error {
	if c.runningMembers.Size() == c.cluster.Spec.Size {
		return nil
	}

	if c.runningMembers.Size() < c.cluster.Spec.Size {
		// TODO: @MDF: Perhaps we want to add 2x at a time if we currently have an odd membership, we should be able to do that
		return c.addOneMember()
	}

	return c.removeOneMember()
}

func (c *Cluster) addOneMember() error {
	c.cluster.Status.SetScalingUpCondition(c.runningMembers.Size(), c.cluster.Spec.Size)
	newMember := c.nextMember()
	return c.addMember(newMember, "new")
}

func (c *Cluster) addMember(toAdd *zookeeperutil.Member, state string) error {
	existingCluster := c.runningMembers.ClusterConfig()

	pod, err := c.createPod(existingCluster, toAdd, state)
	if err != nil {
		return fmt.Errorf("fail to create member's pod (%s): %v", toAdd.Name, err)
	}
	if k8sutil.IsPodReady(pod) {
		c.readyMembers[pod.Name] = (*zookeeperutil.Member)(pod)
	}
	c.logger.Infof("added member (%s)", toAdd.Name)
	_, err = c.eventsCli.Create(k8sutil.NewMemberAddEvent(toAdd.Name, c.cluster))
	if err != nil {
		c.logger.Errorf("failed to create new member add event: %v", err)
	}
	return nil
}

func (c *Cluster) removeOneMember() error {
	c.cluster.Status.SetScalingDownCondition(c.runningMembers.Size(), c.cluster.Spec.Size)

	// TODO: @MDF: Be smarter, don't pick the leader
	return c.removeMember(c.runningMembers.PickOne(), true)
}

func (c *Cluster) replaceDeadMember(toReplace *zookeeperutil.Member) error {
	c.logger.Infof("replacing dead member %q", toReplace.Name)
	_, err := c.eventsCli.Create(k8sutil.ReplacingDeadMemberEvent(toReplace.Name, c.cluster))
	if err != nil {
		c.logger.Errorf("failed to create replacing dead member event: %v", err)
	}

	err = c.removeMember(toReplace, false)
	if err != nil {
		return err
	}

	return c.addMember(toReplace, "replacement")
}

func (c *Cluster) removeMember(toRemove *zookeeperutil.Member, isScalingEvent bool) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("remove member (%s) failed: %v", toRemove.Name, err)
		}
	}()

	if isScalingEvent {
		// Perform a cluster reconfigure dropping the node to be removed
		_, err = zookeeperutil.ReconfigureCluster(c.runningMembers.ClientHostList(), c.runningMembers.ClusterConfig())
		if err != nil {
			c.logger.Errorf("failed to reconfigure remove member from cluster: %v", err)
		}
	}

	_, err = c.eventsCli.Create(k8sutil.MemberRemoveEvent(toRemove.Name, c.cluster))
	if err != nil {
		c.logger.Errorf("failed to create remove member event: %v", err)
	}
	// We can wait if it's a scaling event, if this is a recovery then force delete
	if err := c.removePod(toRemove.Name, isScalingEvent); err != nil {
		return err
	}
	// TODO: @MDF: Add PV support
	/*
		if c.isPodPVEnabled() {
			err = c.removePVC(k8sutil.PVCNameFromMember(toRemove.Name))
			if err != nil {
				return err
			}
		}
	*/

	c.runningMembers.Remove(toRemove.Name)
	c.logger.Infof("removed member (%v) with ID (%d)", toRemove.Name, toRemove.ID)
	return nil
}

func (c *Cluster) removePVC(pvcName string) error {
	err := c.config.KubeCli.CoreV1().PersistentVolumeClaims(c.cluster.Namespace).Delete(pvcName, nil)
	if err != nil && !k8sutil.IsKubernetesResourceNotFoundError(err) {
		return fmt.Errorf("remove pvc (%s) failed: %v", pvcName, err)
	}
	return nil
}

func (c *Cluster) needUpgrade() bool {
	return c.runningMembers.Size() == c.cluster.Spec.Size && c.pickOneOldMember() != nil
}

func (c *Cluster) pickOneOldMember() *zookeeperutil.Member {
	for _, member := range c.runningMembers {
		if k8sutil.GetZookeeperVersion((*v1.Pod)(member)) == c.cluster.Spec.Version {
			continue
		}
		return member
	}
	return nil
}
