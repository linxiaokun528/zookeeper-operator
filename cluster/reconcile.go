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
		c.cluster.Status.Size = c.Members.Size()
	}()

	sp := c.cluster.Spec
	running := PodsToMemberSet(c.runningPods)
	// Reconfigure required if running == membership but clusterConfig != membership
	if len(c.readyPods) > 0 {
		clientHosts := c.Members.ClientHostList()
		newconfig := PodsToMemberSet(append(c.runningPods, c.readyPods...)).ClusterConfig()

		c.logger.Infoln("Reconfiguring ZK cluster")
		config, err := zookeeperutil.ReconfigureCluster(clientHosts, newconfig)
		if err != nil {
			c.logger.Infoln("Reconfigure error")
			return err
		}

		new_ready := []*v1.Pod{}
		for _, pod := range c.readyPods {
			delete(pod.Labels, "waiting")
			new_pod, err := c.config.KubeCli.CoreV1().Pods(c.cluster.Namespace).Update(pod)
			if err != nil {
				c.logger.Warn("Error happended when updating pod %v: %v", pod, err)
				new_ready = append(new_ready, pod)
			} else {
				c.runningPods = append(c.runningPods, new_pod)
			}
		}
		c.readyPods = new_ready
		c.logger.Infoln(fmt.Sprintf("New ZK config: %s", config))

		return nil

	}
	// If not enough are running or membership size != spec size then maybe resize
	if !running.IsEqual(c.Members) || c.Members.Size() != sp.Size {
		return c.reconcileMembers(running)
	}
	c.cluster.Status.ClearCondition(api.ClusterConditionScaling)

	// TODO: @MDF: Try and upgrade the leader last, that way we don't bounce it around repeatedly
	if needUpgrade(c.runningPods, sp) {
		c.cluster.Status.UpgradeVersionTo(sp.Version)

		m := pickOneOldMember(c.runningPods, sp.Version)
		return c.upgradeOneMember(m.Name)
	}
	c.cluster.Status.ClearCondition(api.ClusterConditionUpgrading)

	c.cluster.Status.SetVersion(sp.Version)
	c.cluster.Status.SetReadyCondition()

	return nil
}

// reconcileMembers reconciles
// - running pods on k8s and cluster membership
// - cluster membership and expected size of zookeeper cluster
// Steps:
// 1. Remove all pods from running set that does not belong to member set.
// 2. L consist of remaining pods of runnings
// 3. If L = Members, the current state matches the membership state. END.
// 4. If len(L) < len(Members)/2 + 1, return quorum lost error.
// 5. Add one missing member. END.
func (c *Cluster) reconcileMembers(running zookeeperutil.MemberSet) error {
	c.logger.Infof("running Members: %s", running)
	c.logger.Infof("cluster membership: %s", c.Members)

	unknownMembers := running.Diff(c.Members)
	//if unknownMembers.Size() > 0 {
	//	c.logger.Infof("removing unexpected pods: %v", unknownMembers)
	//	for _, m := range unknownMembers {
	//		if err := c.removePod(m.Name, true); err != nil {
	//			return err
	//		}
	//	}
	//}
	L := running.Diff(unknownMembers)

	if L.Size() == c.Members.Size() {
		return c.resize()
	}

	if L.Size() < c.Members.Size()/2+1 {
		return ErrLostQuorum
	}

	c.logger.Infof("removing one dead member")
	// remove dead Members that doesn't have any running pods before doing resizing.
	return c.replaceDeadMember(c.Members.Diff(L).PickOne())
}

func (c *Cluster) resize() error {
	if c.Members.Size() == c.cluster.Spec.Size {
		return nil
	}

	if c.Members.Size() < c.cluster.Spec.Size {
		// TODO: @MDF: Perhaps we want to add 2x at a time if we currently have an odd membership, we should be able to do that
		return c.addOneMember()
	}

	return c.removeOneMember()
}

func (c *Cluster) addOneMember() error {
	c.cluster.Status.SetScalingUpCondition(c.Members.Size(), c.cluster.Spec.Size)
	newMember := c.newMember()
	return c.addMember(newMember, "new")
}

func (c *Cluster) addMember(toAdd *zookeeperutil.Member, state string) error {
	existingCluster := c.Members.ClusterConfig()
	c.Members.Add(toAdd)

	pod, err := c.createPod(existingCluster, toAdd, state)
	if err != nil {
		return fmt.Errorf("fail to create member's pod (%s): %v", toAdd.Name, err)
	}
	if k8sutil.IsPodReady(pod) {
		c.readyPods = append(c.readyPods, pod)
	}
	c.logger.Infof("added member (%s)", toAdd.Name)
	_, err = c.eventsCli.Create(k8sutil.NewMemberAddEvent(toAdd.Name, c.cluster))
	if err != nil {
		c.logger.Errorf("failed to create new member add event: %v", err)
	}
	return nil
}

func (c *Cluster) removeOneMember() error {
	c.cluster.Status.SetScalingDownCondition(c.Members.Size(), c.cluster.Spec.Size)

	// TODO: @MDF: Be smarter, don't pick the leader
	return c.removeMember(c.Members.PickOne(), true)
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

	// Remove the member from the MemberSet
	c.Members.Remove(toRemove.Name)

	if isScalingEvent {
		// Perform a cluster reconfigure dropping the node to be removed
		_, err = zookeeperutil.ReconfigureCluster(c.Members.ClientHostList(), c.Members.ClusterConfig())
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

func needUpgrade(pods []*v1.Pod, cs api.ClusterSpec) bool {
	return len(pods) == cs.Size && pickOneOldMember(pods, cs.Version) != nil
}

func pickOneOldMember(pods []*v1.Pod, newVersion string) *zookeeperutil.Member {
	for _, pod := range pods {
		if k8sutil.GetZookeeperVersion(pod) == newVersion {
			continue
		}
		return &zookeeperutil.Member{Name: pod.Name, Namespace: pod.Namespace}
	}
	return nil
}
