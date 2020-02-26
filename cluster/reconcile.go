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
	"reflect"
	"sort"
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

	err := c.configCluster()
	if err != nil {
		return err
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

func (c *Cluster) configCluster() error {
	need_reconfig, err := c.needReconfig()
	if err != nil {
		return err
	}

	if need_reconfig {
		return c.reconfig(c.runningMembers.ClientHostList(), c.runningMembers.ClusterConfig())
	}

	return nil
}

func (c *Cluster) needReconfig() (bool, error) {
	if c.runningMembers.Size() == 0 {
		return false, nil
	}
	actualConfig, err := zookeeperutil.GetClusterConfig(c.runningMembers.ClientHostList())
	if err != nil {
		c.logger.Info("Failed to get configure from zookeeper: %v", c.runningMembers.ClientHostList())
		return false, err
	}
	expectedConfig := c.runningMembers.ClusterConfig()

	sort.Strings(actualConfig)
	sort.Strings(expectedConfig)
	return !reflect.DeepEqual(expectedConfig, actualConfig), nil
}

func (c *Cluster) reconfig(hosts []string, desiredConfig []string) error {
	c.logger.Infoln("Reconfiguring ZK cluster")
	err := zookeeperutil.ReconfigureCluster(hosts, desiredConfig)
	if err != nil {
		c.logger.Infoln("Reconfigure error")
		return err
	}

	return nil
}

func (c *Cluster) resize() error {
	if c.runningMembers.Size() == c.cluster.Spec.Size {
		return nil
	}

	if c.runningMembers.Size() < c.cluster.Spec.Size {
		return c.scaleUp()
	}

	return c.scaleDown()
}

func (c *Cluster) scaleUp() error {
	c.cluster.Status.SetScalingUpCondition(c.runningMembers.Size(), c.cluster.Spec.Size)
	all := zookeeperutil.MemberSet{}
	all.Update(c.runningMembers)
	diff := c.cluster.Spec.Size - c.runningMembers.Size()
	newMembers := zookeeperutil.MemberSet{}
	for id := 0; id < diff; id++ {
		newMember := c.nextMember(all)
		newMembers.Add(newMember)
		all.Add(newMember)
	}
	all.Update(newMembers)
	return c.addMembers(newMembers, all)
}

func (c *Cluster) addMembers(newMembers zookeeperutil.MemberSet, newCluster zookeeperutil.MemberSet) error {
	wait := sync.WaitGroup{}
	wait.Add(newMembers.Size())

	errCh := make(chan error, newMembers.Size())
	for _, member := range newMembers {
		go func(newMember *zookeeperutil.Member, newCluster zookeeperutil.MemberSet) {
			defer wait.Done()
			err := c.addOneMember(newMember, newCluster)
			if err != nil {
				errCh <- err
			}
		}(member, newCluster)
	}
	wait.Wait()

	select {
	case err := <-errCh:
		// all errors have been reported before, we only need to inform the controller that there was an error and it should re-try this job once more next time.
		if err != nil {
			return err
		}
	default:
	}

	return nil
}

// TODO: use configmap to avoid the parameter "cluster"
func (c *Cluster) addOneMember(m *zookeeperutil.Member, cluster zookeeperutil.MemberSet) error {
	_, err := c.createPod(m, cluster)
	if err != nil {
		return fmt.Errorf("fail to create member's pod (%s): %v", m.Name, err)
	}

	c.logger.Infof("added member (%s)", m.Name)
	_, err = c.eventsCli.Create(k8sutil.NewMemberAddEvent(m.Name, c.cluster))
	if err != nil {
		c.logger.Errorf("failed to create new member add event: %v", err)
	}
	return nil
}

func (c *Cluster) scaleDown() error {
	c.cluster.Status.SetScalingDownCondition(c.runningMembers.Size(), c.cluster.Spec.Size)
	diff := c.runningMembers.Size() - c.cluster.Spec.Size

	membersToRemove := zookeeperutil.MemberSet{}
	copy := zookeeperutil.MemberSet{}
	copy.Update(c.runningMembers)
	for i := 0; i < diff; i++ {
		memberToRemove := copy.PickOneToRemove()
		membersToRemove.Add(memberToRemove)
		copy.Remove(memberToRemove.Name)
	}

	newCluster := c.runningMembers.Diff(membersToRemove)
	return c.removeMembers(membersToRemove, newCluster)
}

func (c *Cluster) removeMembers(membersToMove zookeeperutil.MemberSet, newCluster zookeeperutil.MemberSet) error {
	err := c.reconfig(newCluster.ClientHostList(), newCluster.ClusterConfig())
	if err != nil {
		return err
	}

	errCh := make(chan error, membersToMove.Size())
	wait := sync.WaitGroup{}
	wait.Add(membersToMove.Size())
	locker := sync.Mutex{}
	for _, m := range membersToMove {
		go func(member *zookeeperutil.Member) {
			defer wait.Done()
			err := c.removeOneMember(member, &locker)
			if err != nil {
				errCh <- err
			}
		}(m)
	}
	wait.Wait()

	select {
	case err := <-errCh:
		// all errors have been reported before, we only need to inform the controller that there was an error and it should re-try this job once more next time.
		if err != nil {
			return err
		}
	default:
	}

	return nil
}

func (c *Cluster) replaceOneDeadMember(toReplace *zookeeperutil.Member, cluster zookeeperutil.MemberSet, locker sync.Locker) error {
	c.logger.Infof("replacing dead member %q", toReplace.Name)
	_, err := c.eventsCli.Create(k8sutil.ReplacingDeadMemberEvent(toReplace.Name, c.cluster))
	if err != nil {
		c.logger.Errorf("failed to create replacing dead member event: %v", err)
	}

	err = c.removeOneMember(toReplace, locker)
	if err != nil {
		return err
	}

	return c.addOneMember(toReplace, cluster)
}

// Remember to reconfig the zookeeper cluster before invoking this function
func (c *Cluster) removeOneMember(m *zookeeperutil.Member, locker sync.Locker) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("remove member (%s) failed: %v", m.Name, err)
		}
	}()

	if err := c.removePod(m.Name); err != nil {
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

	if locker != nil {
		locker.Lock()
	}
	c.runningMembers.Remove(m.Name)
	if locker != nil {
		locker.Unlock()
	}
	_, err = c.eventsCli.Create(k8sutil.MemberRemoveEvent(m.Name, c.cluster))
	if err != nil {
		c.logger.Errorf("failed to create remove member event: %v", err)
	}

	c.logger.Infof("removed member (%v) with ID (%d)", m.Name, m.ID())
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
