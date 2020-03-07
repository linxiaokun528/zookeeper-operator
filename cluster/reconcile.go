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
)

// ErrLostQuorum indicates that the zookeeper zkCR lost its quorum.
var ErrLostQuorum = errors.New("lost quorum")

// Reconcile reconciles zkCR current state to desired state specified by spec.
// - it tries to Reconcile the zkCR to desired size.
// - if the zkCR needs for upgrade, it tries to upgrade old member one by one.
func (c *Cluster) Reconcile() error {
	c.logger.Infoln("Start reconciling")
	defer c.logger.Infoln("Finish reconciling")

	defer func() {
		// TODO: it's not correct
		c.zkCR.Status.Size = c.zkStatus.GetRunningMembers().Size()
	}()

	err := c.configCluster()
	if err != nil {
		return err
	}

	// If not enough are running or membership size != spec size then maybe resize
	if c.zkStatus.GetRunningMembers().Size() != c.zkCR.Spec.Size {
		return c.resize()
	}
	c.zkCR.Status.ClearCondition(api.ClusterConditionScaling)

	// TODO: @MDF: Try and upgrade the leader last, that way we don't bounce it around repeatedly
	if c.needUpgrade() {
		c.zkCR.Status.UpgradeVersionTo(c.zkCR.Spec.Version)

		m := c.pickOneOldMember()
		return c.upgradeOneMember(m.Name())
	}
	c.zkCR.Status.ClearCondition(api.ClusterConditionUpgrading)

	c.zkCR.Status.SetVersion(c.zkCR.Spec.Version)
	c.zkCR.Status.SetReadyCondition()

	return nil
}

func (c *Cluster) configCluster() error {
	need_reconfig, err := c.needReconfig()
	if err != nil {
		return err
	}

	if need_reconfig {
		return c.reconfig(c.zkStatus.GetRunningMembers().GetClientHosts(),
			c.zkStatus.GetRunningMembers().GetClusterConfig())
	}

	return nil
}

func (c *Cluster) needReconfig() (bool, error) {
	if c.zkStatus.GetRunningMembers().Size() == 0 {
		return false, nil
	}
	actualConfig, err := zookeeperutil.GetClusterConfig(c.zkStatus.GetRunningMembers().GetClientHosts())
	if err != nil {
		c.logger.Info("Failed to get configure from zookeeper: %v",
			c.zkStatus.GetRunningMembers().GetClientHosts())
		return false, err
	}
	expectedConfig := c.zkStatus.GetRunningMembers().GetClusterConfig()

	sort.Strings(actualConfig)
	sort.Strings(expectedConfig)

	// TODO: What if we successfully reconfig but fail to delete pods?
	return !reflect.DeepEqual(expectedConfig, actualConfig), nil
}

func (c *Cluster) reconfig(hosts []string, desiredConfig []string) error {
	c.logger.Infoln("Reconfiguring zookeeper cluster", c.zkCR.Name)
	err := zookeeperutil.ReconfigureCluster(hosts, desiredConfig)
	if err != nil {
		c.logger.Infoln("Reconfigure error")
		return err
	}

	return nil
}

func (c *Cluster) resize() error {
	if c.zkStatus.GetRunningMembers().Size() == c.zkCR.Spec.Size {
		return nil
	}

	if c.zkStatus.GetRunningMembers().Size() < c.zkCR.Spec.Size {
		return c.scaleUp()
	}

	return c.scaleDown()
}

func (c *Cluster) scaleUp() error {
	c.zkCR.Status.SetScalingUpCondition(c.zkStatus.GetRunningMembers().Size(), c.zkCR.Spec.Size)
	diff := c.zkCR.Spec.Size - c.zkStatus.GetRunningMembers().Size()
	newMembers := c.zkStatus.AddMembers(diff)
	all := c.zkStatus.GetRunningMembers().Copy()
	all.Update(newMembers)

	wait := sync.WaitGroup{}
	wait.Add(newMembers.Size())
	errCh := make(chan error)

	for _, member := range newMembers.GetElements() {
		go func(newMember *zookeeperutil.Member, allClusterMembers *zookeeperutil.Members) {
			defer wait.Done()
			err := c.addOneMember(newMember, allClusterMembers)
			if err != nil {
				errCh <- err
				c.locker.Lock()
				c.zkStatus.GetUnreadyMembers().Remove(member.ID())
				c.locker.Unlock()
			}
		}(member, all)
	}
	wait.Wait()

	select {
	case err := <-errCh:
		// all errors have been reported before, we only need to inform the controller that there was an error and it should re-try this once more next time.
		if err != nil {
			return err
		}
	default:
	}

	return nil
}

// The member added will be in c.zkStatus.unready
func (c *Cluster) addOneMember(m *zookeeperutil.Member, allClusterMembers *zookeeperutil.Members) error {
	_, err := c.createPod(m, allClusterMembers)
	if err != nil {
		return fmt.Errorf("fail to create member's pod (%s): %v", m.Name(), err)
	}

	c.logger.Infof("added member (%s)", m.Name())
	_, err = c.client.Event().Create(k8sutil.NewMemberAddEvent(m.Name(), c.zkCR))
	if err != nil {
		c.logger.Errorf("failed to create new member add event: %v", err)
	}
	return nil
}

func (c *Cluster) scaleDown() error {
	c.zkCR.Status.SetScalingDownCondition(c.zkStatus.GetRunningMembers().Size(), c.zkCR.Spec.Size)
	diff := c.zkStatus.GetRunningMembers().Size() - c.zkCR.Spec.Size

	membersToRemove := c.zkStatus.RemoveMembers(diff)

	err := c.reconfig(c.zkStatus.GetRunningMembers().GetClientHosts(), c.zkStatus.GetRunningMembers().GetClusterConfig())
	if err != nil {
		c.zkStatus.GetRunningMembers().Update(membersToRemove)
		return err
	}

	errCh := make(chan error)
	wait := sync.WaitGroup{}
	wait.Add(membersToRemove.Size())
	for _, m := range membersToRemove.GetElements() {
		go func(member *zookeeperutil.Member) {
			defer wait.Done()
			err := c.removeOneMember(member)
			if err != nil {
				errCh <- err
				c.locker.Lock()
				c.zkStatus.GetRunningMembers().Add(member)
				c.locker.Unlock()
			}
		}(m)
	}
	wait.Wait()

	select {
	case err := <-errCh:
		// all errors have been reported before, we only need to inform the controller that there was an error and it should re-try this once more next time.
		if err != nil {
			return err
		}
	default:
	}

	return nil
}

func (c *Cluster) ReplaceStoppedMembers() error {
	c.logger.Infof("Some members are stopped: (%v)", c.zkStatus.GetStoppedMembers().GetMemberNames())

	all := c.zkStatus.GetRunningMembers().Copy()
	all.Update(c.zkStatus.GetStoppedMembers())

	// TODO: we have a pattern: wait-for-error. Need to refactor this.
	wait := sync.WaitGroup{}
	wait.Add(c.zkStatus.GetStoppedMembers().Size())
	errCh := make(chan error)
	for _, dead_member := range c.zkStatus.GetStoppedMembers().GetElements() {
		go func() {
			defer wait.Done()
			err := c.replaceOneStoppedMember(dead_member, all)
			if err != nil {
				errCh <- err
			}
		}()
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

func (c *Cluster) replaceOneStoppedMember(toReplace *zookeeperutil.Member, cluster *zookeeperutil.Members) error {
	c.logger.Infof("replacing dead member %q", toReplace.Name)

	err := c.removeOneMember(toReplace)
	if err != nil {
		return err
	}
	c.locker.Lock()
	c.zkStatus.GetStoppedMembers().Remove(toReplace.ID())
	c.locker.Unlock()

	err = c.addOneMember(toReplace, cluster)
	if err != nil {
		return err
	}
	c.zkStatus.GetUnreadyMembers().Add(toReplace)

	_, err = c.client.Event().Create(k8sutil.ReplacingDeadMemberEvent(toReplace.Name(), c.zkCR))
	if err != nil {
		c.logger.Errorf("failed to create replacing dead member event: %v", err)
	}

	return nil
}

// Remember to reconfig the zookeeper zkCR before invoking this function
func (c *Cluster) removeOneMember(m *zookeeperutil.Member) (err error) {
	if err := c.removePod(m.Name()); err != nil {
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

	_, err = c.client.Event().Create(k8sutil.MemberRemoveEvent(m.Name(), c.zkCR))
	if err != nil {
		c.logger.Errorf("failed to create remove member event: %v", err)
	}

	c.logger.Infof("removed member (%v) with ID (%d)", m.Name, m.ID())
	return nil
}

func (c *Cluster) needUpgrade() bool {
	return c.zkStatus.GetRunningMembers().Size() == c.zkCR.Spec.Size && c.pickOneOldMember() != nil
}

func (c *Cluster) pickOneOldMember() *zookeeperutil.Member {
	for _, member := range c.zkStatus.GetRunningMembers().GetElements() {
		if member.Version() == c.zkCR.Spec.Version {
			continue
		}
		return member
	}
	return nil
}
