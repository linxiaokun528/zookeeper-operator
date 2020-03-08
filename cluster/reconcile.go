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
		c.zkCR.Status.Size = c.zkCR.Status.Members.Running.Size()
	}()

	err := c.configCluster()
	if err != nil {
		return err
	}

	// If not enough are running or membership size != spec size then maybe resize
	if c.zkCR.Status.Members.Running.Size() != c.zkCR.Spec.Size {
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
		return c.reconfig(c.zkCR.Status.Members.Running.GetClientHosts(),
			c.zkCR.Status.Members.Running.GetClusterConfig())
	}

	return nil
}

func (c *Cluster) needReconfig() (bool, error) {
	if c.zkCR.Status.Members.Running.Size() == 0 {
		return false, nil
	}
	actualConfig, err := zookeeperutil.GetClusterConfig(c.zkCR.Status.Members.Running.GetClientHosts())
	if err != nil {
		c.logger.Info("Failed to get configure from zookeeper: %v",
			c.zkCR.Status.Members.Running.GetClientHosts())
		return false, err
	}
	expectedConfig := c.zkCR.Status.Members.Running.GetClusterConfig()

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
	if c.zkCR.Status.Members.Running.Size() == c.zkCR.Spec.Size {
		return nil
	}

	if c.zkCR.Status.Members.Running.Size() < c.zkCR.Spec.Size {
		return c.scaleUp()
	}

	return c.scaleDown()
}

func (c *Cluster) scaleUp() error {
	c.zkCR.Status.SetScalingUpCondition(c.zkCR.Status.Members.Running.Size(), c.zkCR.Spec.Size)
	diff := c.zkCR.Spec.Size - c.zkCR.Status.Members.Running.Size()
	newMembers := c.zkCR.Status.Members.AddMembers(diff)
	all := c.zkCR.Status.Members.Running.Copy()
	all.Update(newMembers)

	wait := sync.WaitGroup{}
	wait.Add(newMembers.Size())
	errCh := make(chan error)

	for _, member := range newMembers.GetElements() {
		go func(newMember *api.Member, allClusterMembers *api.Members) {
			defer wait.Done()
			err := c.addOneMember(newMember, allClusterMembers)
			if err != nil {
				errCh <- err
				c.locker.Lock()
				c.zkCR.Status.Members.Unready.Remove(member.ID())
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

// The member added will be in c.zkCR.Status.Members.unready
func (c *Cluster) addOneMember(m *api.Member, allClusterMembers *api.Members) error {
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
	c.zkCR.Status.SetScalingDownCondition(c.zkCR.Status.Members.Running.Size(), c.zkCR.Spec.Size)
	diff := c.zkCR.Status.Members.Running.Size() - c.zkCR.Spec.Size

	membersToRemove := c.zkCR.Status.Members.RemoveMembers(diff)

	err := c.reconfig(c.zkCR.Status.Members.Running.GetClientHosts(), c.zkCR.Status.Members.Running.GetClusterConfig())
	if err != nil {
		c.zkCR.Status.Members.Running.Update(membersToRemove)
		return err
	}

	errCh := make(chan error)
	wait := sync.WaitGroup{}
	wait.Add(membersToRemove.Size())
	for _, m := range membersToRemove.GetElements() {
		go func(member *api.Member) {
			defer wait.Done()
			err := c.removeOneMember(member)
			if err != nil {
				errCh <- err
				c.locker.Lock()
				c.zkCR.Status.Members.Running.Add(member)
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
	c.logger.Infof("Some members are stopped: (%v)", c.zkCR.Status.Members.Stopped.GetMemberNames())

	all := c.zkCR.Status.Members.Running.Copy()
	all.Update(c.zkCR.Status.Members.Stopped)

	// TODO: we have a pattern: wait-for-error. Need to refactor this.
	wait := sync.WaitGroup{}
	wait.Add(c.zkCR.Status.Members.Stopped.Size())
	errCh := make(chan error)
	for _, dead_member := range c.zkCR.Status.Members.Stopped.GetElements() {
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

func (c *Cluster) replaceOneStoppedMember(toReplace *api.Member, cluster *api.Members) error {
	c.logger.Infof("replacing dead member %q", toReplace.Name)

	err := c.removeOneMember(toReplace)
	if err != nil {
		return err
	}
	c.locker.Lock()
	c.zkCR.Status.Members.Stopped.Remove(toReplace.ID())
	c.locker.Unlock()

	err = c.addOneMember(toReplace, cluster)
	if err != nil {
		return err
	}
	c.zkCR.Status.Members.Unready.Add(toReplace)

	_, err = c.client.Event().Create(k8sutil.ReplacingDeadMemberEvent(toReplace.Name(), c.zkCR))
	if err != nil {
		c.logger.Errorf("failed to create replacing dead member event: %v", err)
	}

	return nil
}

// Remember to reconfig the zookeeper zkCR before invoking this function
func (c *Cluster) removeOneMember(m *api.Member) (err error) {
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
	return c.zkCR.Status.Members.Running.Size() == c.zkCR.Spec.Size && c.pickOneOldMember() != nil
}

func (c *Cluster) pickOneOldMember() *api.Member {
	for _, member := range c.zkCR.Status.Members.Running.GetElements() {
		if member.Version() == c.zkCR.Spec.Version {
			continue
		}
		return member
	}
	return nil
}
