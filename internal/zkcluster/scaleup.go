package zkcluster

import (
	"fmt"
	"sync"

	"k8s.io/klog"

	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	"zookeeper-operator/pkg/errors"
)

func (c *Cluster) beginScaleUp() (err error) {
	klog.Infof("Scaling up zookeeper cluster %v(current cluster size: %d, desired cluster size: %d)...",
		c.zkCR.GetFullName(), c.zkCR.Status.Members.Running.Size(), c.zkCR.Spec.Size)

	diff := c.zkCR.Spec.Size - c.zkCR.Status.Members.Running.Size()
	newMembers := c.zkCR.Status.Members.AddMembers(diff)
	defer func() {
		if err == nil {
			klog.Infof("New members are added into zookeeper cluster %v successfully: %v",
				c.zkCR.GetFullName(), newMembers.GetMemberNames())
		}
	}()

	wait := sync.WaitGroup{}
	wait.Add(newMembers.Size())
	errCh := make(chan error, newMembers.Size())

	lastCommittedMembers := &c.zkCR.Status.Members.Running
	if c.zkCR.Status.Members.Running.Size() == 0 {
		lastCommittedMembers = newMembers
	}

	for _, member := range newMembers.GetElements() {
		go func(newMember *api.Member, lastCommittedMembers *api.Members) {
			defer wait.Done()
			err := c.addOneMember(newMember, lastCommittedMembers)
			if err != nil {
				errCh <- err
				c.locker.Lock()
				c.zkCR.Status.Members.Unready.Remove(member.ID())
				c.locker.Unlock()
			}
		}(member, lastCommittedMembers)
	}
	wait.Wait()
	close(errCh)

	return errors.NewCompoundedErrorFromChan(errCh)
}

func (c *Cluster) finishScaleUp() (err error) {
	defer func() {
		if err == nil {
			klog.Infof("Zookeeper cluster %v scaled up successfully", c.zkCR.GetFullName())
		}
	}()
	err = c.reconfig()
	if err == nil {
		c.zkCR.Status.Members.Running.Update(&c.zkCR.Status.Members.Ready)
		c.zkCR.Status.Members.Ready = api.Members{}
	}
	return err
}

// The member added will be in c.zkCR.Status.Members.unready
func (c *Cluster) addOneMember(m *api.Member, lastCommittedMembers *api.Members) error {
	allClusterMembers := lastCommittedMembers.Copy()
	allClusterMembers.Add(m)
	pod := newZookeeperPod(m, allClusterMembers, c.zkCR)
	_, err := c.client.Pod().Create(pod)
	if err != nil {
		return fmt.Errorf("Failed to create member's pod (%s): %v", m.Name(), err)
	}

	klog.Infof("Added member (%s)", m.Name())

	return c.createEvent(c.newMemberAddEvent(m.Name()))
}
