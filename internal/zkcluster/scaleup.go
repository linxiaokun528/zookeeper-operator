package zkcluster

import (
	"fmt"
	"k8s.io/klog"
	"sync"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
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
	case err = <-errCh:
		// all errors have been reported before, we only need to inform the controller that there was an error and it should re-try this once more next time.
		if err != nil {
			return err
		}
	default:
	}

	return nil
}

func (c *Cluster) finishScaleUp() (err error) {
	defer func() {
		if err == nil {
			klog.Infof("Zookeeper cluster %v scaled up successfully", c.zkCR.GetFullName())
		}
	}()
	return c.reconfig()
}

// The member added will be in c.zkCR.Status.Members.unready
func (c *Cluster) addOneMember(m *api.Member, allClusterMembers *api.Members) error {
	pod := newZookeeperPod(m, allClusterMembers, c.zkCR)
	_, err := c.client.Pod().Create(pod)
	if err != nil {
		return fmt.Errorf("fail to create member's pod (%s): %v", m.Name(), err)
	}

	klog.Infof("added member (%s)", m.Name())
	_, err = c.client.Event().Create(NewMemberAddEvent(m.Name(), c.zkCR))
	if err != nil {
		klog.Errorf("failed to create new member add event: %v", err)
	}
	return nil
}
