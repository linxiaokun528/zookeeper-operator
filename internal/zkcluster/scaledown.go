package zkcluster

import (
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog"
	"sync"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	"zookeeper-operator/pkg/errors"
)

func (c *Cluster) scaleDown() (err error) {
	klog.Infof("Scaling down zookeeper cluster %v(current cluster size: %d, desired cluster size: %d)...",
		c.zkCR.GetFullName(), c.zkCR.Status.Members.Running.Size(), c.zkCR.Spec.Size)
	defer func() {
		if err == nil {
			klog.Infof("Zookeeper cluster %v scaled down successfully", c.zkCR.GetFullName())
		}
	}()

	diff := c.zkCR.Status.Members.Running.Size() - c.zkCR.Spec.Size

	membersToRemove := c.zkCR.Status.Members.RemoveMembers(diff)

	err = c.reconfig()
	if err != nil {
		c.zkCR.Status.Members.Running.Update(membersToRemove)
		return err
	}

	errCh := make(chan error, membersToRemove.Size())
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
	close(errCh)

	return errors.NewCompoundedErrorFromChan(errCh)
}

// Remember to reconfig the zookeeper zkCR before invoking this function
func (c *Cluster) removeOneMember(m *api.Member) (err error) {
	podName := fmt.Sprintf("%s/%s", c.zkCR.Namespace, m.Name())
	c.podsToDelete.Add(podName)
	if err := c.client.Pod().Delete(m.Name(), nil); err != nil {
		c.podsToDelete.Remove(podName)
		if !apierrors.IsNotFound(err) {
			return err
		}
	}
	// TODO: @MDF: Add PV support
	/*
		if c.isPodPVEnabled() {
			err = c.removePVC(k8s.PVCNameFromMember(toRemove.Name))
			if err != nil {
				return err
			}
		}
	*/

	return c.createEvent(c.newMemberRemoveEvent(m.Name()))
}
