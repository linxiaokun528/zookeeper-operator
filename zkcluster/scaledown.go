package zkcluster

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sync"
	api "zookeeper-operator/apis/zookeeper/v1alpha1"
	"zookeeper-operator/util/k8sutil"
)

func (c *Cluster) scaleDown() error {
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

// Remember to reconfig the zookeeper zkCR before invoking this function
func (c *Cluster) removeOneMember(m *api.Member) (err error) {
	if err := c.client.Pod().Delete(m.Name(), nil); err != nil {
		if err != nil {
			if apierrors.IsNotFound(err) {
				return err
			}
		}
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
