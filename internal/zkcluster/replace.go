package zkcluster

import (
	"k8s.io/klog"
	"sync"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
)

func (c *Cluster) ReplaceStoppedMembers() (err error) {
	klog.Infof("Replacing stopped members for %v: %v...", c.zkCR.GetFullName(), c.zkCR.Status.Members.Stopped)
	defer func() {
		if err == nil {
			klog.Infof("Stopped members for %v are successfully replaced: %v",
				c.zkCR.GetFullName(), c.zkCR.Status.Members.Stopped)
		}
	}()

	all := c.zkCR.Status.Members.Running.Copy()
	all.Update(&c.zkCR.Status.Members.Stopped)

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
	case err = <-errCh:
		// all errors have been reported before, we only need to inform the controller that there was an error and it should re-try this job once more next time.
		if err != nil {
			return err
		}
	default:
	}

	return nil
}

func (c *Cluster) replaceOneStoppedMember(toReplace *api.Member, cluster *api.Members) error {
	klog.Infof("replacing dead member %q", toReplace.Name)

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

	_, err = c.client.Event().Create(ReplacingDeadMemberEvent(toReplace.Name(), c.zkCR))
	if err != nil {
		klog.Errorf("failed to create replacing dead member event: %v", err)
	}

	return nil
}
