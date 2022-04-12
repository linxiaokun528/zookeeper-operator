package zkcluster

import (
	"sync"

	"k8s.io/klog/v2"

	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	"zookeeper-operator/pkg/errors"
)

func (c *Cluster) ReplaceStoppedMembers() (err error) {
	klog.Infof("Replacing stopped members for %v: %v...", c.zkCR.GetNamespacedName(), c.zkCR.Status.Members.Stopped)
	defer func() {
		if err == nil {
			klog.Infof("Stopped members for %v are successfully replaced: %v",
				c.zkCR.GetNamespacedName(), c.zkCR.Status.Members.Stopped)
		}
	}()

	all := c.zkCR.Status.Members.Running.Copy()
	all.Update(&c.zkCR.Status.Members.Stopped)

	wait := sync.WaitGroup{}
	wait.Add(c.zkCR.Status.Members.Stopped.Size())
	errCh := make(chan error, c.zkCR.Status.Members.Stopped.Size())
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
	close(errCh)

	return errors.NewCompoundedErrorFromChan(errCh)
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

	return c.createEvent(c.newMemberReplaceEvent(toReplace.Name()))
}
