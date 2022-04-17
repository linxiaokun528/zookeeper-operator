package zkcluster

import (
	"k8s.io/apimachinery/pkg/util/wait"
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

	group := wait.Group{}
	errCh := make(chan error, c.zkCR.Status.Members.Stopped.Size())
	for _, dead_member := range c.zkCR.Status.Members.Stopped.GetElements() {
		//for _, t := range []int{1, 2, 3, 4} {
		//	go func() {
		//		fmt.Println(t)  // result: 4 4 4 4
		//	}()
		//}
		//for _, t := range []int{1, 2, 3, 4} {
		//	tmp := t
		//	go func() {
		//		fmt.Println(tmp) // result: 1 2 3 4 (the order may change)
		//	}()
		//}
		tmp := dead_member
		group.Start(func() {
			err := c.replaceOneStoppedMember(tmp, all)
			if err != nil {
				errCh <- err
			}
		})
	}
	group.Wait()
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

	klog.Infof("Replaced stopped member (%s)", toReplace.Name())
	return c.createEvent(c.newMemberReplaceEvent(toReplace.Name()))
}
