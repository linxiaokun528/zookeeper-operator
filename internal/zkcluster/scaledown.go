package zkcluster

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	"zookeeper-operator/pkg/errors"
)

func (c *Cluster) scaleDown() (err error) {
	klog.Infof("Scaling down zookeeper cluster %v(current cluster size: %d, desired cluster size: %d)...",
		c.zkCR.GetNamespacedName(), c.zkCR.Status.Members.Running.Size(), c.zkCR.Spec.Size)
	defer func() {
		if err == nil {
			klog.Infof("Zookeeper cluster %v scaled down successfully", c.zkCR.GetNamespacedName())
		}
	}()

	diff := c.zkCR.Status.Members.Running.Size() - c.zkCR.Spec.Size

	membersToRemove := c.zkCR.Status.Members.RemoveMembers(diff)

	err = c.reconfig()
	if err != nil {
		c.zkCR.Status.Members.Running.Update(membersToRemove)
		return err
	}

	errCh := make(chan error, membersToRemove.Size()) // todo: use errgroup instead
	group := wait.Group{}

	for _, m := range membersToRemove.GetElements() {
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
		tmp := m
		group.Start(func() {
			err := c.removeOneMember(tmp)
			if err != nil {
				errCh <- err
				c.locker.Lock()
				c.zkCR.Status.Members.Running.Add(tmp)
				c.locker.Unlock()
			}
		})
	}
	group.Wait()
	close(errCh)

	return errors.NewCompoundedErrorFromChan(errCh)
}

// Remember to reconfig the zookeeper zkCR before invoking this function
func (c *Cluster) removeOneMember(m *api.Member) (err error) {
	podName := fmt.Sprintf("%s/%s", c.zkCR.Namespace, m.Name())
	c.podsToDelete.Add(podName)
	err = c.client.Delete(c.ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace(), Name: m.Name()}})
	if err != nil {
		c.podsToDelete.RemoveFirst(podName)
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
	klog.Infof("Deleted member (%s)", m.Name())
	return c.createEvent(c.newMemberRemoveEvent(m.Name()))
}
