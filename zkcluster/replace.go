package zkcluster

import (
	"sync"
	api "zookeeper-operator/apis/zookeeper/v1alpha1"
	"zookeeper-operator/util/k8sutil"
)

func (c *Cluster) ReplaceStoppedMembers() error {
	c.logger.Infof("Some members are stopped: (%v)", c.zkCR.Status.Members.Stopped.GetMemberNames())

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
