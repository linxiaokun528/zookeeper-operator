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

package zkcluster

import (
	"fmt"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	k8sutil2 "zookeeper-operator/pkg/k8sutil"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
)

func (c *Cluster) beginUpgrade() {
	klog.Infof("Upgrading zookeeper cluster %v from %s to %s...",
		c.zkCR.GetFullName(), c.zkCR.Status.CurrentVersion, c.zkCR.Spec.Version)
	c.zkCR.Status.UpgradeVersionTo(c.zkCR.Spec.Version)
}

func (c *Cluster) finishUpgrade() {
	c.zkCR.Status.SetVersion(c.zkCR.Status.TargetVersion)
	klog.Infof("Zookeeper cluster %v upgraded from %s to %s successfully.",
		c.zkCR.GetFullName(), c.zkCR.Status.CurrentVersion, c.zkCR.Status.TargetVersion)
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

func (c *Cluster) upgradeOneMember() error {
	member := c.pickOneOldMember()
	if member == nil {
		return nil
	}
	return c.upgradeMember(member.Name())
}

func (c *Cluster) upgradeMember(memberName string) error {
	pod, err := c.client.Pod().Get(memberName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("fail to get pod (%s): %v", memberName, err)
	}
	oldpod := pod.DeepCopy()

	klog.Infof("Upgrading the zookeeper member %v from %s to %s", memberName, getZookeeperVersion(pod), c.zkCR.Spec.Version)
	pod.Spec.Containers[0].Image = k8sutil2.ImageName(c.zkCR.Spec.Repository, c.zkCR.Spec.Version)
	setZookeeperVersion(pod, c.zkCR.Spec.Version)

	_, err = c.client.Pod().Update(pod)
	if err != nil {
		return fmt.Errorf("fail to update the zookeeper member (%s): %v", memberName, err)
	}
	klog.Infof("finished upgrading the zookeeper member %v", memberName)
	_, err = c.client.Event().Create(MemberUpgradedEvent(memberName, getZookeeperVersion(oldpod), c.zkCR.Spec.Version, c.zkCR))
	if err != nil {
		klog.Errorf("failed to create member upgraded event: %v", err)
	}

	return nil
}
