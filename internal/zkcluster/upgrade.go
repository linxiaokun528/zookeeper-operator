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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"zookeeper-operator/pkg/k8sutil"

	internalutil "zookeeper-operator/internal/util/k8s"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
)

func (c *Cluster) beginUpgrade() {
	klog.Infof("Upgrading zookeeper cluster %v from %s to %s...",
		c.zkCR.GetNamespacedName(), c.zkCR.Status.CurrentVersion, c.zkCR.Spec.Version)
	c.zkCR.Status.UpgradeVersionTo(c.zkCR.Spec.Version)
}

func (c *Cluster) finishUpgrade() {
	c.zkCR.Status.SetVersion(c.zkCR.Status.TargetVersion)
	klog.Infof("Zookeeper cluster %v upgraded from %s to %s successfully.",
		c.zkCR.GetNamespacedName(), c.zkCR.Status.CurrentVersion, c.zkCR.Status.TargetVersion)
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
	return c.upgradeMember(member)
}

func (c *Cluster) upgradeMember(m *api.Member) error {
	pod := &corev1.Pod{}
	err := c.client.Get(c.ctx, client.ObjectKey{
		Namespace: m.Namespace(),
		Name:      m.Name(),
	}, pod)
	memberName := k8sutil.GetNamespacedName(pod)
	if err != nil {
		return fmt.Errorf("fail to get pod (%s): %v", memberName, err)
	}
	oldpod := pod.DeepCopy()

	klog.Infof("Upgrading the zookeeper member %v from %s to %s", memberName, internalutil.GetZookeeperVersion(pod), c.zkCR.Spec.Version)
	internalutil.SetZookeeperVersion(pod, c.zkCR.Spec.Version)

	err = c.client.Update(c.ctx, pod)
	if err != nil {
		return fmt.Errorf("fail to update the zookeeper member (%s): %v", memberName, err)
	}

	return c.createEvent(c.newMemberUpgradedEvent(memberName, internalutil.GetZookeeperVersion(oldpod), c.zkCR.Spec.Version))
}
