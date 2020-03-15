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
	api "zookeeper-operator/apis/zookeeper/v1alpha1"

	"zookeeper-operator/util/k8sutil"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *Cluster) pickOneOldMember() *api.Member {
	for _, member := range c.zkCR.Status.Members.Running.GetElements() {
		if member.Version() == c.zkCR.Spec.Version {
			continue
		}
		return member
	}
	return nil
}

func (c *Cluster) needUpgrade() bool {
	return c.zkCR.Status.Members.Running.Size() == c.zkCR.Spec.Size && c.pickOneOldMember() != nil
}

func (c *Cluster) upgradeOneMember(memberName string) error {
	c.zkCR.Status.AppendUpgradingCondition(c.zkCR.Spec.Version)

	pod, err := c.client.Pod().Get(memberName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("fail to get pod (%s): %v", memberName, err)
	}
	oldpod := pod.DeepCopy()

	c.logger.Infof("upgrading the zookeeper member %v from %s to %s", memberName, getZookeeperVersion(pod), c.zkCR.Spec.Version)
	pod.Spec.Containers[0].Image = k8sutil.ImageName(c.zkCR.Spec.Repository, c.zkCR.Spec.Version)
	setZookeeperVersion(pod, c.zkCR.Spec.Version)

	//patchdata, err := k8sutil.CreatePatch(oldpod, pod, v1.Pod{})
	//if err != nil {
	//	return fmt.Errorf("error creating patch: %v", err)
	//}
	//
	//_, err = c.client.Pod().Patch(pod.GetName(), types.StrategicMergePatchType, patchdata)
	// TODO: Use retry mechanism
	_, err = c.client.Pod().Update(pod)
	if err != nil {
		return fmt.Errorf("fail to update the zookeeper member (%s): %v", memberName, err)
	}
	c.logger.Infof("finished upgrading the zookeeper member %v", memberName)
	_, err = c.client.Event().Create(k8sutil.MemberUpgradedEvent(memberName, getZookeeperVersion(oldpod), c.zkCR.Spec.Version, c.zkCR))
	if err != nil {
		c.logger.Errorf("failed to create member upgraded event: %v", err)
	}

	return nil
}
