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

package cluster

import (
	"fmt"

	"zookeeper-operator/util/k8sutil"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func (c *Cluster) upgradeOneMember(memberName string) error {
	c.zkCR.Status.SetUpgradingCondition(c.zkCR.Spec.Version)

	pod, err := c.client.Pod().Get(memberName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("fail to get pod (%s): %v", memberName, err)
	}
	oldpod := pod.DeepCopy()

	c.logger.Infof("upgrading the zookeeper member %v from %s to %s", memberName, k8sutil.GetZookeeperVersion(pod), c.zkCR.Spec.Version)
	pod.Spec.Containers[0].Image = k8sutil.ImageName(c.zkCR.Spec.Repository, c.zkCR.Spec.Version)
	k8sutil.SetZookeeperVersion(pod, c.zkCR.Spec.Version)

	patchdata, err := k8sutil.CreatePatch(oldpod, pod, v1.Pod{})
	if err != nil {
		return fmt.Errorf("error creating patch: %v", err)
	}

	_, err = c.client.Pod().Patch(pod.GetName(), types.StrategicMergePatchType, patchdata)
	if err != nil {
		return fmt.Errorf("fail to update the zookeeper member (%s): %v", memberName, err)
	}
	c.logger.Infof("finished upgrading the zookeeper member %v", memberName)
	_, err = c.client.Event().Create(k8sutil.MemberUpgradedEvent(memberName, k8sutil.GetZookeeperVersion(oldpod), c.zkCR.Spec.Version, c.zkCR))
	if err != nil {
		c.logger.Errorf("failed to create member upgraded event: %v", err)
	}

	return nil
}
