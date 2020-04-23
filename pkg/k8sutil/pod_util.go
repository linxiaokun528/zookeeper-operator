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

package k8sutil

import (
	"k8s.io/api/core/v1"
)

// IsPodReady returns false if the Pod Status is nil
func IsPodReady(pod *v1.Pod) bool {
	condition := getPodReadyCondition(&pod.Status)
	return condition != nil && condition.Status == v1.ConditionTrue
}

func getPodReadyCondition(status *v1.PodStatus) *v1.PodCondition {
	for i := range status.Conditions {
		if status.Conditions[i].Type == v1.PodReady {
			return &status.Conditions[i]
		}
	}
	return nil
}

func GetPodsSeparatedByStatus(pods []*v1.Pod) (active, unready, stopped []*v1.Pod) {
	for _, pod := range pods {
		switch pod.Status.Phase {
		case v1.PodRunning:
			if IsPodReady(pod) {
				active = append(active, pod)
			} else {
				unready = append(unready, pod)
			}
		case v1.PodPending, v1.PodUnknown:
			unready = append(unready, pod)
		default:
			stopped = append(stopped, pod)
		}
	}

	return active, unready, stopped
}
