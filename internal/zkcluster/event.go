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
	"time"

	"k8s.io/klog/v2"

	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *Cluster) createEvent(event *v1.Event) error {
	err := c.client.Create(c.ctx, event)
	if err != nil {
		klog.Errorf("Failed to create event for \"%s\": %v", event.Message, err)
	}
	return err
}

func (c *Cluster) newMemberAddEvent(memberName string) *v1.Event {
	event := c.newClusterEvent()
	event.Type = v1.EventTypeNormal
	event.Reason = "New Member Added"
	event.Message = fmt.Sprintf("New member %s added", memberName)
	return event
}

func (c *Cluster) newMemberRemoveEvent(memberName string) *v1.Event {
	event := c.newClusterEvent()
	event.Type = v1.EventTypeNormal
	event.Reason = "Member Removed"
	event.Message = fmt.Sprintf("Existing member %s removed", memberName)
	return event
}

func (c *Cluster) newMemberReplaceEvent(memberName string) *v1.Event {
	event := c.newClusterEvent()
	event.Type = v1.EventTypeNormal
	event.Reason = "Replacing Dead Member"
	event.Message = fmt.Sprintf("Dead member %s replaced", memberName)
	return event
}

func (c *Cluster) newMemberUpgradedEvent(memberName, oldVersion, newVersion string) *v1.Event {
	event := c.newClusterEvent()
	event.Type = v1.EventTypeNormal
	event.Reason = "Member Upgraded"
	event.Message = fmt.Sprintf("Member %s upgraded from %s to %s ", memberName, oldVersion, newVersion)
	return event
}

func (c *Cluster) newClusterEvent() *v1.Event {
	t := time.Now()
	return &v1.Event{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: c.zkCR.Name + "-",
			Namespace:    c.zkCR.Namespace,
		},
		InvolvedObject: v1.ObjectReference{
			APIVersion:      api.SchemeGroupVersion.String(),
			Kind:            api.Kind,
			Name:            c.zkCR.Name,
			Namespace:       c.zkCR.Namespace,
			UID:             c.zkCR.UID,
			ResourceVersion: c.zkCR.ResourceVersion,
		},
		Source: v1.EventSource{
			Component: "zookeeper-controller",
		},
		// Each zkcluster event is unique so it should not be collapsed with other events
		FirstTimestamp: metav1.Time{Time: t},
		LastTimestamp:  metav1.Time{Time: t},
		Count:          int32(1),
	}
}
