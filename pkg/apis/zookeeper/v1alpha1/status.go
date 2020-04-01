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

package v1alpha1

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ClusterPhaseType string
type ClusterConditionType string

const (
	ClusterCreating    = "Creating"
	ClusterRunning     = "Running"
	ClusterScalingUp   = "ScalingUp"
	ClusterScalingDown = "ScalingDown"
	ClusterRecovering  = "Recovering"
	ClusterUpgrading   = "Upgrading"

	Empty = ""
)

// ClusterStatus represents the current state of a zookeeper zkcluster.
type ClusterStatus struct {
	// Time when first processed
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// Condition keeps track of all zkcluster conditions, if they exist.
	Conditions []*ClusterCondition `json:"conditions,omitempty"`

	// Size is the current size of the zkcluster
	Size int `json:"size,omit"`

	// Members are the zookeeper members in the zkcluster
	Members *ZKCluster `json:"members"`
	// CurrentVersion is the current zkcluster version
	CurrentVersion string `json:"currentVersion"`
	// TargetVersion is the version the zkcluster upgrading to.
	// If the zkcluster is not upgrading, TargetVersion is empty.
	TargetVersion string `json:"targetVersion"`
}

type ClusterPhase struct {
	// Type of the current cluster phase
	Type ClusterPhaseType `json:"type"`
	// The last time this condition was updated.
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
	// The reason for the condition's last transition.
	Reason string `json:"reason,omitempty"`
	// A human readable message indicating details about the transition.
	Message string `json:"message,omitempty"`
}

// ClusterCondition represents one current condition of an zookeeper zkcluster.
// A condition might not show up if it is not happening.
// For example, if a zkcluster is not upgrading, the Upgrading condition would not show up.
// If a zkcluster is upgrading and encountered a problem that prevents the upgrade,
// the Upgrading condition's status will would be False and communicate the problem back.
type ClusterCondition struct {
	// Type of zkcluster condition.
	Type ClusterConditionType `json:"type"`
	// The last time this condition was updated.
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
	// The reason for the condition's last transition.
	Reason string `json:"reason,omitempty"`
	// A human readable message indicating details about the transition.
	Message string `json:"message,omitempty"`
}

func (cs *ClusterStatus) UpgradeVersionTo(v string) {
	cs.TargetVersion = v
}

func (cs *ClusterStatus) SetVersion(v string) {
	cs.TargetVersion = ""
	cs.CurrentVersion = v
}

func (cs *ClusterStatus) AppendScalingUpCondition(from, to int) {
	c := newClusterCondition(ClusterScalingUp, "Scaling up", scalingMsg(from, to))
	cs.appendClusterCondition(c)
}

func (cs *ClusterStatus) AppendScalingDownCondition(from, to int) {
	c := newClusterCondition(ClusterScalingDown, "Scaling down", scalingMsg(from, to))
	cs.appendClusterCondition(c)
}

func (cs *ClusterStatus) AppendRecoveringCondition(deadMembers *Members) {
	c := newClusterCondition(ClusterRecovering,
		fmt.Sprintf("Disaster recovery: Found %d dead members", deadMembers.Size()),
		fmt.Sprintf("The following members are dead: %s", deadMembers.GetMemberNames()))
	cs.appendClusterCondition(c)
}

func (cs *ClusterStatus) AppendUpgradingCondition(to string) {
	// TODO: show x/y members has upgraded.
	c := newClusterCondition(ClusterUpgrading,
		"Cluster upgrading", "upgrading to "+to)
	cs.appendClusterCondition(c)
}

func (cs *ClusterStatus) SetRunningCondition() {
	if cs.GetCurrentCondition().Type != ClusterRunning {
		c := newClusterCondition(ClusterRunning, "Cluster available", "")
		cs.appendClusterCondition(c)
	}
}

func (cs *ClusterStatus) AppendCreatingCondition() {
	c := newClusterCondition(ClusterCreating, "Cluster creating", "")
	cs.appendClusterCondition(c)
}

func (cs *ClusterStatus) appendClusterCondition(c *ClusterCondition) {
	cs.Conditions = append(cs.Conditions, c)
}

func (cs *ClusterStatus) GetCurrentCondition() *ClusterCondition {
	return cs.Conditions[len(cs.Conditions)-1]
}

func newClusterCondition(condType ClusterConditionType, reason, message string) *ClusterCondition {
	return &ClusterCondition{
		Type:           condType,
		LastUpdateTime: metav1.Now(),
		Reason:         reason,
		Message:        message,
	}
}

func scalingMsg(from, to int) string {
	return fmt.Sprintf("Current cluster size: %d, desired cluster size: %d", from, to)
}
