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
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	DefaultRepository       = "zookeeper-instance"
	DefaultZookeeperVersion = "3.5.7"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ZookeeperClusterList is a list of zookeeper clusters.
type ZookeeperClusterList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata
	// More info: http://releases.k8s.io/HEAD/docs/devel/api-conventions.md#metadata
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ZookeeperCluster `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ZookeeperCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ClusterSpec    `json:"spec"`
	Status            *ClusterStatus `json:"status"`
}

func (c *ZookeeperCluster) GetNamespacedName() types.NamespacedName {
	return types.NamespacedName{
		Namespace: c.Namespace,
		Name:      c.Name,
	}
}

func (c *ZookeeperCluster) AsOwner() metav1.OwnerReference {
	trueVar := true
	return metav1.OwnerReference{
		APIVersion: SchemeGroupVersion.String(),
		Kind:       Kind,
		Name:       c.Name,
		UID:        c.UID,
		Controller: &trueVar,
	}
}

func (c *ZookeeperCluster) UnmarshalJSON(b []byte) error {
	type tmpType ZookeeperCluster
	err := json.Unmarshal(b, (*tmpType)(c))
	if err != nil {
		return err
	}

	if c.Status == nil {
		c.Status = &ClusterStatus{}
	}
	if c.Status.Members != nil {
		clusterID := clusterID{namespace: c.Namespace, clusterName: c.Name}
		c.Status.Members.setClusterID(&clusterID)
		c.Status.Members.version = c.Status.CurrentVersion
	}

	return nil
}

type ClusterSpec struct {
	// Size is the expected size of the zookeeper cluster.
	// The zookeeper-operator will eventually make the size of the running
	// zkcluster equal to the expected size.
	// The valid range of the size is from 1 to infinite
	Size int `json:"size"`
	// Repository is the name of the repository that hosts
	// zookeeper container images. It should be direct adapted from the repository in official
	// release:
	//   https://hub.docker.com/_/zookeeper/
	// That means, it should have exact same tags and the same meaning for the tags.
	//
	// By default, it is `zookeeper-instance`.
	Repository string `json:"repository,omitempty"`

	// Version is the expected version of the zookeeper cluster.
	// The zookeeper-operator will eventually make the zookeeper zkcluster version
	// equal to the expected version.
	//
	// The version must follow the [semver]( http://semver.org) format, for example "3.5.3-beta".
	// Only zookeeper released versions greater than or equal to 3.5.0 are supported.
	// Please refer to https://hub.docker.com/_/zookeeper/?tab=tags to search for all supported versions.
	//
	// If version is not set, default is "3.5.7".
	Version string `json:"version,omitempty"`
}
