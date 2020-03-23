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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"reflect"
)

const (
	Plural    = "zookeeperclusters"
	Short     = "zookeeper"
	GroupName = "zookeeper.database.apache.com"
	Version   = "v1alpha1"
)

var (
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
	Kind          = reflect.TypeOf(ZookeeperCluster{}).Name()

	SchemeGroupVersionKind = schema.GroupVersionKind{Group: GroupName, Version: Version,
		Kind: Kind}
	SchemeGroupVersion = SchemeGroupVersionKind.GroupVersion()
)

// Resource gets an ZookeeperCluster GroupResource for a specified resource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// addKnownTypes adds the set of types defined in this package to the supplied scheme.
func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(SchemeGroupVersionKind.GroupVersion(),
		&ZookeeperCluster{},
		&ZookeeperClusterList{},
	)
	metav1.AddToGroupVersion(s, SchemeGroupVersionKind.GroupVersion())
	return nil
}
