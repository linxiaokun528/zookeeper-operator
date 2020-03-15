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
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	api "zookeeper-operator/apis/zookeeper/v1alpha1"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientv1 "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientsetretry "k8s.io/client-go/util/retry"
)

type CRD interface {
	Create() error
	Wait() error
	CreateAndWait() error
	CustomResourceDefinition() *apiextensionsv1.CustomResourceDefinition
}

type crd struct {
	client                   apiextensionsclientv1.CustomResourceDefinitionInterface
	customResourceDefinition *apiextensionsv1.CustomResourceDefinition
}

func (c *crd) Create() error {
	customResourceDefinition, err := c.client.Create(c.customResourceDefinition)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	c.customResourceDefinition = customResourceDefinition
	return nil
}

func (c *crd) Wait() error {
	err := clientsetretry.OnError(clientsetretry.DefaultRetry,
		func(err error) bool {
			return true
		},
		func() error {
			crd, err := c.client.Get(c.customResourceDefinition.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			for _, cond := range crd.Status.Conditions {
				switch cond.Type {
				case apiextensionsv1.Established:
					if cond.Status == apiextensionsv1.ConditionTrue {
						return nil
					}
				case apiextensionsv1.NamesAccepted:
					if cond.Status == apiextensionsv1.ConditionFalse {
						return fmt.Errorf("Name conflict: %v", cond.Reason)
					}
				}
			}
			return fmt.Errorf("Not ready yet")
		})

	if err != nil {
		return fmt.Errorf("wait CRD created failed: %v", err)
	}
	return nil
}

func (c *crd) CreateAndWait() error {
	err := c.Create()
	if err != nil {
		return nil
	}

	return c.Wait()
}

func (c *crd) CustomResourceDefinition() *apiextensionsv1.CustomResourceDefinition {
	return c.customResourceDefinition
}

func NewCRD(client apiextensionsclientv1.CustomResourceDefinitionInterface, crdName, rkind, rplural, shortName string) CRD {
	customResourceDefinition := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: crdName,
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: api.SchemeGroupVersion.Group,
			Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: rplural,
				Kind:   rkind,
			},
		},
	}
	if len(shortName) != 0 {
		customResourceDefinition.Spec.Names.ShortNames = []string{shortName}
	}

	return &crd{
		client:                   client,
		customResourceDefinition: customResourceDefinition,
	}
}
