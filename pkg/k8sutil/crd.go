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
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsclientv1 "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	clientsetretry "k8s.io/client-go/util/retry"
)

type CRDClient interface {
	Create(crd *apiextensionsv1.CustomResourceDefinition) error
	Wait(crd *apiextensionsv1.CustomResourceDefinition) error
	CreateAndWait(crd *apiextensionsv1.CustomResourceDefinition) error
}

type crdClient struct {
	client apiextensionsclientv1.CustomResourceDefinitionInterface
}

func NewCRDClientOrDie(config *rest.Config) CRDClient {
	return &crdClient{
		client: apiextensionsclient.NewForConfigOrDie(config).ApiextensionsV1beta1().CustomResourceDefinitions(),
	}
}

func (c *crdClient) Create(crd *apiextensionsv1.CustomResourceDefinition) error {
	_, err := c.client.Create(crd)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func (c *crdClient) Wait(crd *apiextensionsv1.CustomResourceDefinition) error {
	err := clientsetretry.OnError(clientsetretry.DefaultRetry,
		func(err error) bool {
			return true
		},
		func() error {
			crd, err := c.client.Get(crd.Name, metav1.GetOptions{})
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
		return fmt.Errorf("wait CRDClient created failed: %v", err)
	}
	return nil
}

func (c *crdClient) CreateAndWait(crd *apiextensionsv1.CustomResourceDefinition) error {
	err := c.Create(crd)
	if err != nil {
		return err
	}

	return c.Wait(crd)
}

func NewCRD(scope apiextensionsv1.ResourceScope,
	gvk schema.GroupVersionKind, plural, shortName string) *apiextensionsv1.CustomResourceDefinition {
	customResourceDefinition := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: plural + "." + gvk.Group,
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: gvk.Group,
			Scope: scope,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    gvk.Version,
					Served:  true,
					Storage: true,
				},
			},
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: plural,
				Kind:   gvk.Kind,
			},
		},
	}
	if len(shortName) != 0 {
		customResourceDefinition.Spec.Names.ShortNames = []string{shortName}
	}

	return customResourceDefinition
}
