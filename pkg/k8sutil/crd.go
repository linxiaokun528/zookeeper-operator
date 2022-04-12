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
	"context"
	"fmt"
	"strconv"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientsetretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
)

// TODO: consider manage CRDClient outside of operator. If so, refactor zkclient.Client: use zkclient.CRClient to replace
func InitCRDOrDie(client client.Client, ctx context.Context) {
	err := InitCRD(client, ctx)
	if err != nil {
		panic(err)
	}
}

func InitCRD(client client.Client, ctx context.Context) error {
	crdClient := crdClient{
		client: client,
		ctx:    ctx,
	}
	crd := newCRDForZookeeperCluster()
	return crdClient.CreateAndWait(crd)
}

func newCRDForZookeeperCluster() *apiextensionsv1.CustomResourceDefinition {
	minSize := 1.0
	xPreserveUnknownFields := true

	customResourceDefinition := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: api.Plural + "." + api.GroupName,
			// Set the labels so that it will be easier to clean all resources created by zookeeper-operator.
			Labels: map[string]string{
				"app": "zookeeper-operator",
			},
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: api.GroupName,
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    api.Version,
					Served:  true,
					Storage: true,
					Subresources: &apiextensionsv1.CustomResourceSubresources{
						Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					},
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type:     "object",
							Required: []string{"spec"},
							Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"spec": {
									Type:     "object",
									Required: []string{"size"},
									Properties: map[string]apiextensionsv1.JSONSchemaProps{
										"size": {
											Type:    "integer",
											Minimum: &minSize,
										},
										"version": {
											Type: "string",
											// valid versions: "v0.0.0", "V3.2.3", "3.0.10", "1.1.1"
											// invalid versions: "v0.0.0.", "Vv3.2.3", "03.0.10", "1.1"
											Pattern: `^[vV]?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$`,
											Default: &apiextensionsv1.JSON{
												Raw: []byte(strconv.Quote(api.DefaultZookeeperVersion)),
											},
										},
										"repository": {
											Type: "string",
											Default: &apiextensionsv1.JSON{
												Raw: []byte(strconv.Quote(api.DefaultRepository)),
											},
										},
									},
								},
								"status": {
									Type:                   "object",
									XPreserveUnknownFields: &xPreserveUnknownFields,
								},
							},
						},
					},
				},
			},
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: api.Plural,
				Kind:   api.Kind,
			},
		},
	}
	if len(api.Short) != 0 {
		customResourceDefinition.Spec.Names.ShortNames = []string{api.Short}
	}

	return customResourceDefinition
}

type crdClient struct {
	client client.Client
	ctx    context.Context
}

func (c *crdClient) Create(crd *apiextensionsv1.CustomResourceDefinition) error {
	err := c.client.Create(c.ctx, crd)
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
			err := c.client.Get(c.ctx, client.ObjectKey{
				Namespace: crd.GetNamespace(),
				Name:      crd.GetName(),
			}, crd)
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
