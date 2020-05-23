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

package controller

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"

	client2 "zookeeper-operator/internal/client"
	"zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	apis "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	"zookeeper-operator/pkg/informer"
)

const resyncTime = 2 * time.Minute

type Controller struct {
	client client2.Client
}

func New(client client2.Client) *Controller {
	return &Controller{
		client: client,
	}
}

func (c *Controller) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	cachedInformer := c.getCachedInformer(ctx)

	expectations := controller.NewControllerExpectations()
	podHandlerBuilder := c.newResourceEventHandlerBuilderForPod(expectations)
	ZkHandlerBuilder := c.newResourceEventHandlerBuilderForZookeeper(expectations)
	// We must add the Pod handler before add ZookeeperCluster handler to avoid the influence the "expectations"
	cachedInformer.AddHandler(&corev1.Pod{}, podHandlerBuilder, 1)
	cachedInformer.AddHandler(&apis.ZookeeperCluster{}, ZkHandlerBuilder, 5)

	<-ctx.Done()
}

func (c *Controller) getCachedInformer(ctx context.Context) informer.CachedInformer {
	scheme := runtime.NewScheme()
	err := clientgoscheme.AddToScheme(scheme)
	if err != nil {
		panic(err)
	}
	err = v1alpha1.AddToScheme(scheme)
	if err != nil {
		panic(err)
	}

	cachedInformer, err := informer.NewCachedInformer(ctx, c.client.GetConfig(), scheme, resyncTime)
	if err != nil {
		panic(err)
	}

	return cachedInformer
}

func (c *Controller) newResourceEventHandlerBuilderForPod(
	expectations controller.ControllerExpectationsInterface) informer.ResourceEventHandlerBuilder {

	podHandler := ZkPodEventHandler{
		expectations: expectations,
		cli:          c.client.KubernetesInterface().CoreV1(),
	}

	return func(lister cache.GenericLister, adder informer.ResourceRateLimitingAdder) cache.ResourceEventHandlerFuncs {
		return cache.ResourceEventHandlerFuncs{
			AddFunc:    podHandler.OnAdd,
			UpdateFunc: podHandler.OnUpdate,
			DeleteFunc: podHandler.OnDelete,
		}
	}
}

func (c *Controller) newResourceEventHandlerBuilderForZookeeper(
	expections controller.ControllerExpectationsInterface) informer.ResourceEventHandlerBuilder {

	return func(lister cache.GenericLister, adder informer.ResourceRateLimitingAdder) cache.ResourceEventHandlerFuncs {
		zkSyncer := zookeeperSyncer{
			adder:      adder,
			client:     c.client,
			expections: expections,
		}

		return cache.ResourceEventHandlerFuncs{
			AddFunc: zkSyncer.sync,
			UpdateFunc: func(oldObj, newObj interface{}) {
				zkSyncer.sync(newObj)
			},
			DeleteFunc: func(obj interface{}) {
				cr := obj.(*v1alpha1.ZookeeperCluster)
				expections.DeleteExpectations(cr.GetFullName())
			},
		}
	}
}
