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

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/controller"

	client2 "zookeeper-operator/internal/client"
	"zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
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

	expectations := controller.NewControllerExpectations()

	podInformer := c.newZKPodInformer(expectations)
	go podInformer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced) {
		klog.Errorf("Failed to sync pods for zookeeper clusters!")
		return
	}

	resourceSyncer := c.newResourceSyncerForZookeeper(expectations, ctx)
	resourceSyncer.Run(5, ctx.Done())
}

func (c *Controller) newZKPodInformer(
	expectations controller.ControllerExpectationsInterface) cache.Controller {
	source := cache.NewFilteredListWatchFromClient(
		c.client.KubernetesInterface().CoreV1().RESTClient(),
		string(v1.ResourcePods),
		metav1.NamespaceAll,
		func(options *metav1.ListOptions) {
			options.LabelSelector = labels.SelectorFromSet(map[string]string{"app": "zookeeper"}).String()
		},
	)
	podHandler := ZkPodEventHandler{
		expectations: expectations,
		cli:          c.client.KubernetesInterface().CoreV1(),
	}

	_, podInformer := cache.NewInformer(source, &v1.Pod{}, resyncTime, &podHandler)

	return podInformer
}

func (c *Controller) newResourceSyncerForZookeeper(expections controller.ControllerExpectationsInterface,
	ctx context.Context) informer.ResourceSyncer {
	scheme := runtime.NewScheme()
	err := clientgoscheme.AddToScheme(scheme)
	if err != nil {
		panic(err)
	}
	err = v1alpha1.AddToScheme(scheme)
	if err != nil {
		panic(err)
	}

	factory, err := informer.NewResourceSyncerFactory(c.client.GetConfig(), scheme, resyncTime)
	if err != nil {
		panic(err)
	}
	go factory.Start(ctx.Done())

	resourceSyncer := factory.ResourceSyncer(&v1alpha1.ZookeeperCluster{},
		func(lister cache.GenericLister, adder informer.ResourceRateLimitingAdder) cache.ResourceEventHandlerFuncs {
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
		})

	return resourceSyncer
}
