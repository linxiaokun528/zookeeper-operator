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

	"gopkg.in/fatih/set.v0"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"

	client2 "zookeeper-operator/internal/client"
	"zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	apis "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	"zookeeper-operator/pkg/informer"
)

const resyncTime = 2 * time.Minute

type Controller struct {
	client client2.Client
	ctx    context.Context
}

func New(client client2.Client) *Controller {
	return &Controller{
		client: client,
	}
}

func (c *Controller) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()
	c.ctx = ctx
	cachedInformer := c.newCachedInformer()
	podsToDelete := set.New(set.ThreadSafe)

	c.addHandlerForPods(cachedInformer, podsToDelete)
	c.addHandlerForZookeeper(cachedInformer, podsToDelete)

	<-ctx.Done()
}

func (c *Controller) newCachedInformer() informer.CachedInformer {
	scheme := runtime.NewScheme()
	err := clientgoscheme.AddToScheme(scheme)
	if err != nil {
		panic(err)
	}
	err = v1alpha1.AddToScheme(scheme)
	if err != nil {
		panic(err)
	}

	cachedInformer, err := informer.NewCachedInformer(c.ctx, c.client.GetConfig(), scheme, resyncTime)
	if err != nil {
		panic(err)
	}

	return cachedInformer
}

func (c *Controller) addHandlerForPods(
	cachedInformer informer.CachedInformer,
	podsToDelete set.Interface) {
	podHandler := ZkPodEventHandler{
		zkLister:     cachedInformer.GetLister(&apis.ZookeeperCluster{}),
		podsToDelete: podsToDelete,
		cli:          c.client.KubernetesInterface().CoreV1(),
		ctx:          c.ctx,
	}

	podHandlerBuilder := func(lister cache.GenericLister, adder informer.ResourceRateLimitingAdder) cache.ResourceEventHandlerFuncs {
		return cache.ResourceEventHandlerFuncs{
			AddFunc:    podHandler.OnAdd,
			UpdateFunc: podHandler.OnUpdate,
			DeleteFunc: podHandler.OnDelete,
		}
	}

	cachedInformer.AddHandler(&corev1.Pod{}, podHandlerBuilder, 1)
}

func (c *Controller) addHandlerForZookeeper(
	cachedInformer informer.CachedInformer, podsToDelete set.Interface) {
	ZkHandlerBuilder := func(lister cache.GenericLister, adder informer.ResourceRateLimitingAdder) cache.ResourceEventHandlerFuncs {
		zkSyncer := zookeeperSyncer{
			adder:        adder,
			client:       c.client,
			podLister:    cachedInformer.GetLister(&corev1.Pod{}),
			podsToDelete: podsToDelete,
			ctx:          c.ctx,
		}

		return cache.ResourceEventHandlerFuncs{
			AddFunc: zkSyncer.sync,
			UpdateFunc: func(oldObj, newObj interface{}) {
				zkSyncer.sync(newObj)
			},
		}
	}

	cachedInformer.AddHandler(&apis.ZookeeperCluster{}, ZkHandlerBuilder, 5)
}
