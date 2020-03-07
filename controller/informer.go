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
	"fmt"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	api "zookeeper-operator/apis/zookeeper/v1alpha1"
	zkInformers "zookeeper-operator/generated/informers/externalversions"
)

func (c *Controller) Run(ctx context.Context) {
	c.eventQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	defer utilruntime.HandleCrash()
	defer c.eventQueue.ShutDown()

	sharedInformerFactory := zkInformers.NewSharedInformerFactory(c.client.ZookeeperInterface(), time.Minute*2)
	go sharedInformerFactory.Start(ctx.Done())

	c.zkInformer = sharedInformerFactory.Zookeeper().V1alpha1().ZookeeperClusters()
	c.zkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.onAddZookeeperClus,
		UpdateFunc: c.onUpdateZookeeperClus,
		DeleteFunc: c.onDeleteZookeeperClus,
	})

	if !cache.WaitForNamedCacheSync("zookeeper", ctx.Done(), c.zkInformer.Informer().HasSynced) {
		return
	}

	for i := 0; i < 5; i++ {
		go wait.Until(c.worker, time.Second, ctx.Done())
	}

	c.zkInformer.Informer().Run(ctx.Done())
}

func (c *Controller) worker() {
	// invoked oncely process any until exhausted
	for c.processNextWorkItem() {
	}
}

func (c *Controller) onAddZookeeperClus(obj interface{}) {
	c.addToQueue(obj)
}

func (c *Controller) onUpdateZookeeperClus(oldObj, newObj interface{}) {
	c.addToQueue(newObj)
}

func (c *Controller) onDeleteZookeeperClus(obj interface{}) {
	_, ok := obj.(*api.ZookeeperCluster)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			panic(fmt.Sprintf("unknown object from ZookeeperCluster delete event: %#v", obj))
		}
		_, ok = tombstone.Obj.(*api.ZookeeperCluster)
		if !ok {
			panic(fmt.Sprintf("Tombstone contained object that is not an ZookeeperCluster: %#v", obj))
		}

		key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
		if err != nil {
			c.logger.Warningf("Couldn't get key for object %+v: %v", obj, err)
			return
		}
		c.eventQueue.Forget(key)
	}
	// Don't need to do anything. If users want to delete the zk cluster, they can specify "--cascade=true"
	// c.addToQueue(clus)
}

func (c *Controller) addToQueue(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		c.logger.Warningf("Couldn't get key for object %+v: %v", obj, err)
		return
	}
	c.eventQueue.Add(key)
}
