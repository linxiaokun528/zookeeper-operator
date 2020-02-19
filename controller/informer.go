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
	"fmt"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	api "zookeeper-operator/apis/zookeeper/v1alpha1"
	zkInformers "zookeeper-operator/generated/informers/externalversions"
	"zookeeper-operator/util/k8sutil"
	"zookeeper-operator/util/probe"

	"k8s.io/client-go/tools/cache"
)

func (c *Controller) Start() {
	// TODO: get rid of this init code. CRD and storage class will be managed outside of operator.
	for {
		err := c.initResource()
		if err == nil {
			break
		}
		c.logger.Errorf("initialization failed: %v", err)
		c.logger.Infof("retry in %v...", initRetryWaitTime)
		time.Sleep(initRetryWaitTime)
	}

	probe.SetReady()
	c.run()
	//panic("unreachable")
}

func (c *Controller) run() {
	c.eventQueue = workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "events")
	defer utilruntime.HandleCrash()
	defer c.eventQueue.ShutDown()

	sharedInformerFactory := zkInformers.NewSharedInformerFactory(c.Config.ZookeeperCRCli, time.Second*300)
	go sharedInformerFactory.Start(c.ctx.Done())

	c.zkInformer = sharedInformerFactory.Zookeeper().V1alpha1().ZookeeperClusters()
	c.zkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.onAddZookeeperClus,
		UpdateFunc: c.onUpdateZookeeperClus,
		DeleteFunc: c.onDeleteZookeeperClus,
	})

	if !cache.WaitForNamedCacheSync("zookeeper", c.ctx.Done(), c.zkInformer.Informer().HasSynced) {
		return
	}

	for i := 0; i < 5; i++ {
		go wait.Until(c.worker, time.Second, c.ctx.Done())
	}

	// TODO: use workqueue to avoid blocking
	c.zkInformer.Informer().Run(c.ctx.Done())
	c.eventQueue.ShutDown()
}

func (c *Controller) worker() {
	// invoked oncely process any until exhausted
	for c.processNextWorkItem() {
	}
}

func (c *Controller) initResource() error {
	if c.Config.CreateCRD {
		err := c.initCRD()
		if err != nil {
			return fmt.Errorf("fail to init CRD: %v", err)
		}
	}
	return nil
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
	clus := obj.(*api.ZookeeperCluster)
	if !c.managed(clus) {
		return
	}

	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		c.logger.Warningf("Couldn't get key for object %+v: %v", obj, err)
		return
	}
	c.eventQueue.Add(key)
}

func (c *Controller) managed(clus *api.ZookeeperCluster) bool {
	if v, ok := clus.Annotations[k8sutil.AnnotationScope]; ok {
		if c.Config.ClusterWide {
			return v == k8sutil.AnnotationClusterWide
		}
	} else {
		if !c.Config.ClusterWide {
			return true
		}
	}
	return false
}
