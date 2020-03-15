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
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"time"
	v1alpha1 "zookeeper-operator/apis/zookeeper/v1alpha1"
	"zookeeper-operator/client"
	zkInformers "zookeeper-operator/generated/informers/externalversions"
	"zookeeper-operator/util/informer"
)

var initRetryWaitTime = 30 * time.Second

type Controller struct {
	client client.Client
}

func New(client client.Client) *Controller {
	return &Controller{
		client: client,
	}
}

func (c *Controller) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	resourceSyncer := c.newResourceSyncerForZookeeper()
	resourceSyncer.Run(ctx, 5)

	// TODO: add a pod informer to watch related pods
}

func (c *Controller) newResourceSyncerForZookeeper() *informer.ResourceSyncer {
	sharedInformerFactory := zkInformers.NewSharedInformerFactory(c.client.ZookeeperInterface(), time.Minute*2)

	zkInformer := sharedInformerFactory.Zookeeper().V1alpha1().ZookeeperClusters().Informer()
	groupVersionResource := v1alpha1.SchemeGroupVersion.WithResource("zookeeperclusters")

	resourceSyncer := informer.NewResourceSyncer(zkInformer, &groupVersionResource,
		func(lister cache.GenericLister, adder informer.ResourceRateLimitingAdder) informer.Syncer {
			zkSyncer := zookeeperSyncer{
				adder:  adder,
				client: c.client,
			}

			return informer.Syncer{
				Sync: zkSyncer.sync,
			}
		})

	return resourceSyncer
}
