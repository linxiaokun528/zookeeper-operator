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
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"time"
	"zookeeper-operator/internal/util/k8sclient"
	"zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	"zookeeper-operator/pkg/informer"
)

var initRetryWaitTime = 30 * time.Second

type Controller struct {
	client k8sclient.Client
}

func New(client k8sclient.Client) *Controller {
	return &Controller{
		client: client,
	}
}

func (c *Controller) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	resourceSyncer := c.newResourceSyncerForZookeeper(ctx)
	resourceSyncer.Run(5, ctx.Done())

	// TODO: add a pod informer to watch related pods
}

func (c *Controller) newResourceSyncerForZookeeper(ctx context.Context) informer.ResourceSyncer {
	scheme := runtime.NewScheme()
	err := clientgoscheme.AddToScheme(scheme)
	if err != nil {
		panic(err)
	}
	err = v1alpha1.AddToScheme(scheme)
	if err != nil {
		panic(err)
	}

	factory, err := informer.NewResourceSyncerFactory(c.client.GetConfig(), scheme, nil)
	if err != nil {
		panic(err)
	}
	go factory.Start(ctx.Done())

	resourceSyncer := factory.ResourceSyncer(&v1alpha1.ZookeeperCluster{},
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
