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
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"time"
	"zookeeper-operator/client"

	"github.com/sirupsen/logrus"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	api "zookeeper-operator/apis/zookeeper/v1alpha1"
	"zookeeper-operator/cluster"
	zkInformer "zookeeper-operator/generated/informers/externalversions/zookeeper/v1alpha1"
)

var initRetryWaitTime = 30 * time.Second

type Controller struct {
	logger *logrus.Entry

	client client.Client

	eventQueue workqueue.RateLimitingInterface
	zkInformer zkInformer.ZookeeperClusterInformer
}

func New(client client.Client) *Controller {
	return &Controller{
		logger: logrus.WithField("pkg", "controller"),
		client: client,
	}
}

// ProcessNextWorkItem processes next item in queue by consumer
func (c *Controller) processNextWorkItem() bool {
	obj, quit := c.eventQueue.Get()
	if quit {
		return false
	}
	defer c.eventQueue.Done(obj)

	key := obj.(string)
	forget, err := c.syncHandler(key)
	if err == nil {
		if forget {
			c.eventQueue.Forget(key)
		}
		return true
	}

	utilruntime.HandleError(fmt.Errorf("Error syncing zookeeper cluster: %v", err))
	c.eventQueue.AddRateLimited(key)

	return true
}

func (c *Controller) syncHandler(key string) (bool, error) {
	ns, name, err := cache.SplitMetaNamespaceKey(key)

	if err != nil {
		return false, err
	}
	if len(ns) == 0 || len(name) == 0 {
		return false, fmt.Errorf("invalid zookeeper cluster key %q: either namespace or name is missing", key)
	}
	sharedCluster, err := c.zkInformer.Lister().ZookeeperClusters(ns).Get(name)
	if sharedCluster.DeletionTimestamp != nil {
		return true, nil
	}

	sharedCluster = sharedCluster.DeepCopy()
	zkCluster := cluster.New(c.client.GetCRClient(sharedCluster.Namespace), sharedCluster)
	defer func() {
		if !zkCluster.IsFinished() {
			c.eventQueue.AddAfter(key, 30*time.Second)
		}
	}()

	start := time.Now()

	if sharedCluster.Status.Phase == api.ClusterPhaseNone {
		err = zkCluster.Create()
		if err != nil {
			return false, err
		}
	}
	rerr := zkCluster.Sync()

	cluster.ReconcileHistogram.WithLabelValues(sharedCluster.Name).Observe(time.Since(start).Seconds())

	if rerr != nil {
		cluster.ReconcileFailed.WithLabelValues(rerr.Error()).Inc()
		if cluster.IsFatalError(rerr) {
			sharedCluster.Status.SetReason(rerr.Error())
			c.logger.Errorf("cluster failed: %v", rerr)
			zkCluster.ReportFailedStatus()
		}
		return false, rerr
	}

	return true, nil
}
