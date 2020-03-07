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
	api "zookeeper-operator/apis/zookeeper/v1alpha1"
	v1alpha1 "zookeeper-operator/apis/zookeeper/v1alpha1"
	"zookeeper-operator/client"
	"zookeeper-operator/cluster"
	zkInformers "zookeeper-operator/generated/informers/externalversions"
	"zookeeper-operator/util/informer"

	"github.com/sirupsen/logrus"
)

var initRetryWaitTime = 30 * time.Second

type Controller struct {
	logger *logrus.Entry

	client client.Client
}

func New(client client.Client) *Controller {
	return &Controller{
		logger: logrus.WithField("pkg", "controller"),
		client: client,
	}
}

func (c *Controller) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	resourceSyncer := c.NewZookeeperSyncer(ctx)
	resourceSyncer.Run(ctx, 5)

	// TODO: add a pod informer to watch related pods
}

func (c *Controller) NewZookeeperSyncer(ctx context.Context) *informer.ResourceSyncer {
	sharedInformerFactory := zkInformers.NewSharedInformerFactory(c.client.ZookeeperInterface(), time.Minute*2)

	zkInformer := sharedInformerFactory.Zookeeper().V1alpha1().ZookeeperClusters().Informer()
	groupVersionResource := v1alpha1.SchemeGroupVersion.WithResource("zookeeperclusters")

	resourceSyncer := informer.NewResourceSyncer(zkInformer, &groupVersionResource, c.logger,
		func(lister cache.GenericLister, adder informer.ResourceRateLimitingAdder) informer.Syncer {
			zkSyncer := ZookeeperSyncer{
				adder:  adder,
				lister: lister,
				client: c.client,
				logger: c.logger,
			}

			return informer.Syncer{
				Sync: zkSyncer.sync,
			}
		})

	return resourceSyncer
}

type ZookeeperSyncer struct {
	adder  informer.ResourceRateLimitingAdder
	lister cache.GenericLister
	client client.Client
	logger *logrus.Entry
}

func (z *ZookeeperSyncer) sync(obj interface{}) (bool, error) {
	sharedCluster := obj.(*api.ZookeeperCluster)
	sharedCluster = sharedCluster.DeepCopy()
	zkCluster := cluster.New(z.client.GetCRClient(sharedCluster.Namespace), sharedCluster)
	defer func() {
		if !zkCluster.IsFinished() {
			z.adder.AddAfter(sharedCluster, 30*time.Second)
		}
	}()

	start := time.Now()

	if sharedCluster.Status.Phase == api.ClusterPhaseNone {
		err := zkCluster.Create()
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
			z.logger.Errorf("cluster failed: %v", rerr)
			zkCluster.ReportFailedStatus()
		}
		return false, rerr
	}

	return true, nil
}
