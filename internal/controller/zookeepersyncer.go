package controller

import (
	"time"

	"gopkg.in/fatih/set.v0"
	"k8s.io/client-go/tools/cache"

	"k8s.io/klog"

	client2 "zookeeper-operator/internal/client"
	"zookeeper-operator/internal/zkcluster"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	"zookeeper-operator/pkg/informer"
)

const retryWaitTime = 30 * time.Second

type zookeeperSyncer struct {
	adder        informer.ResourceRateLimitingAdder
	client       client2.Client
	podLister    cache.GenericLister
	podsToDelete set.Interface
}

func (z *zookeeperSyncer) sync(obj interface{}) {
	cr := obj.(*api.ZookeeperCluster)
	zkCluster := zkcluster.New(z.client.GetCRClient(cr.Namespace), cr, z.podLister, z.podsToDelete)
	var err error = nil
	defer func() {
		if err == nil && !zkCluster.IsFinished() {
			z.adder.AddAfter(cr, retryWaitTime)
		}
	}()

	start := time.Now()

	err = zkCluster.SyncAndUpdateStatus()

	zkcluster.ReconcileHistogram.WithLabelValues(cr.Name).Observe(
		time.Since(start).Seconds())

	if err != nil {
		klog.Errorf("Error happend when syncing zookeeper cluster %s: %s", cr.GetFullName(), err.Error())
		zkcluster.ReconcileFailed.WithLabelValues(err.Error()).Inc()
		z.adder.AddRateLimited(cr)
	} else {
		z.adder.Forget(cr)
	}

}
