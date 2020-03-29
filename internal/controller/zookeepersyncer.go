package controller

import (
	"time"

	"k8s.io/apimachinery/pkg/runtime"

	client2 "zookeeper-operator/internal/client"
	"zookeeper-operator/internal/zkcluster"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	"zookeeper-operator/pkg/informer"
)

const retryWaitTime = 30 * time.Second

type zookeeperSyncer struct {
	adder  informer.ResourceRateLimitingAdder
	client client2.Client
}

func (z *zookeeperSyncer) sync(obj runtime.Object) (bool, error) {
	cr := obj.(*api.ZookeeperCluster)
	zkCluster := zkcluster.New(z.client.GetCRClient(cr.Namespace), cr)
	defer func() {
		if !zkCluster.IsFinished() {
			z.adder.AddAfter(cr, retryWaitTime)
		}
	}()

	start := time.Now()

	rerr := zkCluster.SyncAndUpdateStatus()

	zkcluster.ReconcileHistogram.WithLabelValues(cr.Name).Observe(
		time.Since(start).Seconds())

	if rerr != nil {
		zkcluster.ReconcileFailed.WithLabelValues(rerr.Error()).Inc()
		return false, rerr
	}

	return true, nil
}
