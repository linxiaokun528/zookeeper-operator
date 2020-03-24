package controller

import (
	"k8s.io/apimachinery/pkg/runtime"
	"time"
	"zookeeper-operator/internal/util/k8sclient"
	"zookeeper-operator/internal/zkcluster"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	"zookeeper-operator/pkg/informer"
)

type zookeeperSyncer struct {
	adder  informer.ResourceRateLimitingAdder
	client k8sclient.Client
}

func (z *zookeeperSyncer) sync(obj runtime.Object) (bool, error) {
	cr := obj.(*api.ZookeeperCluster)
	zkCluster := zkcluster.New(z.client.GetCRClient(cr.Namespace), cr)
	defer func() {
		if !zkCluster.IsFinished() {
			z.adder.AddAfter(cr, 30*time.Second)
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
