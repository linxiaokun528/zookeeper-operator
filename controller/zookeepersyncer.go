package controller

import (
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime"
	"time"
	api "zookeeper-operator/apis/zookeeper/v1alpha1"
	"zookeeper-operator/client"
	"zookeeper-operator/util/informer"
	"zookeeper-operator/zkcluster"
)

type zookeeperSyncer struct {
	adder  informer.ResourceRateLimitingAdder
	client client.Client
	logger *logrus.Entry
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
