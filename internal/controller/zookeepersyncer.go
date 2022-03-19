package controller

import (
	"context"
	"time"

	"gopkg.in/fatih/set.v0"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	"k8s.io/klog/v2"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	ctx          context.Context
}

func (z *zookeeperSyncer) sync(obj interface{}) {
	cr := obj.(*api.ZookeeperCluster)
	zkClient := z.client.GetCRClient(cr.Namespace)
	zkCluster := zkcluster.New(z.ctx, zkClient, cr, z.podLister, z.podsToDelete)
	var err error = nil
	defer func() {
		if err == nil && !zkCluster.IsFinished() {
			z.adder.AddAfter(informer.NewUpdateEvent(cr, cr), retryWaitTime)
		}
	}()

	start := time.Now()

	err = zkCluster.SyncAndUpdateStatus()

	zkcluster.ReconcileHistogram.WithLabelValues(cr.Name).Observe(
		time.Since(start).Seconds())

	if err != nil {
		klog.Errorf("Error happend when syncing zookeeper cluster %s: %s", cr.GetFullName(), err.Error())
		zkcluster.ReconcileFailed.WithLabelValues(err.Error()).Inc()
		cr, err = zkClient.ZookeeperCluster().Get(z.ctx, cr.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return
		} else {
			klog.Errorf("Error happend when getting zookeeper cluster %s: %s", cr.GetFullName(), err.Error())
		}

		// When use rate limited, always try to use the newest cr, or we may keep updating the same outmoded cr
		// infinitely
		// for example:
		// time1: create cr(version1)
		//        typeForEventQueue.processing: creation event
		//        typeForEventQueue.dirty: <empty>
		//        DelayingExecutor.priorityQueue: <empty>
		// time2: delete cr before the above creation finishes.(version2)
		//        typeForEventQueue.processing: creation event
		//        typeForEventQueue.dirty: deletion event
		//        DelayingExecutor.priorityQueue: <empty>
		// time3: zkCluster.SyncAndUpdateStatus() fails because the CR has been deleted, and put an update event
		//            into typeForEventQueue through AddRateLimited
		//        typeForEventQueue.processing: creation event
		//        typeForEventQueue.dirty: deletion event
		//        DelayingExecutor.priorityQueue: update event1
		// time4: creation is marked Done
		//        typeForEventQueue.processing: deletion event(moved from typeForEventQueue.dirty)
		//        typeForEventQueue.dirty: <empty>
		//        DelayingExecutor.priorityQueue: update event1
		// time5: deletion is marked Done
		//        typeForEventQueue.processing: <empty>
		//        typeForEventQueue.dirty: <empty>
		//        DelayingExecutor.priorityQueue: update event1
		// time6: time is up, update event1 is moved into event queue
		//        typeForEventQueue.processing: update event1
		//        typeForEventQueue.dirty: <empty>
		//        DelayingExecutor.priorityQueue: <empty>
		// time7: zkCluster.SyncAndUpdateStatus() fails because the CR has been deleted,
		//            and put another update event into typeForEventQueue through AddRateLimited
		//        typeForEventQueue.processing: update event1
		//        typeForEventQueue.dirty: <empty>
		//        DelayingExecutor.priorityQueue: update event2
		// time8: update1 is marked done
		//        typeForEventQueue.processing: <empty>
		//        typeForEventQueue.dirty: <empty>
		//        DelayingExecutor.priorityQueue: update event2
		// time9: time is up, update event2 is moved into event queue
		//        typeForEventQueue.processing: update event2
		//        typeForEventQueue.dirty: <empty>
		//        DelayingExecutor.priorityQueue: <empty>
		// loop between time time6 and time9
		z.adder.AddRateLimited(informer.NewUpdateEvent(cr, cr))
	} else {
		z.adder.Forget(informer.NewUpdateEvent(cr, cr))
	}

}
