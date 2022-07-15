package controller

import (
	"context"
	"time"

	"github.com/linxiaokun528/go-kit/pkg/util/collection"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"zookeeper-operator/internal/zkcluster"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
)

const retryWaitTime = 30 * time.Second

type ZookeeperReconciler struct {
	client       client.Client
	podsToDelete collection.Collection[string]
}

func NewZookeeperReconciler(podsToDelete collection.Collection[string]) *ZookeeperReconciler {
	return &ZookeeperReconciler{
		podsToDelete: podsToDelete,
	}
}

func (z *ZookeeperReconciler) Reconcile(ctx context.Context,
	req reconcile.Request) (result reconcile.Result, err error) {
	cr := api.ZookeeperCluster{}
	err = z.client.Get(ctx, req.NamespacedName, &cr) // see if the client will support generics in the future
	if apierrors.IsNotFound(err) {
		return result, nil
	}
	zkCluster := zkcluster.New(ctx, z.client, &cr, z.podsToDelete)

	defer func() {
		if err == nil && !zkCluster.IsFinished() {
			result.RequeueAfter = retryWaitTime
		}
	}()

	err = zkCluster.SyncAndUpdateStatus()

	if err != nil {
		// todo: test if we need this error message
		klog.Errorf("Error happend when syncing zookeeper cluster %s: %s", cr.GetNamespacedName(), err.Error())
	}

	return result, err
}

func (z *ZookeeperReconciler) InjectClient(c client.Client) error {
	z.client = c
	return nil
}
