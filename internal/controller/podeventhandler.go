package controller

import (
	"context"

	"gopkg.in/fatih/set.v0"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"

	"k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"zookeeper-operator/pkg/k8sutil"
)

type zkPodEventHandler struct {
	podsToDelete    set.Interface
	client          client.Client
	ctx             context.Context
	handlerForOwner handler.EnqueueRequestForOwner
}

func NewZkPodEventHandler(ctx context.Context, podsToDelete set.Interface) *zkPodEventHandler {
	return &zkPodEventHandler{
		podsToDelete: podsToDelete,
		ctx:          ctx,
		handlerForOwner: handler.EnqueueRequestForOwner{
			OwnerType:    &api.ZookeeperCluster{},
			IsController: true,
		},
	}
}

func (p *zkPodEventHandler) Delete(event event.DeleteEvent, queue workqueue.RateLimitingInterface) {
	pod := event.Object.(*v1.Pod)
	if p.podsToDelete.Has(k8sutil.GetNamespacedName(pod)) {
		p.podsToDelete.Remove(k8sutil.GetNamespacedName(pod))
		return
	} else {
		klog.Infof("Pod deletion observed: %s/%s", pod.Namespace, pod.Name)

		pod.DeletionTimestamp = nil
		pod.ResourceVersion = ""

		// this method will block the following processing of events, we need to make it return as fast as possible
		go func() {
			// Don't need to worry if the corresponding zookeeper cluster is deleted.
			// Even if we create the pod after the zookeeper cluster is deleted,
			// the pod will be terminated automatically anyway.
			err := p.client.Create(p.ctx, pod)
			if err != nil {
				klog.Error(err)
				p.handlerForOwner.Delete(event, queue)
			} else {
				klog.Infof("Deleted pod %s/%s added back.", pod.Namespace, pod.Name)
			}
		}()
	}
}

func (p *zkPodEventHandler) Create(event.CreateEvent, workqueue.RateLimitingInterface) {}

func (p *zkPodEventHandler) Update(event.UpdateEvent, workqueue.RateLimitingInterface) {}

func (p *zkPodEventHandler) Generic(event.GenericEvent, workqueue.RateLimitingInterface) {}

func (p *zkPodEventHandler) InjectClient(c client.Client) error {
	p.client = c
	return nil
}
