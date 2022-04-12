package controller

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	apis "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
)

type zkPodPredicate struct {
	client client.Client
	ctx    context.Context
}

func NewZkPodPredicate(ctx context.Context) *zkPodPredicate {
	return &zkPodPredicate{ctx: ctx}
}

func (z *zkPodPredicate) isPodFromZkController(pod *v1.Pod) bool {
	for _, ref := range pod.GetOwnerReferences() {
		refGV, err := schema.ParseGroupVersion(ref.APIVersion)
		if err != nil {
			panic(err)
		}

		if ref.Kind == apis.Kind && refGV.Group == apis.GroupName && refGV.Version == apis.Version {
			zkCluster := apis.ZookeeperCluster{}
			// This method will block the following event processing, we need to make it return as soon as possible.
			// So we need to make sure client is a cached client.
			err := z.client.Get(z.ctx, client.ObjectKey{
				Namespace: pod.Namespace,
				Name:      ref.Name,
			}, &zkCluster)
			if err != nil {
				return false
			}
			if zkCluster.UID != ref.UID {
				// The controller we found with this Name is not the same one that the
				// ControllerRef points to.
				return false
			}
			return true
		}
	}

	return false
}

func (z *zkPodPredicate) Create(event.CreateEvent) bool {
	return false
}

func (z *zkPodPredicate) Delete(e event.DeleteEvent) bool {
	return z.isPodFromZkController(e.Object.(*v1.Pod))
}

func (z *zkPodPredicate) Update(event.UpdateEvent) bool {
	return false
}

func (z *zkPodPredicate) Generic(event.GenericEvent) bool {
	return false
}

func (p *zkPodPredicate) InjectClient(c client.Client) error {
	p.client = c
	return nil
}
