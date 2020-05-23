package informer

import (
	"context"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	sigcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type ResourceEventHandlerBuilder func(lister cache.GenericLister, adder ResourceRateLimitingAdder) cache.ResourceEventHandlerFuncs

type CachedInformer interface {
	AddHandler(obj runtime.Object, resourceEventHandlerBuilder ResourceEventHandlerBuilder, workerNum int)
	GetLister(obj runtime.Object) cache.GenericLister
}

type cachedInformer struct {
	config *rest.Config
	cache  sigcache.Cache
	scheme *runtime.Scheme
	ctx    context.Context
}

func (r *cachedInformer) getGVR(obj runtime.Object) schema.GroupVersionResource {
	gvk, err := apiutil.GVKForObject(obj, r.scheme)
	if err != nil {
		panic(err)
	}

	return schema.GroupVersionResource{
		Group:    gvk.Group,
		Version:  gvk.Version,
		Resource: strings.ToLower(gvk.Kind),
	}
}

func (r *cachedInformer) getInformer(obj runtime.Object) cache.SharedIndexInformer {
	informer, err := r.cache.GetInformer(obj)
	if err != nil {
		panic(err)
	}

	gvr := r.getGVR(obj)

	if !cache.WaitForCacheSync(
		r.ctx.Done(), informer.HasSynced) {
		panic(fmt.Errorf("Unable to sync cache for %v", gvr))
	}

	return informer.(cache.SharedIndexInformer)
}

func (r *cachedInformer) GetLister(obj runtime.Object) cache.GenericLister {
	informer := r.getInformer(obj)
	return cache.NewGenericLister(informer.(cache.SharedIndexInformer).GetIndexer(), r.getGVR(obj).GroupResource())
}

func (r *cachedInformer) AddHandler(obj runtime.Object, resourceEventHandlerBuilder ResourceEventHandlerBuilder, workerNum int) {
	informer := r.getInformer(obj)
	lister := r.GetLister(obj)
	gvr := r.getGVR(obj)
	handler := newCachedHandler(gvr, lister, resourceEventHandlerBuilder)
	informer.AddEventHandler(handler)
	go handler.Start(workerNum, r.ctx.Done())
}

func NewCachedInformer(ctx context.Context, config *rest.Config, scheme *runtime.Scheme, resync time.Duration) (CachedInformer, error) {
	opt := sigcache.Options{
		Scheme: scheme,
		Resync: &resync,
	}

	cache, err := sigcache.New(config, opt)
	go cache.Start(ctx.Done())
	if err != nil {
		return nil, err
	}

	return &cachedInformer{config: config, cache: cache, scheme: scheme, ctx: ctx}, nil
}

type cachedResourceEventHandler struct {
	eventQueue *eventConsumingQueue
	handler    cache.ResourceEventHandlerFuncs
	gvr        schema.GroupVersionResource
}

func newCachedHandler(gvr schema.GroupVersionResource, lister cache.GenericLister, resourceEventHandlerBuilder ResourceEventHandlerBuilder) *cachedResourceEventHandler {
	result := cachedResourceEventHandler{gvr: gvr}

	eventQueue := newEventConsumingQueue(lister, result.consume)
	adder := resourceRateLimitingAdder{
		eventQueue: eventQueue,
	}
	handler := resourceEventHandlerBuilder(lister, &adder)

	result.handler = handler
	result.eventQueue = eventQueue

	return &result
}

func (i *cachedResourceEventHandler) Start(workerNum int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	i.eventQueue.Start(workerNum, stopCh)
}

func (i *cachedResourceEventHandler) formatedGVR() string {
	if i.gvr.Group == "" {
		return fmt.Sprintf("%s/%s", i.gvr.Version, i.gvr.Resource)
	} else {
		return fmt.Sprintf("%s/%s/%s", i.gvr.Group, i.gvr.Version, i.gvr.Resource)
	}
}

func (i *cachedResourceEventHandler) OnAdd(obj interface{}) {
	klog.V(4).Infof("Receive a creation event of %s for %s", i.formatedGVR(), objToKey(obj))
	if i.handler.AddFunc == nil {
		return
	}

	i.eventQueue.Add(&event{
		eventType: watch.Added,
		newObj:    obj.(runtime.Object),
	})
}

func (i *cachedResourceEventHandler) OnUpdate(oldObj, newObj interface{}) {
	klog.V(4).Infof("Receive a update event of %s for %s", i.formatedGVR(), objToKey(newObj))
	if i.handler.UpdateFunc == nil {
		return
	}

	i.eventQueue.Add(&event{
		eventType: watch.Modified,
		oldObj:    oldObj.(runtime.Object),
		newObj:    newObj.(runtime.Object),
	})

}

func (i *cachedResourceEventHandler) OnDelete(obj interface{}) {
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if ok {
		obj = tombstone.Obj
	}
	klog.V(4).Infof("Receive a deletion event of %s for %s", i.formatedGVR(), objToKey(obj))

	if i.handler.DeleteFunc == nil {
		return
	}

	i.eventQueue.Add(&event{
		eventType: watch.Deleted,
		newObj:    obj.(runtime.Object),
	})

}

func (i *cachedResourceEventHandler) consume(item interface{}) {
	event := item.(*event)

	if event.eventType == watch.Deleted {
		i.handler.OnDelete(event.newObj)
	} else {
		// Actually, I don't think this will happen.
		if event.newObj.(metav1.Object).GetDeletionTimestamp() != nil {
			return
		}

		if event.eventType == watch.Modified {
			i.handler.OnUpdate(event.oldObj, event.newObj)
		} else {
			i.handler.OnAdd(event.newObj)
		}
	}
}
