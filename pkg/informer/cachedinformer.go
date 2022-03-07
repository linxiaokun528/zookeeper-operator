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
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	sigcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"zookeeper-operator/pkg/util"
)

type ResourceEventHandlerBuilder func(lister cache.GenericLister, adder ResourceRateLimitingAdder) cache.ResourceEventHandlerFuncs

type CachedInformer interface {
	AddHandler(obj client.Object, resourceEventHandlerBuilder ResourceEventHandlerBuilder, workerNum int)
	GetLister(obj client.Object) cache.GenericLister
}

type cachedInformer struct {
	config *rest.Config
	cache  sigcache.Cache
	scheme *runtime.Scheme
	ctx    context.Context
}

func (r *cachedInformer) getGVR(obj client.Object) schema.GroupVersionResource {
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

func (r *cachedInformer) getInformer(obj client.Object) cache.SharedIndexInformer {
	informer, err := r.cache.GetInformer(r.ctx, obj)
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

func (r *cachedInformer) GetLister(obj client.Object) cache.GenericLister {
	informer := r.getInformer(obj)
	return cache.NewGenericLister(informer.GetIndexer(), r.getGVR(obj).GroupResource())
}

func (r *cachedInformer) AddHandler(obj client.Object, resourceEventHandlerBuilder ResourceEventHandlerBuilder, workerNum int) {
	informer := r.getInformer(obj)
	lister := r.GetLister(obj)
	gvr := r.getGVR(obj)
	handler := newCachedHandler(gvr, lister, resourceEventHandlerBuilder)
	informer.AddEventHandler(handler)
	go handler.Start(workerNum, r.ctx)
}

func NewCachedInformer(ctx context.Context, config *rest.Config, scheme *runtime.Scheme, resync time.Duration) (CachedInformer, error) {
	opt := sigcache.Options{
		Scheme: scheme,
		Resync: &resync,
	}

	cache, err := sigcache.New(config, opt)
	if err != nil {
		return nil, err
	}
	go cache.Start(ctx)

	return &cachedInformer{config: config, cache: cache, scheme: scheme, ctx: ctx}, nil
}

type cachedResourceEventHandler struct {
	eventQueue *rateLimitingTypeForEventQueue
	handler    cache.ResourceEventHandlerFuncs
	gvr        schema.GroupVersionResource
}

func newCachedHandler(gvr schema.GroupVersionResource, lister cache.GenericLister, resourceEventHandlerBuilder ResourceEventHandlerBuilder) *cachedResourceEventHandler {
	eventQueue := newRateLimitingQueue(lister, workqueue.DefaultControllerRateLimiter())
	adder := resourceRateLimitingAdder{
		eventQueue: eventQueue,
	}
	handler := resourceEventHandlerBuilder(lister, &adder)

	result := cachedResourceEventHandler{
		eventQueue: eventQueue,
		handler:    handler,
		gvr:        gvr,
	}

	return &result
}

func (i *cachedResourceEventHandler) Start(workerNum int, ctx context.Context) {
	defer utilruntime.HandleCrash()

	go func() {
		<-ctx.Done()
		i.eventQueue.ShutDown()
	}()

	producer := func() interface{} {
		klog.V(4).Infof("Try to get an event from %s queue...", i.formatedGVR())
		event, quit := i.eventQueue.Get()
		defer func() {
			if r := recover(); r != nil {
				// If errors happen in this function, the event won't be consumed. Need to mark it as done so that
				// it won't block following events.
				i.eventQueue.Done(event)
				panic(r)
			}
		}()
		klog.V(4).Infof("Got event %s from %s queue: %#v", event.getType(), i.formatedGVR(), event)
		if quit {
			return nil
		}

		return event
	}
	processor := util.NewParallelConsumingProcessor(producer, i.consume)
	processor.Start(workerNum, ctx)
}

func (i *cachedResourceEventHandler) formatedGVR() string {
	if i.gvr.Group == "" {
		return fmt.Sprintf("%s/%s", i.gvr.Version, i.gvr.Resource)
	} else {
		return fmt.Sprintf("%s/%s/%s", i.gvr.Group, i.gvr.Version, i.gvr.Resource)
	}
}

func (i *cachedResourceEventHandler) OnAdd(obj interface{}) {
	klog.V(2).Infof("Receive a creation event of %s for %s", i.formatedGVR(), objToKey(obj))
	if i.handler.AddFunc == nil {
		return
	}

	i.eventQueue.Add(newAdditionEvent(obj.(runtime.Object)))
}

func (i *cachedResourceEventHandler) OnUpdate(oldObj, newObj interface{}) {
	klog.V(2).Infof("Receive a update event of %s for %s", i.formatedGVR(), objToKey(newObj))
	if i.handler.UpdateFunc == nil {
		return
	}

	i.eventQueue.Add(newUpdateEvent(oldObj.(runtime.Object), newObj.(runtime.Object)))
}

func (i *cachedResourceEventHandler) OnDelete(obj interface{}) {
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if ok {
		obj = tombstone.Obj
	}
	klog.V(2).Infof("Receive a deletion event of %s for %s", i.formatedGVR(), objToKey(obj))

	if i.handler.DeleteFunc == nil {
		return
	}

	i.eventQueue.Add(newDeletionEvent(obj.(runtime.Object)))
}

func (i *cachedResourceEventHandler) consume(item interface{}) {
	if item == nil {
		return
	}

	event := item.(*event)
	defer i.eventQueue.Done(event)

	if event.getType() == watch.Deleted {
		i.handler.OnDelete(event.oldObj)
	} else {
		// Actually, I don't think 'event.newObj.(metav1.Object).GetDeletionTimestamp() != nil' will happen.
		if event.newObj == nil || event.newObj.(metav1.Object).GetDeletionTimestamp() != nil {
			return
		}

		if event.getType() == watch.Modified {
			i.handler.OnUpdate(event.oldObj, event.newObj)
		} else {
			i.handler.OnAdd(event.newObj)
		}
	}
}
