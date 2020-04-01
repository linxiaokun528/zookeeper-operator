package informer

import (
	"context"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

type syncFunc func(obj runtime.Object) (bool, error)

type Syncer struct {
	Sync   syncFunc
	Delete syncFunc
}

type ResourceSyncerFactory interface {
	ResourceSyncer(obj runtime.Object, newSyncerFunc NewSyncerFunc) ResourceSyncer
	Start(stopCh <-chan struct{}) error
}

type NewSyncerFunc func(lister cache.GenericLister, adder ResourceRateLimitingAdder) Syncer

type resourceSyncerFactory struct {
	config *rest.Config
	cache  sigcache.Cache
	scheme *runtime.Scheme
}

func (r *resourceSyncerFactory) ResourceSyncer(obj runtime.Object,
	newSyncerFunc NewSyncerFunc) ResourceSyncer {

	gvk, err := apiutil.GVKForObject(obj, r.scheme)
	if err != nil {
		panic(err)
	}

	gvr := schema.GroupVersionResource{
		Group:    gvk.Group,
		Version:  gvk.Version,
		Resource: strings.ToLower(gvk.Kind),
	}

	i, err := r.cache.GetInformer(obj)
	if err != nil {
		panic(err)
	}

	return newResourceSyncer(i, gvr, newSyncerFunc)
}

func (r *resourceSyncerFactory) Start(stopCh <-chan struct{}) error {
	return r.cache.Start(stopCh)
}

func NewResourceSyncerFactory(config *rest.Config, scheme *runtime.Scheme,
	resync time.Duration) (ResourceSyncerFactory, error) {
	opt := sigcache.Options{
		Scheme: scheme,
		Resync: &resync,
	}

	cache, err := sigcache.New(config, opt)
	if err != nil {
		return nil, err
	}

	return &resourceSyncerFactory{config: config, cache: cache, scheme: scheme}, nil
}

type ResourceSyncer interface {
	Run(workerNum int, stopCh <-chan struct{})
}

type resourceSyncer struct {
	eventQueue *eventConsumingQueue
	syncer     Syncer

	gvr      schema.GroupVersionResource
	informer sigcache.Informer
	lister   cache.GenericLister
}

func newResourceSyncer(informer sigcache.Informer, gvr schema.GroupVersionResource,
	newSyncerFunc NewSyncerFunc) *resourceSyncer {
	// TODO: this is quite "hack". We should convert sigcache.reader into cache.GenericLister
	lister := cache.NewGenericLister(informer.(cache.SharedIndexInformer).GetIndexer(), gvr.GroupResource())

	result := resourceSyncer{
		gvr:      gvr,
		informer: informer,
		lister:   lister,
	}

	eventQueue := newEventConsumingQueue(result.consume)
	adder := resourceRateLimitingAdder{
		eventQueue: eventQueue,
		keyGetter:  result.objToKey,
	}
	result.syncer = newSyncerFunc(lister, &adder)
	result.eventQueue = eventQueue
	return &result
}

func (i *resourceSyncer) objToKey(obj interface{}) string {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		panic(fmt.Sprintf("Couldn't get key for object %+v: %v", obj, err))
	}
	return key
}

func (i *resourceSyncer) Run(workerNum int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer i.eventQueue.ShutDown()

	if !cache.WaitForNamedCacheSync(
		fmt.Sprintf("%v", i.gvr),
		context.TODO().Done(), i.informer.HasSynced) {
		return
	}

	i.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    i.onAdd,
		UpdateFunc: i.onUpdate,
		DeleteFunc: i.onDelete,
	})

	i.eventQueue.Start(workerNum, stopCh)
}

func (i *resourceSyncer) onAdd(obj interface{}) {
	klog.Info(fmt.Sprintf("Receive a creation event for %s", i.objToKey(obj)))
	i.eventQueue.Add(&event{
		Type: watch.Modified,
		key:  i.objToKey(obj),
	})
}

func (i *resourceSyncer) onUpdate(oldObj, newObj interface{}) {
	klog.Info(fmt.Sprintf("Receive a update event for %s", i.objToKey(newObj)))
	i.eventQueue.Add(&event{
		Type: watch.Modified,
		key:  i.objToKey(newObj),
	})
}

func (i *resourceSyncer) onDelete(obj interface{}) {
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if ok {
		obj = tombstone.Obj
	}
	klog.Info(fmt.Sprintf("Receive a deletion event for %s", i.objToKey(obj)))

	if i.syncer.Delete != nil {
		i.eventQueue.Add(&event{
			Type: watch.Deleted,
			key:  i.objToKey(obj),
		})
	}
}

func (i *resourceSyncer) consume(item interface{}) (bool, error) {
	event := item.(*event)
	ns, name, err := cache.SplitMetaNamespaceKey(event.key)

	if err != nil {
		return false, err
	}

	var obj runtime.Object
	if ns == "" {
		obj, err = i.lister.Get(name)
	} else {
		obj, err = i.lister.ByNamespace(ns).Get(name)
	}

	if event.Type == watch.Deleted {
		return i.syncer.Delete(obj)
	} else {
		if err != nil {
			return false, apierrors.NewNotFound(i.gvr.GroupResource(), name)
		}
		if obj.(metav1.Object).GetDeletionTimestamp() != nil {
			return true, nil
		}

		return i.syncer.Sync(obj.DeepCopyObject())
	}
}
