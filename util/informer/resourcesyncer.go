package informertype

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

type syncFunc func(obj interface{}) (bool, error)

type Syncer struct {
	synce  syncFunc
	delete syncFunc
}

type NewSyncerFunc func(lister cache.GenericLister, adder ResourceRateLimitingAdder) Syncer

type ResourceSyncer struct {
	eventQueue *eventConsumingQueue
	syncer     Syncer

	resource *schema.GroupVersionResource
	informer cache.SharedIndexInformer
	lister   cache.GenericLister

	logger *logrus.Entry
}

func NewResourceSyncer(informer cache.SharedIndexInformer, resource *schema.GroupVersionResource,
	logger *logrus.Entry, newSyncerFunc NewSyncerFunc) *ResourceSyncer {
	lister := cache.NewGenericLister(informer.GetIndexer(), resource.GroupResource())

	result := ResourceSyncer{
		resource: resource,
		informer: informer,
		logger:   logger,
		lister:   lister,
	}

	eventQueue := newEventConsumingQueue("events", result.consume)
	adder := resourceRateLimitingAdder{
		eventQueue: eventQueue,
		keyGetter:  result.objToKey,
	}
	result.syncer = newSyncerFunc(lister, &adder)
	result.eventQueue = eventQueue

	return &result
}

func (i *ResourceSyncer) objToKey(obj interface{}) string {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		panic(fmt.Sprintf("Couldn't get key for object %+v: %v", obj, err))
	}
	return key
}

func (i *ResourceSyncer) Run(ctx context.Context, workerNum int) {
	defer utilruntime.HandleCrash()
	defer i.eventQueue.ShutDown()

	i.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    i.onAdd,
		UpdateFunc: i.onUpdate,
		DeleteFunc: i.onDelete,
	})

	if !cache.WaitForNamedCacheSync("zookeeper", ctx.Done(), i.informer.HasSynced) {
		return
	}

	i.eventQueue.Consume(workerNum)

	i.informer.Run(ctx.Done())
}

func (i *ResourceSyncer) onAdd(obj interface{}) {
	i.eventQueue.Add(&event{
		Type: watch.Added,
		key:  i.objToKey(obj),
	})
}

func (i *ResourceSyncer) onUpdate(oldObj, newObj interface{}) {
	i.eventQueue.Add(&event{
		Type: watch.Modified,
		key:  i.objToKey(newObj),
	})
}

func (i *ResourceSyncer) onDelete(obj interface{}) {
	if i.syncer.delete != nil {
		i.eventQueue.Add(&event{
			Type: watch.Deleted,
			key:  i.objToKey(obj),
		})
	}
}

func (i *ResourceSyncer) consume(item interface{}) (bool, error) {
	event := item.(event)
	if event.Type == watch.Deleted {
		return i.syncer.delete(event.key)
	} else {
		ns, name, err := cache.SplitMetaNamespaceKey(event.key)

		if err != nil {
			return false, err
		}

		if len(ns) == 0 || len(name) == 0 {
			return false, fmt.Errorf("invalid key %v: either namespace or name is missing", event.key)
		}
		obj, err := i.lister.ByNamespace(ns).Get(name)
		if err != nil {
			return false, errors.NewNotFound(i.resource.GroupResource(), name)
		}
		if obj.(metav1.Object).GetDeletionTimestamp() != nil {
			return true, nil
		}

		return i.syncer.synce(obj.DeepCopyObject())
	}
}
