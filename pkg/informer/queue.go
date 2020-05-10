package informer

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type Consumer func(obj interface{}) (bool, error)

type ConsumingQueue struct {
	workqueue.RateLimitingInterface
	consumer Consumer

	wait sync.WaitGroup
}

func NewConsumingQueue(queue workqueue.RateLimitingInterface, consumer Consumer) *ConsumingQueue {
	return &ConsumingQueue{
		RateLimitingInterface: queue,
		consumer:              consumer,
		wait:                  sync.WaitGroup{},
	}
}

func (c *ConsumingQueue) Start(consumerNum int, stopCh <-chan struct{}) {
	c.wait.Add(consumerNum)
	for i := 0; i < consumerNum; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (c *ConsumingQueue) worker() {
	defer c.wait.Done()
	for c.processNextWorkItem() {
	}
}

// ProcessNextWorkItem processes next item in queue by consumer
func (c *ConsumingQueue) processNextWorkItem() bool {
	obj, quit := c.Get()
	if quit {
		return false
	}
	defer c.Done(obj)

	key := obj.(string)
	forget, err := c.consumer(key)
	if err == nil {
		if forget {
			c.Forget(key)
		}
		return true
	}

	utilruntime.HandleError(fmt.Errorf("Error syncing %v: %v.", key, err))
	c.AddRateLimited(key)

	return true
}

func (c *ConsumingQueue) ShutDown() {
	c.RateLimitingInterface.ShutDown()
	c.wait.Wait()
}

func objToKey(obj interface{}) string {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		panic(fmt.Sprintf("Couldn't get key for object %+v: %v", obj, err))
	}
	return key
}

type eventConsumingQueue struct {
	queue       *ConsumingQueue
	lister      cache.GenericLister
	deletedObjs map[string]runtime.Object
}

func (e *eventConsumingQueue) Len() int {
	return e.queue.Len()
}

func (e *eventConsumingQueue) ShutDown() {
	e.queue.ShutDown()
}

func (e *eventConsumingQueue) ShuttingDown() bool {
	return e.queue.ShuttingDown()
}

func (e *eventConsumingQueue) Add(resource_event *event) {
	e.queue.Add(e.encode(resource_event))
}

func (e *eventConsumingQueue) AddAfter(resource_event *event, duration time.Duration) {
	e.queue.AddAfter(e.encode(resource_event), duration)
}

func (e *eventConsumingQueue) AddRateLimited(resource_event *event) {
	e.queue.AddRateLimited(e.encode(resource_event))
}

func (e *eventConsumingQueue) Forget(resource_event *event) {
	e.queue.Forget(e.encode(resource_event))
}

func (e *eventConsumingQueue) NumRequeues(resource_event *event) int {
	return e.queue.NumRequeues(e.encode(resource_event))
}

func (e *eventConsumingQueue) Get() (resource_event *event, shutdown bool) {
	item, shutdown := e.queue.Get()

	if item == nil {
		return nil, shutdown
	}

	return e.decode(item.(string)), shutdown
}

func (e *eventConsumingQueue) Done(resource_event *event) {
	e.queue.Done(e.encode(resource_event))
}

func (e *eventConsumingQueue) Start(consumerNum int, stopCh <-chan struct{}) {
	e.queue.Start(consumerNum, stopCh)
}

func (e *eventConsumingQueue) encode(resource_event *event) string {
	key := objToKey(resource_event.obj)
	if resource_event.Type == watch.Deleted {
		e.deletedObjs[key] = resource_event.obj
	}
	return fmt.Sprintf("%s|%s", resource_event.Type, key)
}

func (e *eventConsumingQueue) decode(event_key string) *event {
	parts := strings.Split(event_key, "|")
	if len(parts) != 2 {
		panic(fmt.Sprintf("Invalid event_key for eventConsumingQueue: %s", event_key))
	}

	event_type := watch.EventType(parts[0])
	obj_key := parts[1]

	var obj runtime.Object
	if event_type == watch.Deleted {
		obj = e.deletedObjs[obj_key]
		delete(e.deletedObjs, obj_key)
	} else {
		ns, name, err := cache.SplitMetaNamespaceKey(obj_key)

		if err != nil {
			panic(err)
		}

		if ns == "" {
			obj, err = e.lister.Get(name)
		} else {
			obj, err = e.lister.ByNamespace(ns).Get(name)
		}

		// TODO: there is a chance that the object is deleted when doing the decode, so the error might be
		// something like "zookeepercluster.zookeeper.database.apache.com \"example-zookeeper-cluster\" not found".
		// Need to deal with this kind of situation in the future.
		if err != nil {
			panic(err)
		}
	}
	return &event{
		Type: event_type,
		obj:  obj,
	}
}

func newEventConsumingQueue(lister cache.GenericLister, consumer Consumer) *eventConsumingQueue {
	eventQueue := eventConsumingQueue{
		lister:      lister,
		deletedObjs: map[string]runtime.Object{},
	}
	consumingQueue := NewConsumingQueue(
		workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()), func(obj interface{}) (bool, error) {
			return consumer(eventQueue.decode(obj.(string)))
		})
	eventQueue.queue = consumingQueue

	return &eventQueue
}

type ResourceRateLimitingAdder interface {
	Add(item runtime.Object)
	Len() int
	// AddAfter adds an item to the workqueue after the indicated duration has passed
	AddAfter(item runtime.Object, duration time.Duration)

	// AddRateLimited adds an item to the workqueue after the rate limiter says it's ok
	AddRateLimited(item runtime.Object)

	// NumRequeues returns back how many times the item was requeued
	NumRequeues(item runtime.Object) int

	// We don't want users to invoke the following APIs
	// Forget(item runtime.Object)
	// Get() (item runtime.Object, shutdown bool)
	// Done(item runtime.Object)
	// ShutDown()
	// ShuttingDown() bool
}

type event struct {
	Type watch.EventType
	obj  runtime.Object
}

type resourceRateLimitingAdder struct {
	eventQueue *eventConsumingQueue
}

func (r *resourceRateLimitingAdder) toEvent(item runtime.Object) *event {
	return &event{
		Type: watch.Modified,
		obj:  item,
	}
}

func (r *resourceRateLimitingAdder) Add(item runtime.Object) {
	r.eventQueue.Add(r.toEvent(item))
}

func (r *resourceRateLimitingAdder) AddAfter(item runtime.Object, duration time.Duration) {
	r.eventQueue.AddAfter(r.toEvent(item), duration)
}

func (r *resourceRateLimitingAdder) AddRateLimited(item runtime.Object) {
	r.eventQueue.AddRateLimited(r.toEvent(item))
}

func (r *resourceRateLimitingAdder) Forget(item runtime.Object) {
	r.eventQueue.Forget(r.toEvent(item))
}

// NumRequeues returns back how many times the item was requeued
func (r *resourceRateLimitingAdder) NumRequeues(item runtime.Object) int {
	return r.eventQueue.NumRequeues(r.toEvent(item))
}

// NumRequeues returns back how many times the item was requeued
func (r *resourceRateLimitingAdder) Len() int {
	return r.eventQueue.Len()
}
