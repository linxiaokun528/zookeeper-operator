package informer

import (
	"fmt"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type Consumer func(obj interface{})

type ConsumingQueue struct {
	// Don't just use "workqueue.RateLimitingInterface" here, because we don't want to inherit the all functions of
	// workqueue.RateLimitingInterface: We don't want users to invoke "workqueue.RateLimitingInterface.ShutDown".
	queue    workqueue.RateLimitingInterface
	consumer Consumer

	wait sync.WaitGroup
}

func NewConsumingQueue(queue workqueue.RateLimitingInterface, consumer Consumer) *ConsumingQueue {
	return &ConsumingQueue{
		queue:    queue,
		consumer: consumer,
		wait:     sync.WaitGroup{},
	}
}

func (c *ConsumingQueue) Start(consumerNum int, stopCh <-chan struct{}) {
	c.wait.Add(consumerNum)
	for i := 0; i < consumerNum; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}
	<-stopCh
	c.shutDown()
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
	c.consumer(obj)

	return true
}

// Don't invoke this function directly. Use the stopCh parameter in function "Start" to control when to shutDown.
func (c *ConsumingQueue) shutDown() {
	c.queue.ShutDown()
	c.wait.Wait()
}

func (c *ConsumingQueue) Len() int {
	return c.queue.Len()
}

func (c *ConsumingQueue) ShuttingDown() bool {
	return c.queue.ShuttingDown()
}

func (c *ConsumingQueue) Add(item interface{}) {
	c.queue.Add(item)
}

func (c *ConsumingQueue) AddAfter(item interface{}, duration time.Duration) {
	c.queue.AddAfter(item, duration)
}

func (c *ConsumingQueue) AddRateLimited(item interface{}) {
	c.queue.AddRateLimited(item)
}

func (c *ConsumingQueue) Forget(item interface{}) {
	c.queue.Forget(item)
}

func (c *ConsumingQueue) NumRequeues(item interface{}) int {
	return c.queue.NumRequeues(item)
}

func (c *ConsumingQueue) Get() (item interface{}, shutdown bool) {
	return c.queue.Get()
}

func (c *ConsumingQueue) Done(item interface{}) {
	c.queue.Done(item)
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
	oldObjs     map[string]runtime.Object
	lock        sync.Mutex
}

func (e *eventConsumingQueue) Len() int {
	return e.queue.Len()
}

func (e *eventConsumingQueue) ShuttingDown() bool {
	return e.queue.ShuttingDown()
}

func (e *eventConsumingQueue) Add(resource_event *event) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.queue.Add(e.encode(resource_event, true))
}

func (e *eventConsumingQueue) AddAfter(resource_event *event, duration time.Duration) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.queue.AddAfter(e.encode(resource_event, true), duration)
}

func (e *eventConsumingQueue) AddRateLimited(resource_event *event) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.queue.AddRateLimited(e.encode(resource_event, true))
}

func (e *eventConsumingQueue) Forget(resource_event *event) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.queue.Forget(e.encode(resource_event, false))
}

func (e *eventConsumingQueue) NumRequeues(resource_event *event) int {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.queue.NumRequeues(e.encode(resource_event, false))
}

func (e *eventConsumingQueue) Done(resource_event *event) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.queue.Done(e.encode(resource_event, false))
}

func (e *eventConsumingQueue) Start(consumerNum int, stopCh <-chan struct{}) {
	e.queue.Start(consumerNum, stopCh)
}

func (e *eventConsumingQueue) encode(resource_event *event, add_to_old_objs bool) string {
	key := objToKey(resource_event.newObj)
	if resource_event.eventType == watch.Deleted {
		e.deletedObjs[key] = resource_event.newObj
	} else if resource_event.eventType == watch.Modified {
		if _, ok := e.oldObjs[key]; !ok {
			if add_to_old_objs {
				e.oldObjs[key] = resource_event.oldObj
			}
		}
	}

	return fmt.Sprintf("%s|%s", resource_event.eventType, key)
}

func (e *eventConsumingQueue) decode(event_key string) *event {
	parts := strings.Split(event_key, "|")
	if len(parts) != 2 {
		panic(fmt.Sprintf("Invalid event_key for eventConsumingQueue: %s", event_key))
	}

	event_type := watch.EventType(parts[0])
	obj_key := parts[1]

	var new_obj runtime.Object
	var old_obj runtime.Object = nil
	if event_type == watch.Deleted {
		new_obj = e.deletedObjs[obj_key]
		e.queue.Forget(event_key)
		delete(e.deletedObjs, obj_key)
		delete(e.oldObjs, obj_key)
	} else {
		ns, name, err := cache.SplitMetaNamespaceKey(obj_key)

		if err != nil {
			panic(err)
		}

		if ns == "" {
			new_obj, err = e.lister.Get(name)
		} else {
			new_obj, err = e.lister.ByNamespace(ns).Get(name)
		}

		if err != nil {
			// There is a chance that the object is deleted when doing the decode, so the error might be
			// something like "zookeepercluster.zookeeper.database.apache.com \"example-zookeeper-cluster\" not found".
			// We will receive a deletion event later in this situation.
			if apierrors.IsNotFound(err) {
				delete(e.oldObjs, obj_key)
				return nil
			} else {
				panic(err)
			}
		}

		if event_type == watch.Modified {
			// Don't need to forget the event_key for update event.
			old_obj = e.oldObjs[obj_key]
			delete(e.oldObjs, obj_key)
		} else {
			e.queue.Forget(event_type)
		}
	}

	return &event{
		eventType: event_type,
		oldObj:    old_obj,
		newObj:    new_obj,
	}
}

func (e *eventConsumingQueue) consume(obj interface{}, consumer Consumer) {
	// To avoid panic like concurrent map read and map write
	e.lock.Lock()
	// TODO: what if a panic happend in e.decode?
	event := e.decode(obj.(string))
	e.lock.Unlock()

	if event != nil {
		consumer(event)
	}
}

func newEventConsumingQueue(lister cache.GenericLister, consumer Consumer) *eventConsumingQueue {
	eventQueue := eventConsumingQueue{
		lister:      lister,
		deletedObjs: map[string]runtime.Object{},
		oldObjs:     map[string]runtime.Object{},
		lock:        sync.Mutex{},
	}
	consumingQueue := NewConsumingQueue(
		workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		func(obj interface{}) {
			eventQueue.consume(obj, consumer)
		})
	eventQueue.queue = consumingQueue

	return &eventQueue
}

type event struct {
	eventType watch.EventType
	newObj    runtime.Object
	oldObj    runtime.Object
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

	// Forget indicates that an item is finished being retried.  Doesn't matter whether it's for perm failing
	// or for success, we'll stop the rate limiter from tracking it.  This only clears the `rateLimiter`, you
	// still have to call `Done` on the queue.
	Forget(item runtime.Object)

	// We don't want users to invoke the following APIs
	// Get() (item runtime.Object, shutdown bool)
	// Done(item runtime.Object)
	// shutDown()
	// ShuttingDown() bool
}

type resourceRateLimitingAdder struct {
	eventQueue *eventConsumingQueue
}

func (r *resourceRateLimitingAdder) toEvent(item runtime.Object) *event {
	return &event{
		eventType: watch.Modified,
		newObj:    item,
		oldObj:    item,
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

func (r *resourceRateLimitingAdder) NumRequeues(item runtime.Object) int {
	return r.eventQueue.NumRequeues(r.toEvent(item))
}

func (r *resourceRateLimitingAdder) Len() int {
	return r.eventQueue.Len()
}
