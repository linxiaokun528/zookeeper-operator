package informertype

import (
	"fmt"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/workqueue"
	"sync"
	"time"
)

type Consumer func(obj interface{}) (bool, error)

type ConsumingQueue struct {
	workqueue.RateLimitingInterface
	consumer Consumer

	stopCh chan struct{}
	wait   sync.WaitGroup
}

func NewConsumingQueue(queue workqueue.RateLimitingInterface, consumer Consumer) *ConsumingQueue {
	return &ConsumingQueue{
		RateLimitingInterface: queue,
		consumer:              consumer,
		wait:                  sync.WaitGroup{},
	}
}

func (c *ConsumingQueue) Consume(consumerNum int) {
	c.wait.Add(consumerNum)
	for i := 0; i < consumerNum; i++ {
		go wait.Until(c.worker, time.Second, c.stopCh)
	}
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
	close(c.stopCh)
	c.RateLimitingInterface.ShutDown()
	c.wait.Wait()
}

type eventConsumingQueue struct {
	queue *ConsumingQueue
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
	e.queue.Add(resource_event)
}

func (e *eventConsumingQueue) AddAfter(resource_event *event, duration time.Duration) {
	e.queue.AddAfter(resource_event, duration)
}

func (e *eventConsumingQueue) AddRateLimited(resource_event *event) {
	e.queue.AddRateLimited(resource_event)
}

func (e *eventConsumingQueue) Forget(resource_event *event) {
	e.queue.Forget(resource_event)
}

func (e *eventConsumingQueue) NumRequeues(resource_event *event) int {
	return e.queue.NumRequeues(resource_event)
}

func (e *eventConsumingQueue) Get() (resource_event *event, shutdown bool) {
	item, shutdown := e.queue.Get()
	return item.(*event), shutdown
}

func (e *eventConsumingQueue) Done(resource_event *event) {
	e.queue.Done(resource_event)
}

func (e *eventConsumingQueue) Consume(consumerNum int) {
	e.queue.Consume(consumerNum)
}

func newEventConsumingQueue(name string, consumer Consumer) *eventConsumingQueue {
	return &eventConsumingQueue{
		NewConsumingQueue(
			workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), name), consumer),
	}
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
	key  string
}

type getKeyFunc func(interface{}) string

type resourceRateLimitingAdder struct {
	eventQueue *eventConsumingQueue
	keyGetter  getKeyFunc
}

func (r *resourceRateLimitingAdder) toEvent(item runtime.Object) *event {
	return &event{
		Type: watch.Modified,
		key:  r.keyGetter(item),
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
