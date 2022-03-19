package informer

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"zookeeper-operator/pkg/util"
)

type Event struct {
	eventType   watch.EventType
	resourceKey resourceKey
	newObj      client.Object
	oldObj      client.Object
}

func (e *Event) GetType() watch.EventType {
	return e.eventType
}

func (e *Event) GetResourceKey() resourceKey {
	return e.resourceKey
}

func (e *Event) GetGeneration() int64 {
	if e.newObj != nil {
		return e.newObj.GetGeneration()
	} else {
		return e.oldObj.GetGeneration()
	}
}

func NewAdditionEvent(obj client.Object) *Event {
	util.CheckNotNil(obj)
	return &Event{
		eventType:   watch.Added,
		resourceKey: newResourceKey(obj),
		newObj:      obj,
		oldObj:      nil,
	}
}

func NewDeletionEvent(obj client.Object) *Event {
	util.CheckNotNil(obj)
	return &Event{
		eventType:   watch.Deleted,
		resourceKey: newResourceKey(obj),
		newObj:      nil,
		oldObj:      obj,
	}
}

func NewUpdateEvent(oldObj, newObj client.Object) *Event {
	util.CheckNotNil(oldObj)
	util.CheckNotNil(newObj)
	return &Event{
		eventType:   watch.Modified,
		resourceKey: newResourceKey(newObj),
		newObj:      newObj,
		oldObj:      oldObj,
	}
}

// the format of resourceKey is "<namespace>/<name>", like "default/example-zookeeper-cluster"
type resourceKey string

func (r *resourceKey) GetNamespace() string {
	ns, _, err := cache.SplitMetaNamespaceKey(string(*r))

	if err != nil {
		panic(err)
	}
	return ns
}

func (r *resourceKey) GetName() string {
	_, name, err := cache.SplitMetaNamespaceKey(string(*r))

	if err != nil {
		panic(err)
	}
	return name
}

// the format of eventKey is "<namespace>/<name>", like "default/example-zookeeper-cluster"
func newResourceKey(obj client.Object) resourceKey {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		panic(fmt.Sprintf("Couldn't get resourceKey for object %#v: %v", obj, err))
	}

	return resourceKey(key)
}

type eventSet map[resourceKey]*Event

func (k *eventSet) hasEventForTheSameResource(e *Event) bool {
	_, exists := (*k)[e.resourceKey]
	return exists
}

func (k *eventSet) add(e *Event) {
	(*k)[e.resourceKey] = e
}

func (k *eventSet) delete(e *Event) {
	delete(*k, e.resourceKey)
}

// adapted from k8s.io/client-go@v0.22.2/util/workqueue/queue.go
type typeForEventQueue struct {
	// queue defines the order in which we will work on items. Every
	// element of queue should be in the dirty eventSet and not in the
	// processing eventSet.
	queue []resourceKey

	// dirty defines all of the items that need to be processed.
	dirty eventSet

	// Things that are currently being processed are in the processing eventSet.
	// These things may be simultaneously in the dirty eventSet. When we finish
	// processing something and remove it from this eventSet, we'll check if
	// it's in the dirty eventSet, and if so, add it to the queue.
	processing eventSet

	cond *sync.Cond

	shuttingDown bool

	// Used to get the current object
	lister cache.GenericLister
}

// Add marks item as needing processing.
func (t *typeForEventQueue) Add(event *Event) {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	if t.shuttingDown {
		return
	}

	if !t.dirty.hasEventForTheSameResource(event) {
		t.dirty.add(event)
		if !t.processing.hasEventForTheSameResource(event) {
			t.queue = append(t.queue, event.resourceKey)
			t.cond.Signal()
		}
	} else {
		// in this subclause, it means the event has another event with the same resource
		// either in t.processing or t.queue
		newEvent := event
		oldEvent := t.dirty[newEvent.resourceKey]
		if oldEvent.GetGeneration() > newEvent.GetGeneration() {
			// This may happen when the newEvent is added by the adder
			return
		}

		switch oldEvent.eventType {
		case watch.Added:
			switch newEvent.eventType {
			case watch.Added: // This may happen when the newEvent is added by the adder
				t.dirty[newEvent.resourceKey] = newEvent
				return
			case watch.Modified:
				oldEvent.newObj = newEvent.newObj
				return
			case watch.Deleted:
				t.dirty[newEvent.resourceKey] = newEvent
				return
			}
		case watch.Modified:
			switch newEvent.eventType {
			case watch.Added:
				panic(fmt.Errorf("resource %s has an %s Event after a %s Event",
					newEvent.resourceKey, newEvent.GetType(), oldEvent.GetType()))
			case watch.Modified:
				oldEvent.newObj = newEvent.newObj
				return
			case watch.Deleted:
				t.dirty[newEvent.resourceKey] = newEvent
				return
			}
		case watch.Deleted:
			switch newEvent.eventType {
			case watch.Added:
				t.dirty[newEvent.resourceKey] = newEvent
				return
			case watch.Modified:
				panic(fmt.Errorf("resource %s has an %s Event after a %s Event",
					newEvent.resourceKey, newEvent.GetType(), oldEvent.GetType()))
			case watch.Deleted: // This may happen when the newEvent is added by the adder
				t.dirty[newEvent.resourceKey] = newEvent
				return
			}
		}
	}
}

// Len returns the current queue length, for informational purposes only. You
// shouldn't e.g. gate a call to Add() or Get() on Len() being a particular
// value, that can't be synchronized properly.
func (t *typeForEventQueue) Len() int {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	return len(t.queue)
}

// Get blocks until it can return an item to be processed. If shutdown = true,
// the caller should end their goroutine. You must call Done with item when you
// have finished processing it.
func (t *typeForEventQueue) Get() (e *Event, shutdown bool) {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	for len(t.queue) == 0 && !t.shuttingDown {
		t.cond.Wait()
	}
	if len(t.queue) == 0 {
		// We must be shutting down.
		return nil, true
	}

	resource := t.queue[0]
	t.queue = t.queue[1:]
	e = t.dirty[resource]

	t.processing.add(e)
	t.dirty.delete(e)

	return e, false
}

// Done marks Event as done processing, and if it has been marked as dirty again
// while it was being processed, it will be re-added to the queue for
// re-processing.
func (t *typeForEventQueue) Done(event *Event) {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()

	t.processing.delete(event)
	if t.dirty.hasEventForTheSameResource(event) {
		t.queue = append(t.queue, event.resourceKey)
		t.cond.Signal()
	}
}

// ShutDown will cause q to ignore all new items added to it. As soon as the
// worker goroutines have drained the existing items in the queue, they will be
// instructed to exit.
func (t *typeForEventQueue) ShutDown() {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	t.shuttingDown = true
	t.cond.Broadcast()
}

func (t *typeForEventQueue) ShuttingDown() bool {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()

	return t.shuttingDown
}

// delayingTypeForEventQueue wraps an typeForEventQueue and provides delayed re-enquing
// adapted from k8s.io/client-go@v0.22.2/util/workqueue/delaying_queue.go
type delayingTypeForEventQueue struct {
	typeForEventQueue

	delayingExecutor *util.DelayingExecutor
}

// AddAfter adds the given item to the work queue after the given delay
func (q *delayingTypeForEventQueue) AddAfter(resourceEvent *Event, duration time.Duration) {
	// don't add if we're already shutting down
	if q.ShuttingDown() {
		return
	}

	// immediately add things with no delay
	if duration <= 0 {
		q.Add(resourceEvent)
		return
	}

	q.delayingExecutor.ExcuteFuncAfter(func() {
		q.Add(resourceEvent)
	}, duration)
}

func (q *delayingTypeForEventQueue) ShutDown() {
	q.delayingExecutor.ShutDownFast()
	q.typeForEventQueue.ShutDown()
}

// rateLimitingType wraps an Interface and provides rateLimited re-enquing
// adapted from k8s.io/client-go@v0.22.2/util/workqueue/rate_limiting_queue.go
type rateLimitingTypeForEventQueue struct {
	delayingTypeForEventQueue

	rateLimiter workqueue.RateLimiter
}

// AddRateLimited AddAfter's the item based on the time when the rate limiter says it's ok
func (r *rateLimitingTypeForEventQueue) AddRateLimited(resourceEvent *Event) {
	r.AddAfter(resourceEvent, r.rateLimiter.When(resourceEvent.resourceKey))
}

func (r *rateLimitingTypeForEventQueue) NumRequeues(resourceEvent *Event) int {
	return r.rateLimiter.NumRequeues(resourceEvent.resourceKey)
}

func (r *rateLimitingTypeForEventQueue) Forget(resourceEvent *Event) {
	r.rateLimiter.Forget(resourceEvent.resourceKey)
}

func newRateLimitingQueue(lister cache.GenericLister, rateLimiter workqueue.RateLimiter) *rateLimitingTypeForEventQueue {
	return &rateLimitingTypeForEventQueue{
		delayingTypeForEventQueue: delayingTypeForEventQueue{
			typeForEventQueue: typeForEventQueue{
				dirty:        eventSet{},
				processing:   eventSet{},
				cond:         sync.NewCond(&sync.Mutex{}),
				shuttingDown: false,
				lister:       lister,
			},
			delayingExecutor: util.NewDelayingExecutor(1000),
		},
		rateLimiter: rateLimiter,
	}
}

// adapted from k8s.io/client-go@v0.22.2/util/workqueue/rate_limiting_queue.go
type ResourceRateLimitingAdder interface {
	Add(resourceEvent *Event)
	Len() int
	// AddAfter adds an Event to the workqueue after the indicated duration has passed
	AddAfter(resourceEvent *Event, duration time.Duration)

	// AddRateLimited adds an Event to the workqueue after the rate limiter says it's ok
	AddRateLimited(resourceEvent *Event)

	// NumRequeues returns back how many times the item was requeued
	NumRequeues(resourceEvent *Event) int

	// Forget indicates that an Event is finished being retried.  Doesn't matter whether it's for perm failing
	// or for success, we'll stop the rate limiter from tracking it.  This only clears the `rateLimiter`, you
	// still have to call `Done` on the queue.
	Forget(resourceEvent *Event)

	// We don't want users to invoke the following APIs
	// Get() (Event *Event, shutdown bool)
	// Done(Event *Event)
	// shutDown()
	// ShuttingDown() bool
}
