package informer

import (
	"fmt"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"zookeeper-operator/pkg/util"
)

type event struct {
	key    eventKey
	newObj runtime.Object
	oldObj runtime.Object
}

func (e *event) getType() watch.EventType {
	return e.key.getEventType()
}

func newAdditionEvent(obj runtime.Object) *event {
	return &event{
		key:    newKey(watch.Added, obj),
		newObj: obj,
		oldObj: nil,
	}
}

func newDeletionEvent(obj runtime.Object) *event {
	return &event{
		key:    newKey(watch.Deleted, obj),
		newObj: nil,
		oldObj: obj,
	}
}

func newUpdateEvent(oldObj, newObj runtime.Object) *event {
	return &event{
		key:    newKey(watch.Modified, newObj),
		newObj: newObj,
		oldObj: oldObj,
	}
}

type eventKey string

func objToKey(obj interface{}) string {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		panic(fmt.Sprintf("Couldn't get eventKey for object %+v: %v", obj, err))
	}

	return key
}

func newKey(eventType watch.EventType, obj runtime.Object) eventKey {
	return eventKey(fmt.Sprintf("%s|%s", eventType, objToKey(obj)))
}

// TODO: make cache a global variable, so that we don't need to pass lister to this function
func (s eventKey) getNewObj(lister cache.GenericLister) runtime.Object {
	ns := s.GetNamespace()
	name := s.GetName()
	var err error
	var newObj runtime.Object
	if ns == "" {
		newObj, err = lister.Get(name)
	} else {
		newObj, err = lister.ByNamespace(ns).Get(name)
	}

	if err != nil {
		// There is a chance that the object is deleted when doing the decode, so the error might be
		// something like "zookeepercluster.zookeeper.database.apache.com \"example-zookeeper-cluster\" not found".
		// We will receive a deletion event later in this situation.
		if apierrors.IsNotFound(err) {
			return nil
		} else {
			panic(err)
		}
	}
	return newObj
}

func (s eventKey) split() []string {
	parts := strings.Split(string(s), "|")
	if len(parts) != 2 {
		panic(fmt.Sprintf("Invalid eventKey: %s", string(s)))
	}
	return parts
}

func (s eventKey) getEventType() watch.EventType {
	return watch.EventType(s.split()[0])
}

func (s eventKey) GetNamespace() string {
	ns, _, err := cache.SplitMetaNamespaceKey(s.split()[1])

	if err != nil {
		panic(err)
	}
	return ns
}

func (s eventKey) GetName() string {
	_, name, err := cache.SplitMetaNamespaceKey(s.split()[1])

	if err != nil {
		panic(err)
	}
	return name
}

type keyObjDict map[eventKey]runtime.Object

func (s keyObjDict) has(key eventKey) bool {
	_, exists := s[key]
	return exists
}

func (s keyObjDict) insert(key eventKey, oldObj runtime.Object) {
	s[key] = oldObj
}

func (s keyObjDict) delete(key eventKey) {
	delete(s, key)
}

type keySet map[eventKey]struct{}

func (k keySet) has(key eventKey) bool {
	_, exists := k[key]
	return exists
}

func (k keySet) insert(key eventKey) {
	k[key] = struct{}{}
}

func (k keySet) delete(key eventKey) {
	delete(k, key)
}

// adapted from k8s.io/client-go@v0.22.2/util/workqueue/queue.go
type typeForEventQueue struct {
	// queue defines the order in which we will work on items. Every
	// element of queue should be in the dirty keyObjDict and not in the
	// processing keyObjDict.
	queue []eventKey

	// dirty defines all of the items that need to be processed.
	dirty keyObjDict

	// Things that are currently being processed are in the processing keyObjDict.
	// These things may be simultaneously in the dirty keyObjDict. When we finish
	// processing something and remove it from this keyObjDict, we'll check if
	// it's in the dirty keyObjDict, and if so, add it to the queue.
	processing keySet

	cond *sync.Cond

	shuttingDown bool

	// Used to get the current object
	lister cache.GenericLister
}

// Add marks item as needing processing.
func (t *typeForEventQueue) Add(event *event) {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	if t.shuttingDown {
		return
	}

	// Don't want to overwrite the oldObj
	if t.dirty.has(event.key) {
		t.cond.Signal()
		return
	}

	t.dirty.insert(event.key, event.oldObj)
	if t.processing.has(event.key) {
		return
	}

	t.queue = append(t.queue, event.key)
	t.cond.Signal()
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
func (t *typeForEventQueue) Get() (e *event, shutdown bool) {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	for len(t.queue) == 0 && !t.shuttingDown {
		t.cond.Wait()
	}
	if len(t.queue) == 0 {
		// We must be shutting down.
		return nil, true
	}

	eventkey := t.queue[0]
	t.queue = t.queue[1:]
	oldObj := t.dirty[eventkey]

	t.processing.insert(eventkey)
	t.dirty.delete(eventkey)

	event := event{
		key:    eventkey,
		oldObj: oldObj,
		newObj: eventkey.getNewObj(t.lister),
	}
	return &event, false
}

// Done marks event as done processing, and if it has been marked as dirty again
// while it was being processed, it will be re-added to the queue for
// re-processing.
func (t *typeForEventQueue) Done(event *event) {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()

	t.processing.delete(event.key)
	if t.dirty.has(event.key) {
		t.queue = append(t.queue, event.key)
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
func (q *delayingTypeForEventQueue) AddAfter(resourceEvent *event, duration time.Duration) {
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
func (r *rateLimitingTypeForEventQueue) AddRateLimited(resourceEvent *event) {
	r.AddAfter(resourceEvent, r.rateLimiter.When(resourceEvent))
}

func (r *rateLimitingTypeForEventQueue) NumRequeues(resourceEvent *event) int {
	return r.rateLimiter.NumRequeues(resourceEvent)
}

func (r *rateLimitingTypeForEventQueue) Forget(resourceEvent *event) {
	r.rateLimiter.Forget(resourceEvent)
}

func newRateLimitingQueue(lister cache.GenericLister, rateLimiter workqueue.RateLimiter) *rateLimitingTypeForEventQueue {
	return &rateLimitingTypeForEventQueue{
		delayingTypeForEventQueue: delayingTypeForEventQueue{
			typeForEventQueue: typeForEventQueue{
				dirty:        keyObjDict{},
				processing:   keySet{},
				cond:         sync.NewCond(&sync.Mutex{}),
				shuttingDown: false,
				lister:       lister,
			},
			delayingExecutor: util.NewDelayingExecutor(1000),
		},
		rateLimiter: rateLimiter,
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
	eventQueue *rateLimitingTypeForEventQueue
}

func (r *resourceRateLimitingAdder) toEvent(item runtime.Object) *event {
	return newUpdateEvent(item, item)
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
