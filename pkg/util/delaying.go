package util

import (
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/utils/clock"
)

// adapted from k8s.io/client-go@v0.22.2/util/workqueue/delaying_queue.go

// waitFor holds the executee to add and the time it should be executed
type waitFor struct {
	executee Executee
	readyAt  time.Time
}

func waitForComporator(first, second interface{}) bool {
	firstWaitFor := first.(*waitFor)
	secondWaitFor := second.(*waitFor)
	return firstWaitFor.readyAt.Before(secondWaitFor.readyAt)
}

type Executee interface {
	Execute()
}

type executableFunc func()

func (e executableFunc) Execute() {
	e()
}

type DelayingExecutor struct {
	// waitingForAddCh is a buffered channel that feeds waitingForAdd
	waitingForAddCh       chan *waitFor
	clock                 clock.Clock
	stopCh                chan struct{}
	shutDownWhenFinishing bool
	priorityQueue         PriorityQueue
	shuttingDown          bool
}

func NewDelayingExecutor(size int) *DelayingExecutor {
	return newDelayingExecutor(size, NewPriorityQueue(waitForComporator))
}

func newDelayingExecutor(size int, priorityQueue PriorityQueue) *DelayingExecutor {
	executor := &DelayingExecutor{
		// Don't need to close the channel, or we may get "panic: send on closed channel"
		waitingForAddCh: make(chan *waitFor, size),
		clock:           clock.RealClock{},
		stopCh:          make(chan struct{}),
		priorityQueue:   priorityQueue,
		shuttingDown:    false,
	}

	go executor.waitingLoop()
	return executor
}

func (d *DelayingExecutor) ExcuteFuncAfter(f func(), duration time.Duration) {
	d.ExcuteExcutableAfter((executableFunc)(f), duration)
}

func (d *DelayingExecutor) ExcuteExcutableAfter(e Executee, duration time.Duration) {
	if d.shuttingDown {
		return
	}

	// immediately add things with no delay
	if duration <= 0 {
		go e.Execute()
		return
	}

	select {
	// unblock if d.stopCh is close
	case <-d.stopCh:
	case d.waitingForAddCh <- &waitFor{executee: e, readyAt: d.clock.Now().Add(duration)}:
	}
}

func (d *DelayingExecutor) waitingLoop() {
	defer utilruntime.HandleCrash()

	// Make a placeholder channel to use when there are no items in our list
	never := make(<-chan time.Time)

	// Make a timer that expires when the item at the head of the waiting list is ready
	var nextReadyAtTimer clock.Timer

	for {
		if d.shuttingDown && !d.shutDownWhenFinishing {
			return
		}

		now := d.clock.Now()

		// Add ready entries
		for d.priorityQueue.Len() > 0 {
			entry := d.priorityQueue.Peek().(*waitFor)
			if entry.readyAt.After(now) {
				break
			}

			entry = d.priorityQueue.Get().(*waitFor)
			go entry.executee.Execute()
		}

		// Set up a wait for the first item's readyAt (if one exists)
		nextReadyAt := never
		if d.priorityQueue.Len() > 0 {
			if nextReadyAtTimer != nil {
				nextReadyAtTimer.Stop()
			}
			entry := d.priorityQueue.Peek().(*waitFor)
			nextReadyAtTimer = d.clock.NewTimer(entry.readyAt.Sub(now))
			nextReadyAt = nextReadyAtTimer.C()
		}

		select {
		case <-d.stopCh:

		case <-nextReadyAt:
			// continue the loop, which will add ready items
		case waitEntry := <-d.waitingForAddCh:
			if waitEntry.readyAt.After(d.clock.Now()) {
				d.priorityQueue.Add(waitEntry)
			} else {
				go waitEntry.executee.Execute()
			}
		}

		if d.shutDownWhenFinishing && len(d.waitingForAddCh) == 0 && d.priorityQueue.Len() == 0 {
			return
		}
	}
}

func (d *DelayingExecutor) ShutDownFast() {
	d.shuttingDown = true
	close(d.stopCh)
}

func (d *DelayingExecutor) ShutDownWhenFinishing() {
	d.shutDownWhenFinishing = true
	d.shuttingDown = true
}

type delayingObj struct {
	obj interface{}
	ch  chan<- interface{}
}

func (d *delayingObj) Execute() {
	d.ch <- d.obj
}

type DelayingChannel struct {
	executor *DelayingExecutor
	ch       chan interface{} // TODO: use generic types instead interface{}
}

func NewDelayingChannel(repeated bool, size int) *DelayingChannel {
	var priorityQueue PriorityQueue
	if repeated {
		priorityQueue = NewPriorityQueue(waitForComporator)
	} else {
		hasher := func(obj interface{}) interface{} {
			return obj.(*waitFor).executee.(*delayingObj).obj
		}
		repeator := func(original, new interface{}) bool {
			return original.(*waitFor).readyAt.After(new.(*waitFor).readyAt)
		}
		priorityQueue = NewPrioritySet(waitForComporator, hasher, repeator)
	}

	return &DelayingChannel{
		executor: newDelayingExecutor(size, priorityQueue),
		ch:       make(chan interface{}, size),
	}
}

func (d *DelayingChannel) Get() interface{} {
	return <-d.ch
}

func (d *DelayingChannel) AddAfter(entry interface{}, duration time.Duration) {
	d.executor.ExcuteExcutableAfter(&delayingObj{
		obj: entry,
		ch:  d.ch,
	}, duration)
}

func (d *DelayingChannel) Close() {
	// close(d.ch) don't close channel here. Some entries in "AddAfter" may be send to the channel later
	d.executor.ShutDownWhenFinishing()
}
