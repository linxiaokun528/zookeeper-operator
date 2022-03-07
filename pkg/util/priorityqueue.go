package util

import (
	"container/heap"
)

type Comparator func(first, second interface{}) bool
type Repeator func(original, new interface{}) bool
type Hasher func(obj interface{}) interface{}

type PriorityQueue interface {
	Peek() interface{} // TODO: use Generics instead
	Len() int
	Add(x interface{})
	Get() interface{}
	Remove(e interface{}) bool
}

func NewPriorityQueue(comparator Comparator) PriorityQueue {
	list := &priorityQueueList{
		entries:    []interface{}{},
		comporator: comparator,
	}
	heap.Init(list)
	return &priorityQueue{
		list: list,
	}
}

func NewPrioritySet(comparator Comparator, hasher Hasher, repeator Repeator) PriorityQueue {
	list := &prioritySetList{
		entries:    []*prioritySetEntry{},
		comparator: comparator,
	}
	heap.Init(list)
	return &prioritySet{
		list:         list,
		knownEntries: map[interface{}]*prioritySetEntry{},
		hasher:       hasher,
		repeator:     repeator,
	}
}

type priorityQueueList struct {
	entries    []interface{}
	comporator Comparator
}

func (pq *priorityQueueList) Len() int {
	return len(pq.entries)
}

func (pq *priorityQueueList) Less(i, j int) bool {
	return pq.comporator(pq.entries[i], pq.entries[j])
}

func (pq *priorityQueueList) Swap(i, j int) {
	pq.entries[i], pq.entries[j] = pq.entries[j], pq.entries[i]
}

// Push adds an item to the list. Push should not be called directly; instead,
// use `heap.Push`.
func (pq *priorityQueueList) Push(x interface{}) {
	pq.entries = append(pq.entries, x)
}

// Pop removes an item from the list. Pop should not be called directly;
// instead, use `heap.Pop`.
func (pq *priorityQueueList) Pop() interface{} {
	n := len(pq.entries)
	item := pq.entries[n-1]
	pq.entries = pq.entries[0:(n - 1)]
	return item
}

type priorityQueue struct {
	list *priorityQueueList
}

func (pq *priorityQueue) Peek() interface{} {
	if len(pq.list.entries) == 0 {
		panic("Peek from an empty priorityQueue.")
	}
	return pq.list.entries[0]
}

func (pq *priorityQueue) Len() int {
	return pq.list.Len()
}

func (pq *priorityQueue) Add(x interface{}) {
	heap.Push(pq.list, x)
}

func (pq *priorityQueue) Get() interface{} {
	return heap.Pop(pq.list)
}

func (pq *priorityQueue) Remove(e interface{}) bool {
	result := false
	for pq.removeOne(e) {
		result = true
	}
	return result
}

func (pq *priorityQueue) removeOne(e interface{}) bool {
	for i, entry := range pq.list.entries {
		if e == entry {
			heap.Remove(pq.list, i)
			return true
		}
	}
	return false
}

type prioritySetEntry struct {
	entry interface{}
	index int
	hash  interface{}
}

type prioritySetList struct {
	entries    []*prioritySetEntry
	comparator Comparator
}

func (p *prioritySetList) Len() int {
	return len(p.entries)
}

func (p *prioritySetList) Less(i, j int) bool {
	return p.comparator(p.entries[i].entry, p.entries[j].entry)
}

func (p *prioritySetList) Swap(i, j int) {
	p.entries[i], p.entries[j] = p.entries[j], p.entries[i]
	p.entries[i].index = i
	p.entries[j].index = j
}

// Push adds an item to the list. Push should not be called directly; instead,
// use `heap.Push`.
func (p *prioritySetList) Push(x interface{}) {
	p.entries = append(p.entries,
		&prioritySetEntry{
			entry: x,
			index: p.Len(),
		})
}

// Pop removes an item from the list. Pop should not be called directly;
// instead, use `heap.Pop`.
func (p *prioritySetList) Pop() interface{} {
	n := len(p.entries)
	item := p.entries[n-1]
	item.index = -1
	p.entries = p.entries[0:(n - 1)]
	return item
}

type prioritySet struct {
	list         *prioritySetList
	knownEntries map[interface{}]*prioritySetEntry
	hasher       Hasher
	repeator     Repeator
}

func (p *prioritySet) Add(x interface{}) {
	hash := p.hasher(x)
	existing, exists := p.knownEntries[hash]
	if exists {
		if p.repeator(existing.entry, x) {
			existing.entry = x
			heap.Fix(p.list, existing.index)
		}
	} else {
		heap.Push(p.list, x)
	}
}

func (p *prioritySet) Get() interface{} {
	item := heap.Pop(p.list).(prioritySetEntry)
	delete(p.knownEntries, item.hash)
	return item.entry
}

func (p *prioritySet) Peek() interface{} {
	if len(p.list.entries) == 0 {
		panic("Peek from an empty priorityQueue.")
	}
	return p.list.entries[0].entry
}

func (p *prioritySet) Len() int {
	return p.list.Len()
}

func (p *prioritySet) Remove(e interface{}) bool {
	hash := p.hasher(e)
	existing, exists := p.knownEntries[hash]
	if exists {
		delete(p.knownEntries, hash)
		heap.Remove(p.list, existing.index)
	}

	return exists
}
