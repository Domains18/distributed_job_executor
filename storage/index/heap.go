package index

import (
	"github.com/domains18/kombucha/core"
)

// dueItem wraps a Job for the due heap.
type dueItem struct {
	job   *core.Job
	index int // index in the heap slice for container/heap
}

type dueHeap []*dueItem

func (h dueHeap) Len() int { return len(h) }

func (h dueHeap) Less(i, j int) bool {
	// 1. RunAt ascending
	if !h[i].job.RunAt.Equal(h[j].job.RunAt) {
		return h[i].job.RunAt.Before(h[j].job.RunAt)
	}
	// 2. Priority descending (higher priority first)
	if h[i].job.Priority != h[j].job.Priority {
		return h[i].job.Priority > h[j].job.Priority
	}
	// 3. ID ascending
	return h[i].job.ID.Compare(h[j].job.ID) < 0
}

func (h dueHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *dueHeap) Push(x any) {
	n := len(*h)
	item := x.(*dueItem)
	item.index = n
	*h = append(*h, item)
}

func (h *dueHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// leaseItem wraps a Job for the lease heap.
type leaseItem struct {
	job   *core.Job
	index int // index in the heap slice for container/heap
}

type leaseHeap []*leaseItem

func (h leaseHeap) Len() int { return len(h) }

func (h leaseHeap) Less(i, j int) bool {
	// 1. LeaseExpiry ascending
	if !h[i].job.LeaseExpiry.Equal(h[j].job.LeaseExpiry) {
		return h[i].job.LeaseExpiry.Before(h[j].job.LeaseExpiry)
	}
	// 2. ID ascending
	return h[i].job.ID.Compare(h[j].job.ID) < 0
}

func (h leaseHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *leaseHeap) Push(x any) {
	n := len(*h)
	item := x.(*leaseItem)
	item.index = n
	*h = append(*h, item)
}

func (h *leaseHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}
