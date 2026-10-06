package index

import (
	"container/heap"
	"sort"
	"sync"
	"time"

	"github.com/domains18/kombucha/core"
)

// QueueStats holds observability metrics for a queue.
type QueueStats struct {
	Pending          int           `json:"pending"`
	Leased           int           `json:"leased"`
	Succeeded        int           `json:"succeeded"`
	Dead             int           `json:"dead"`
	Cancelled        int           `json:"cancelled"`
	OldestPendingAge time.Duration `json:"oldest_pending_age,omitempty"`
}

// Index holds the in-memory job state, idempotency index, and priority heaps.
// Read operations take an RLock and return deep copies.
// Mutations are intended to be driven exclusively by the engine's single writer loop.
type Index struct {
	mu         sync.RWMutex
	jobs       map[core.JobID]*core.Job
	idem       map[string]core.JobID
	due        map[string]*dueHeap
	lease      leaseHeap
	dueItems   map[core.JobID]*dueItem
	leaseItems map[core.JobID]*leaseItem
}

// New creates an initialized in-memory Index.
func New() *Index {
	return &Index{
		jobs:       make(map[core.JobID]*core.Job),
		idem:       make(map[string]core.JobID),
		due:        make(map[string]*dueHeap),
		lease:      make(leaseHeap, 0),
		dueItems:   make(map[core.JobID]*dueItem),
		leaseItems: make(map[core.JobID]*leaseItem),
	}
}

// Get returns a copy of the job with the given ID.
func (idx *Index) Get(id core.JobID) (*core.Job, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	j, ok := idx.jobs[id]
	if !ok {
		return nil, false
	}
	return j.Clone(), true
}

// GetByIdem returns a copy of the job registered with the idempotency key.
func (idx *Index) GetByIdem(key string) (*core.Job, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	id, ok := idx.idem[key]
	if !ok {
		return nil, false
	}
	j, ok := idx.jobs[id]
	if !ok {
		return nil, false
	}
	return j.Clone(), true
}

// Put updates or inserts a job into the index and adjusts heaps accordingly.
// Must be called by the writer goroutine (holds write lock).
func (idx *Index) Put(job *core.Job) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.putLocked(job)
}

func (idx *Index) putLocked(job *core.Job) {
	stored := job.Clone()
	idx.jobs[stored.ID] = stored

	if stored.IdemKey != "" {
		idx.idem[stored.IdemKey] = stored.ID
	}

	switch stored.State {
	case core.StatePending:
		// Remove from lease heap if previously leased
		idx.removeLeaseLocked(stored.ID)
		// Add or update in due heap
		idx.putDueLocked(stored)

	case core.StateLeased:
		// Remove from due heap
		idx.removeDueLocked(stored.ID)
		// Add or update in lease heap
		idx.putLeaseLocked(stored)

	case core.StateSucceeded, core.StateDead, core.StateCancelled:
		// Terminal states do not live in due or lease heaps
		idx.removeDueLocked(stored.ID)
		idx.removeLeaseLocked(stored.ID)
	}
}

func (idx *Index) putDueLocked(job *core.Job) {
	h, ok := idx.due[job.Queue]
	if !ok {
		h = &dueHeap{}
		idx.due[job.Queue] = h
	}

	if item, exists := idx.dueItems[job.ID]; exists {
		item.job = job
		heap.Fix(h, item.index)
	} else {
		item := &dueItem{job: job}
		heap.Push(h, item)
		idx.dueItems[job.ID] = item
	}
}

func (idx *Index) removeDueLocked(id core.JobID) {
	item, exists := idx.dueItems[id]
	if !exists {
		return
	}
	delete(idx.dueItems, id)
	h := idx.due[item.job.Queue]
	if h != nil && item.index >= 0 && item.index < len(*h) {
		heap.Remove(h, item.index)
	}
}

func (idx *Index) putLeaseLocked(job *core.Job) {
	if item, exists := idx.leaseItems[job.ID]; exists {
		item.job = job
		heap.Fix(&idx.lease, item.index)
	} else {
		item := &leaseItem{job: job}
		heap.Push(&idx.lease, item)
		idx.leaseItems[job.ID] = item
	}
}

func (idx *Index) removeLeaseLocked(id core.JobID) {
	item, exists := idx.leaseItems[id]
	if !exists {
		return
	}
	delete(idx.leaseItems, id)
	if item.index >= 0 && item.index < len(idx.lease) {
		heap.Remove(&idx.lease, item.index)
	}
}

// PopDue retrieves up to max jobs that are due (RunAt <= now) for the given queue.
// Note: This removes them from the due heap. The caller must transition them.
func (idx *Index) PopDue(queue string, now time.Time, max int) []*core.Job {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	h, ok := idx.due[queue]
	if !ok || len(*h) == 0 {
		return nil
	}

	var results []*core.Job
	for len(*h) > 0 && len(results) < max {
		top := (*h)[0]
		if top.job.RunAt.After(now) {
			break
		}
		item := heap.Pop(h).(*dueItem)
		delete(idx.dueItems, item.job.ID)
		results = append(results, item.job.Clone())
	}
	return results
}

// PopExpiredLeases returns all leased jobs whose LeaseExpiry <= now.
// Removes them from the lease heap.
func (idx *Index) PopExpiredLeases(now time.Time) []*core.Job {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	var expired []*core.Job
	for len(idx.lease) > 0 {
		top := idx.lease[0]
		if top.job.LeaseExpiry.After(now) {
			break
		}
		item := heap.Pop(&idx.lease).(*leaseItem)
		delete(idx.leaseItems, item.job.ID)
		expired = append(expired, item.job.Clone())
	}
	return expired
}

// NextWakeTime returns the earliest time when something becomes due or a lease expires.
func (idx *Index) NextWakeTime() (time.Time, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var earliest time.Time
	hasAny := false

	// Check due heaps
	for _, h := range idx.due {
		if len(*h) > 0 {
			t := (*h)[0].job.RunAt
			if !hasAny || t.Before(earliest) {
				earliest = t
				hasAny = true
			}
		}
	}

	// Check lease heap
	if len(idx.lease) > 0 {
		t := idx.lease[0].job.LeaseExpiry
		if !hasAny || t.Before(earliest) {
			earliest = t
			hasAny = true
		}
	}

	return earliest, hasAny
}

// AllJobs returns a slice of all stored jobs (as clones) sorted by ID. Used for snapshotting.
func (idx *Index) AllJobs() []*core.Job {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	jobs := make([]*core.Job, 0, len(idx.jobs))
	for _, j := range idx.jobs {
		jobs = append(jobs, j.Clone())
	}
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].ID.Compare(jobs[j].ID) < 0
	})
	return jobs
}

// Stats returns per-queue metrics.
func (idx *Index) Stats(now time.Time) map[string]QueueStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	stats := make(map[string]QueueStats)

	for _, j := range idx.jobs {
		s := stats[j.Queue]
		switch j.State {
		case core.StatePending:
			s.Pending++
			age := now.Sub(j.CreatedAt)
			if age > s.OldestPendingAge {
				s.OldestPendingAge = age
			}
		case core.StateLeased:
			s.Leased++
		case core.StateSucceeded:
			s.Succeeded++
		case core.StateDead:
			s.Dead++
		case core.StateCancelled:
			s.Cancelled++
		}
		stats[j.Queue] = s
	}

	return stats
}

// Count returns the total number of jobs tracked in the index.
func (idx *Index) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.jobs)
}
