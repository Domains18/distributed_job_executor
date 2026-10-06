package index

import (
	"testing"
	"time"

	"github.com/domains18/kombucha/core"
)

func TestIndex_DueHeapOrdering(t *testing.T) {
	idx := New()
	baseTime := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

	// Create jobs with varying RunAt and Priority
	j1 := &core.Job{
		ID:       core.MustNewJobID(),
		Queue:    "default",
		State:    core.StatePending,
		RunAt:    baseTime.Add(10 * time.Second),
		Priority: 0,
	}
	j2 := &core.Job{
		ID:       core.MustNewJobID(),
		Queue:    "default",
		State:    core.StatePending,
		RunAt:    baseTime.Add(5 * time.Second),
		Priority: 0,
	}
	j3 := &core.Job{
		ID:       core.MustNewJobID(),
		Queue:    "default",
		State:    core.StatePending,
		RunAt:    baseTime.Add(5 * time.Second),
		Priority: 10, // higher priority with same RunAt
	}

	idx.Put(j1)
	idx.Put(j2)
	idx.Put(j3)

	// At baseTime + 6s, j3 (prio 10) and j2 (prio 0) are due; j3 must come first
	popped := idx.PopDue("default", baseTime.Add(6*time.Second), 10)
	if len(popped) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(popped))
	}
	if popped[0].ID != j3.ID {
		t.Errorf("expected j3 (higher priority) first, got %v", popped[0].ID)
	}
	if popped[1].ID != j2.ID {
		t.Errorf("expected j2 second, got %v", popped[1].ID)
	}

	// At baseTime + 11s, j1 is due
	popped2 := idx.PopDue("default", baseTime.Add(11*time.Second), 10)
	if len(popped2) != 1 || popped2[0].ID != j1.ID {
		t.Fatalf("expected j1, got %v", popped2)
	}
}

func TestIndex_QueueIsolation(t *testing.T) {
	idx := New()
	now := time.Now().UTC()

	// Fill "bulk" queue with 1,000 jobs
	for i := 0; i < 1000; i++ {
		idx.Put(&core.Job{
			ID:       core.MustNewJobID(),
			Queue:    "bulk",
			State:    core.StatePending,
			RunAt:    now,
			Priority: 0,
		})
	}

	// Add 1 job to "critical" queue
	critJob := &core.Job{
		ID:       core.MustNewJobID(),
		Queue:    "critical",
		State:    core.StatePending,
		RunAt:    now,
		Priority: 100,
	}
	idx.Put(critJob)

	// Popping from "critical" should return critJob immediately without scanning bulk
	popped := idx.PopDue("critical", now, 10)
	if len(popped) != 1 || popped[0].ID != critJob.ID {
		t.Fatalf("failed to retrieve critical job isolated from bulk queue")
	}
}

func TestIndex_LeaseHeapAndExpiry(t *testing.T) {
	idx := New()
	baseTime := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

	j1 := &core.Job{
		ID:          core.MustNewJobID(),
		Queue:       "default",
		State:       core.StateLeased,
		LeaseExpiry: baseTime.Add(20 * time.Second),
		LeaseToken:  1,
	}
	j2 := &core.Job{
		ID:          core.MustNewJobID(),
		Queue:       "default",
		State:       core.StateLeased,
		LeaseExpiry: baseTime.Add(10 * time.Second),
		LeaseToken:  1,
	}

	idx.Put(j1)
	idx.Put(j2)

	// At baseTime + 15s, j2 should be expired
	expired := idx.PopExpiredLeases(baseTime.Add(15 * time.Second))
	if len(expired) != 1 || expired[0].ID != j2.ID {
		t.Fatalf("expected j2 expired, got %v", expired)
	}

	// At baseTime + 25s, j1 should be expired
	expired2 := idx.PopExpiredLeases(baseTime.Add(25 * time.Second))
	if len(expired2) != 1 || expired2[0].ID != j1.ID {
		t.Fatalf("expected j1 expired, got %v", expired2)
	}
}

func TestIndex_Stats(t *testing.T) {
	idx := New()
	now := time.Now().UTC()

	idx.Put(&core.Job{
		ID:        core.MustNewJobID(),
		Queue:     "email",
		State:     core.StatePending,
		CreatedAt: now.Add(-5 * time.Minute),
	})
	idx.Put(&core.Job{
		ID:        core.MustNewJobID(),
		Queue:     "email",
		State:     core.StateLeased,
		CreatedAt: now,
	})
	idx.Put(&core.Job{
		ID:        core.MustNewJobID(),
		Queue:     "email",
		State:     core.StateSucceeded,
		CreatedAt: now,
	})

	stats := idx.Stats(now)
	emailStats, ok := stats["email"]
	if !ok {
		t.Fatal("expected stats for queue 'email'")
	}

	if emailStats.Pending != 1 || emailStats.Leased != 1 || emailStats.Succeeded != 1 {
		t.Fatalf("unexpected stats: %+v", emailStats)
	}
	if emailStats.OldestPendingAge < 4*time.Minute {
		t.Fatalf("unexpected oldest pending age: %v", emailStats.OldestPendingAge)
	}
}
