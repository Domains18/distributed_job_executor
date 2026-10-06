package snapshot

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/domains18/kombucha/core"
)

func TestSnapshot_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()

	jobs := []*core.Job{
		{
			ID:          core.MustNewJobID(),
			Type:        "email",
			Queue:       "default",
			Payload:     []byte(`{"a":1}`),
			State:       core.StatePending,
			Attempt:     1,
			MaxAttempts: 3,
			RunAt:       now,
			CreatedAt:   now,
			UpdatedAt:   now,
		},
		{
			ID:          core.MustNewJobID(),
			Type:        "billing",
			Queue:       "critical",
			Payload:     []byte(`{"b":2}`),
			State:       core.StateLeased,
			Attempt:     2,
			MaxAttempts: 5,
			RunAt:       now,
			LeaseExpiry: now.Add(30 * time.Second),
			LeaseToken:  10,
			WorkerID:    "worker-1",
			CreatedAt:   now,
			UpdatedAt:   now,
		},
	}

	snapLSN := core.LSN(100)
	if err := Save(dir, snapLSN, jobs); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loadedLSN, loadedJobs, err := LoadNewest(dir)
	if err != nil {
		t.Fatalf("LoadNewest failed: %v", err)
	}

	if loadedLSN != snapLSN {
		t.Fatalf("expected LSN %d, got %d", snapLSN, loadedLSN)
	}

	if len(loadedJobs) != len(jobs) {
		t.Fatalf("expected %d jobs, got %d", len(jobs), len(loadedJobs))
	}

	for i := range jobs {
		if loadedJobs[i].ID != jobs[i].ID ||
			loadedJobs[i].Type != jobs[i].Type ||
			loadedJobs[i].Queue != jobs[i].Queue ||
			!bytes.Equal(loadedJobs[i].Payload, jobs[i].Payload) ||
			loadedJobs[i].State != jobs[i].State ||
			loadedJobs[i].LeaseToken != jobs[i].LeaseToken {
			t.Fatalf("job %d mismatch:\ngot: %+v\nwant: %+v", i, loadedJobs[i], jobs[i])
		}
	}
}

func TestSnapshot_CorruptedFallback(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()

	jobsOld := []*core.Job{
		{
			ID:        core.MustNewJobID(),
			Type:      "old_job",
			State:     core.StatePending,
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	jobsNew := []*core.Job{
		{
			ID:        core.MustNewJobID(),
			Type:      "new_job",
			State:     core.StatePending,
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	// Save older snapshot at LSN 50
	if err := Save(dir, 50, jobsOld); err != nil {
		t.Fatal(err)
	}
	// Save newer snapshot at LSN 100
	if err := Save(dir, 100, jobsNew); err != nil {
		t.Fatal(err)
	}

	// Corrupt newer snapshot
	newPath := filepath.Join(dir, "snapshot.00000000000000000100.snap")
	data, err := os.ReadFile(newPath)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(newPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	// LoadNewest should detect corruption in snapshot 100 and cleanly fallback to snapshot 50
	lsn, loadedJobs, err := LoadNewest(dir)
	if err != nil {
		t.Fatalf("expected clean fallback, got error: %v", err)
	}

	if lsn != 50 {
		t.Fatalf("expected fallback LSN 50, got %d", lsn)
	}
	if len(loadedJobs) != 1 || loadedJobs[0].Type != "old_job" {
		t.Fatalf("expected old_job from fallback snapshot, got %v", loadedJobs)
	}
}
