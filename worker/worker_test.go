package worker

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/domains18/kombucha/api"
	"github.com/domains18/kombucha/core"
	"github.com/domains18/kombucha/storage/engine"
	"github.com/domains18/kombucha/storage/wal"
)

func setupCoordinator(t *testing.T) (*httptest.Server, *engine.Engine) {
	dir := t.TempDir()
	parking := api.NewParkingLot()

	eng, err := engine.Open(engine.Config{
		DataDir: dir,
		WALOptions: wal.Options{
			Sync: true,
		},
		OnJobEligible: func(queue string) {
			parking.Signal(queue)
		},
	})
	if err != nil {
		t.Fatalf("Open engine failed: %v", err)
	}

	srv := api.NewServer(api.ServerConfig{
		Engine:     eng,
		ParkingLot: parking,
	})

	ts := httptest.NewServer(srv.Handler())
	return ts, eng
}

func TestWorker_Execution_Success(t *testing.T) {
	ts, eng := setupCoordinator(t)
	defer ts.Close()
	defer eng.Close()

	w := NewWorker(Config{
		CoordinatorURL:   ts.URL,
		WorkerID:         "test-worker-1",
		Capacity:         2,
		PollWaitDuration: 1 * time.Second,
		LeaseDuration:    5 * time.Second,
	})

	var executed atomic.Bool
	w.Register("test.task", HandlerFunc(func(ctx context.Context, job core.Job) error {
		executed.Store(true)
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerExited := make(chan struct{})
	go func() {
		_ = w.Start(ctx)
		close(workerExited)
	}()

	// Submit job
	job, err := eng.Submit(&core.Job{
		Type:  "test.task",
		Queue: "default",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait until job reaches StateSucceeded on coordinator
	deadline := time.Now().Add(5 * time.Second)
	var updated *core.Job
	for time.Now().Before(deadline) {
		updated, _ = eng.GetJob(job.ID)
		if updated.State == core.StateSucceeded {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if updated == nil || updated.State != core.StateSucceeded {
		t.Fatalf("expected StateSucceeded, got %v", updated.State)
	}

	cancel()
	<-workerExited
}

func TestWorker_PanicRecovery(t *testing.T) {
	ts, eng := setupCoordinator(t)
	defer ts.Close()
	defer eng.Close()

	w := NewWorker(Config{
		CoordinatorURL:   ts.URL,
		WorkerID:         "panic-worker",
		Capacity:         2,
		PollWaitDuration: 1 * time.Second,
		LeaseDuration:    5 * time.Second,
	})

	var ran atomic.Int32
	w.Register("panic.task", HandlerFunc(func(ctx context.Context, job core.Job) error {
		ran.Add(1)
		panic("simulated critical handler crash")
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerExited := make(chan struct{})
	go func() {
		_ = w.Start(ctx)
		close(workerExited)
	}()

	job, err := eng.Submit(&core.Job{
		Type:        "panic.task",
		Queue:       "default",
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait until panic is caught and job is returned to pending on coordinator
	deadline := time.Now().Add(5 * time.Second)
	var updated *core.Job
	for time.Now().Before(deadline) {
		updated, _ = eng.GetJob(job.ID)
		if ran.Load() > 0 && updated.State == core.StatePending {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if updated == nil || updated.State != core.StatePending {
		t.Fatalf("expected StatePending after recoverable panic, got %v", updated.State)
	}

	cancel()
	<-workerExited
}

func TestWorker_PermanentFailure_DeadLetter(t *testing.T) {
	ts, eng := setupCoordinator(t)
	defer ts.Close()
	defer eng.Close()

	w := NewWorker(Config{
		CoordinatorURL:   ts.URL,
		WorkerID:         "dead-worker",
		Capacity:         1,
		PollWaitDuration: 1 * time.Second,
		LeaseDuration:    5 * time.Second,
	})

	w.Register("bad.payload", HandlerFunc(func(ctx context.Context, job core.Job) error {
		return fmt.Errorf("malformed input: %w", ErrPermanent)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerExited := make(chan struct{})
	go func() {
		_ = w.Start(ctx)
		close(workerExited)
	}()

	job, _ := eng.Submit(&core.Job{
		Type:        "bad.payload",
		Queue:       "default",
		MaxAttempts: 10,
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		updated, _ := eng.GetJob(job.ID)
		if updated.State == core.StateDead {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	updated, _ := eng.GetJob(job.ID)
	if updated.State != core.StateDead {
		t.Fatalf("expected StateDead immediately for ErrPermanent, got %v", updated.State)
	}

	cancel()
	<-workerExited
}

func TestWorker_CooperativeCancellation(t *testing.T) {
	ts, eng := setupCoordinator(t)
	defer ts.Close()
	defer eng.Close()

	w := NewWorker(Config{
		CoordinatorURL:   ts.URL,
		WorkerID:         "cancel-worker",
		Capacity:         1,
		PollWaitDuration: 1 * time.Second,
		LeaseDuration:    2 * time.Second,
	})

	started := make(chan struct{})
	cancelled := make(chan struct{})

	w.Register("long.job", HandlerFunc(func(ctx context.Context, job core.Job) error {
		close(started)
		select {
		case <-ctx.Done():
			close(cancelled)
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return nil
		}
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerExited := make(chan struct{})
	go func() {
		_ = w.Start(ctx)
		close(workerExited)
	}()

	job, _ := eng.Submit(&core.Job{
		Type:  "long.job",
		Queue: "default",
	})

	// Wait until handler starts running
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	// Coordinator cancels job
	_, err := eng.Cancel(job.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Worker should observe cancellation via heartbeat and cancel context
	select {
	case <-cancelled:
		// Succeeded in cooperative cancellation!
	case <-time.After(5 * time.Second):
		t.Fatal("cooperative cancellation was not observed by handler")
	}

	cancel()
	<-workerExited
}

func TestWorker_GracefulDrain(t *testing.T) {
	ts, eng := setupCoordinator(t)
	defer ts.Close()
	defer eng.Close()

	w := NewWorker(Config{
		CoordinatorURL:   ts.URL,
		WorkerID:         "drain-worker",
		Capacity:         2,
		PollWaitDuration: 1 * time.Second,
		DrainTimeout:     5 * time.Second,
	})

	started := make(chan struct{})
	inFlightFinished := atomic.Bool{}

	w.Register("drain.task", HandlerFunc(func(ctx context.Context, job core.Job) error {
		close(started)
		time.Sleep(200 * time.Millisecond)
		inFlightFinished.Store(true)
		return nil
	}))

	workerCtx, workerCancel := context.WithCancel(context.Background())

	workerExited := make(chan struct{})
	go func() {
		_ = w.Start(workerCtx)
		close(workerExited)
	}()

	// Submit task
	_, _ = eng.Submit(&core.Job{Type: "drain.task", Queue: "default"})

	// Wait until task is actively executing in-flight
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("task did not start in-flight")
	}

	// Initiate drain by cancelling worker context while job is in-flight
	workerCancel()

	// Worker must finish the inFlight task before exiting
	select {
	case <-workerExited:
		if !inFlightFinished.Load() {
			t.Fatal("worker exited before in-flight job completed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain took too long")
	}
}
