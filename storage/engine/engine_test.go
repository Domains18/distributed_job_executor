package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/domains18/kombucha/core"
	"github.com/domains18/kombucha/storage/wal"
)

func TestEngine_StateMachine_HappyPath(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(Config{
		DataDir:    dir,
		WALOptions: wal.Options{Sync: true},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer eng.Close()

	// 1. Submit
	submitted, err := eng.Submit(&core.Job{
		Type:        "order.confirmation_email",
		Queue:       "email",
		Payload:     []byte(`{"order_id":101}`),
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if submitted.State != core.StatePending {
		t.Fatalf("expected pending state, got %v", submitted.State)
	}
	if submitted.Attempt != 0 {
		t.Fatalf("expected attempt 0, got %d", submitted.Attempt)
	}

	// 2. Lease
	leased, err := eng.Lease([]string{"email"}, "worker-A", 1, 30*time.Second)
	if err != nil {
		t.Fatalf("Lease failed: %v", err)
	}
	if len(leased) != 1 {
		t.Fatalf("expected 1 leased job, got %d", len(leased))
	}
	j := leased[0]
	if j.ID != submitted.ID {
		t.Fatalf("expected job %v, got %v", submitted.ID, j.ID)
	}
	if j.State != core.StateLeased {
		t.Fatalf("expected leased state, got %v", j.State)
	}
	if j.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", j.Attempt)
	}
	if j.LeaseToken != 1 {
		t.Fatalf("expected lease token 1, got %d", j.LeaseToken)
	}

	// 3. Complete (Success)
	completed, err := eng.Complete(CompleteArgs{
		JobID:      j.ID,
		WorkerID:   "worker-A",
		LeaseToken: j.LeaseToken,
		Success:    true,
	})
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}
	if completed.State != core.StateSucceeded {
		t.Fatalf("expected succeeded state, got %v", completed.State)
	}
}

func TestEngine_FencingToken_WithFakeClock(t *testing.T) {
	dir := t.TempDir()
	clock := core.NewFakeClock(time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC))

	eng, err := Open(Config{
		DataDir:    dir,
		Clock:      clock,
		WALOptions: wal.Options{Sync: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Submit job
	job, err := eng.Submit(&core.Job{
		Type:        "inventory.reserve",
		Queue:       "critical",
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Worker A leases for 10 seconds
	leasedA, err := eng.Lease([]string{"critical"}, "worker-A", 1, 10*time.Second)
	if err != nil || len(leasedA) != 1 {
		t.Fatalf("worker A lease failed: %v", err)
	}
	tokenA := leasedA[0].LeaseToken
	if tokenA != 1 {
		t.Fatalf("expected token 1, got %d", tokenA)
	}

	// Advance clock past lease expiry (15 seconds)
	clock.Advance(15 * time.Second)

	// Scheduler runs ExpireLeases
	expiredCount, err := eng.ExpireLeases()
	if err != nil {
		t.Fatal(err)
	}
	if expiredCount != 1 {
		t.Fatalf("expected 1 expired lease, got %d", expiredCount)
	}

	// Worker B leases the expired job
	leasedB, err := eng.Lease([]string{"critical"}, "worker-B", 1, 10*time.Second)
	if err != nil || len(leasedB) != 1 {
		t.Fatalf("worker B lease failed: %v", err)
	}
	tokenB := leasedB[0].LeaseToken
	if tokenB != 2 {
		t.Fatalf("expected token 2, got %d", tokenB)
	}

	// Worker A wakes up from GC pause and attempts to complete with stale tokenA (1)
	_, errA := eng.Complete(CompleteArgs{
		JobID:      job.ID,
		WorkerID:   "worker-A",
		LeaseToken: tokenA,
		Success:    true,
	})
	if !errors.Is(errA, ErrStaleToken) {
		t.Fatalf("expected ErrStaleToken for Worker A, got %v", errA)
	}

	// Worker B completes with tokenB (2) -> must succeed!
	completedB, errB := eng.Complete(CompleteArgs{
		JobID:      job.ID,
		WorkerID:   "worker-B",
		LeaseToken: tokenB,
		Success:    true,
	})
	if errB != nil {
		t.Fatalf("worker B complete failed: %v", errB)
	}
	if completedB.State != core.StateSucceeded {
		t.Fatalf("expected state succeeded, got %v", completedB.State)
	}
}

func TestEngine_AttemptCountsAtLeaseTime_PoisonPill(t *testing.T) {
	dir := t.TempDir()
	clock := core.NewFakeClock(time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC))

	eng, err := Open(Config{
		DataDir:    dir,
		Clock:      clock,
		WALOptions: wal.Options{Sync: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// MaxAttempts = 2
	job, err := eng.Submit(&core.Job{
		Type:        "poison.pill",
		Queue:       "default",
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 1st Lease -> burns attempt 1 immediately
	leased1, _ := eng.Lease([]string{"default"}, "w-1", 1, 10*time.Second)
	if leased1[0].Attempt != 1 {
		t.Fatalf("expected attempt 1 at lease time, got %d", leased1[0].Attempt)
	}

	// w-1 vanishes. Expire lease
	clock.Advance(15 * time.Second)
	eng.ExpireLeases()

	// 2nd Lease -> burns attempt 2 immediately
	leased2, _ := eng.Lease([]string{"default"}, "w-2", 1, 10*time.Second)
	if leased2[0].Attempt != 2 {
		t.Fatalf("expected attempt 2 at lease time, got %d", leased2[0].Attempt)
	}

	// w-2 reports retryable failure, but attempts (2) >= MaxAttempts (2)
	failed, err := eng.Complete(CompleteArgs{
		JobID:      job.ID,
		WorkerID:   "w-2",
		LeaseToken: leased2[0].LeaseToken,
		Success:    false,
		Error:      "poison crashed",
		Retryable:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Must be Dead-Lettered, NOT retried!
	if failed.State != core.StateDead {
		t.Fatalf("expected StateDead after exhausting max attempts, got %v", failed.State)
	}
}

func TestEngine_Idempotency(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(Config{
		DataDir:    dir,
		WALOptions: wal.Options{Sync: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	j1, err := eng.Submit(&core.Job{
		Type:        "order.confirmation",
		Queue:       "email",
		IdemKey:     "order-999-confirm",
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Re-submit with same idempotency key
	j2, err := eng.Submit(&core.Job{
		Type:        "order.confirmation",
		Queue:       "email",
		IdemKey:     "order-999-confirm",
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	if j1.ID != j2.ID {
		t.Fatalf("idempotency violated: expected same job ID %v, got %v", j1.ID, j2.ID)
	}

	stats := eng.Stats()
	if stats["email"].Pending != 1 {
		t.Fatalf("expected only 1 pending job, got %d", stats["email"].Pending)
	}
}

func TestEngine_Recovery_LeaseReconciliation(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:    dir,
		WALOptions: wal.Options{Sync: true},
	}

	// 1. Run coordinator, submit and lease
	eng1, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}

	job, err := eng1.Submit(&core.Job{
		Type:        "billing.charge",
		Queue:       "critical",
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	leased, err := eng1.Lease([]string{"critical"}, "worker-1", 1, 30*time.Second)
	if err != nil || len(leased) != 1 {
		t.Fatal("lease failed")
	}
	token := leased[0].LeaseToken

	// Coordinator abruptly stops / restarts with lease held
	eng1.Close()

	// 2. Recover coordinator
	eng2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer eng2.Close()

	recoveredJob, ok := eng2.GetJob(job.ID)
	if !ok {
		t.Fatal("job not found after recovery")
	}

	// Lease reconciliation check: Leased -> Pending
	if recoveredJob.State != core.StatePending {
		t.Fatalf("expected lease reconciled to StatePending, got %v", recoveredJob.State)
	}

	// Old worker-1 attempts to complete with previous token -> must be rejected
	_, errComplete := eng2.Complete(CompleteArgs{
		JobID:      job.ID,
		WorkerID:   "worker-1",
		LeaseToken: token,
		Success:    true,
	})
	if errComplete == nil {
		t.Fatal("expected completion with old token to fail after recovery reconciliation")
	}

	// New worker leases the job
	newLease, err := eng2.Lease([]string{"critical"}, "worker-2", 1, 30*time.Second)
	if err != nil || len(newLease) != 1 {
		t.Fatalf("re-lease after recovery failed: %v", err)
	}
	if newLease[0].LeaseToken <= token {
		t.Fatalf("expected new lease token > %d, got %d", token, newLease[0].LeaseToken)
	}
}

func TestEngine_SnapshotAndRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:    dir,
		WALOptions: wal.Options{Sync: true},
	}

	eng1, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Submit 20 jobs
	for i := 0; i < 20; i++ {
		_, err := eng1.Submit(&core.Job{
			Type:        "test.job",
			Queue:       "default",
			Payload:     []byte(`{"index":1}`),
			MaxAttempts: 3,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Lease and complete 5
	leased, _ := eng1.Lease([]string{"default"}, "worker-1", 5, 10*time.Second)
	for _, j := range leased {
		_, _ = eng1.Complete(CompleteArgs{
			JobID:      j.ID,
			WorkerID:   "worker-1",
			LeaseToken: j.LeaseToken,
			Success:    true,
		})
	}

	// Take Snapshot
	snapLSN, err := eng1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if snapLSN == 0 {
		t.Fatal("expected non-zero snapshot LSN")
	}

	// Mutate 5 more after snapshot
	for i := 0; i < 5; i++ {
		_, err := eng1.Submit(&core.Job{
			Type:        "post.snap.job",
			Queue:       "default",
			MaxAttempts: 3,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	eng1.Close()

	// Reopen from snapshot + replayed tail
	eng2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen after snapshot failed: %v", err)
	}
	defer eng2.Close()

	stats := eng2.Stats()
	defStats := stats["default"]
	if defStats.Succeeded != 5 {
		t.Fatalf("expected 5 succeeded jobs, got %d", defStats.Succeeded)
	}
	if defStats.Pending != 20 { // 15 original + 5 new
		t.Fatalf("expected 20 pending jobs, got %d", defStats.Pending)
	}
}

func TestEngine_Cancellation(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(Config{
		DataDir:    dir,
		WALOptions: wal.Options{Sync: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// 1. Cancel pending job
	j1, _ := eng.Submit(&core.Job{Type: "cancel.pending", Queue: "default"})
	cancelled1, err := eng.Cancel(j1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled1.State != core.StateCancelled {
		t.Fatalf("expected cancelled state, got %v", cancelled1.State)
	}

	// 2. Cancel leased job -> cooperative cancel flag
	j2, _ := eng.Submit(&core.Job{Type: "cancel.leased", Queue: "default"})
	leased, _ := eng.Lease([]string{"default"}, "w-1", 1, 30*time.Second)

	cancelled2, err := eng.Cancel(j2.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Still StateLeased until worker completes or lease expires
	if cancelled2.State != core.StateLeased || !cancelled2.CancelReq {
		t.Fatalf("expected StateLeased with CancelReq=true, got %+v", cancelled2)
	}

	// Worker heartbeats -> observes cancellation!
	hb, err := eng.Heartbeat(j2.ID, "w-1", leased[0].LeaseToken, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !hb.Cancelled {
		t.Fatal("expected heartbeat to return Cancelled=true")
	}

	// Worker completes -> becomes StateCancelled
	completed, err := eng.Complete(CompleteArgs{
		JobID:      j2.ID,
		WorkerID:   "w-1",
		LeaseToken: leased[0].LeaseToken,
		Success:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != core.StateCancelled {
		t.Fatalf("expected job to become StateCancelled on complete, got %v", completed.State)
	}
}

func TestEngine_DoubleReplay_Idempotency(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:    dir,
		WALOptions: wal.Options{Sync: true},
	}

	eng, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Submit jobs
	for i := 0; i < 10; i++ {
		_, _ = eng.Submit(&core.Job{
			Type:        "idempotency.job",
			Queue:       "default",
			MaxAttempts: 3,
		})
	}
	eng.Close()

	// Replay 1
	eng1, err := Open(cfg)
	if err != nil {
		t.Fatalf("first recovery failed: %v", err)
	}
	jobs1 := eng1.Index().AllJobs()
	eng1.Close()

	// Replay 2
	eng2, err := Open(cfg)
	if err != nil {
		t.Fatalf("second recovery failed: %v", err)
	}
	jobs2 := eng2.Index().AllJobs()
	eng2.Close()

	b1, _ := json.Marshal(jobs1)
	b2, _ := json.Marshal(jobs2)

	if !bytes.Equal(b1, b2) {
		t.Fatalf("double replay produced different index states:\n1: %s\n2: %s", string(b1), string(b2))
	}
}
