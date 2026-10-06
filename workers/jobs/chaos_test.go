package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/domains18/kombucha/api"
	"github.com/domains18/kombucha/core"
	"github.com/domains18/kombucha/scheduler"
	"github.com/domains18/kombucha/storage/engine"
	"github.com/domains18/kombucha/storage/wal"
	"github.com/domains18/kombucha/worker"
)

func TestKombuchaCo_ChaosRun(t *testing.T) {
	dir := t.TempDir()
	parking := api.NewParkingLot()

	// Engine configuration with fast backoff for testing
	eng, err := engine.Open(engine.Config{
		DataDir:        dir,
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
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
	defer eng.Close()

	// Start scheduler
	sched := scheduler.New(eng, parking, core.RealClock{})
	sched.Start()
	defer sched.Stop()

	// Start API server
	srv := api.NewServer(api.ServerConfig{
		Engine:     eng,
		ParkingLot: parking,
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Handlers with 10% simulated failure rate
	initialStock := map[string]int{"KOMBUCHA-GINGER": 100}
	emailH := NewEmailHandler(0.10)
	invH := NewInventoryHandler(initialStock)
	payH := NewPaymentHandler(0.10)
	invcH := NewInvoiceHandler()
	analyticsH := NewAnalyticsHandler()
	searchH := NewSearchIndexHandler()

	ctx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()

	// Spawn 3 worker processes
	for i := 1; i <= 3; i++ {
		w := worker.NewWorker(worker.Config{
			CoordinatorURL:   ts.URL,
			WorkerID:         fmt.Sprintf("kombucha-worker-%d", i),
			Queues:           []string{"critical", "email", "default", "bulk"},
			Capacity:         5,
			LeaseDuration:    2 * time.Second,
			PollWaitDuration: 1 * time.Second,
		})

		w.Register("order.confirmation_email", emailH)
		w.Register("inventory.reserve", invH)
		w.Register("payment.capture", payH)
		w.Register("invoice.generate", invcH)
		w.Register("analytics.sync", analyticsH)
		w.Register("search.index", searchH)

		go func() {
			_ = w.Start(ctx)
		}()
	}

	// Submit 50 orders (each submitting 6 jobs = 300 jobs total)
	numOrders := 50
	var jobIDs []core.JobID

	for i := 1; i <= numOrders; i++ {
		orderID := fmt.Sprintf("ord-%04d", i)

		// 1. Email
		emailPayload, _ := json.Marshal(EmailPayload{
			OrderID:    orderID,
			Email:      fmt.Sprintf("customer%d@example.com", i),
			OrderTotal: 2500,
		})
		j1, _ := eng.Submit(&core.Job{
			Type:        "order.confirmation_email",
			Queue:       "email",
			Payload:     emailPayload,
			MaxAttempts: 5,
		})
		jobIDs = append(jobIDs, j1.ID)

		// 2. Inventory
		invPayload, _ := json.Marshal(InventoryPayload{
			OrderID: orderID,
			SKU:     "KOMBUCHA-GINGER",
			Qty:     1,
		})
		j2, _ := eng.Submit(&core.Job{
			Type:        "inventory.reserve",
			Queue:       "critical",
			Payload:     invPayload,
			MaxAttempts: 5,
		})
		jobIDs = append(jobIDs, j2.ID)

		// 3. Payment
		payPayload, _ := json.Marshal(PaymentPayload{
			OrderID:        orderID,
			AmountCents:    2500,
			IdempotencyKey: fmt.Sprintf("pay-%s", orderID),
		})
		j3, _ := eng.Submit(&core.Job{
			Type:        "payment.capture",
			Queue:       "critical",
			Payload:     payPayload,
			MaxAttempts: 5,
		})
		jobIDs = append(jobIDs, j3.ID)

		// 4. Invoice
		invcPayload, _ := json.Marshal(InvoicePayload{OrderID: orderID})
		j4, _ := eng.Submit(&core.Job{
			Type:        "invoice.generate",
			Queue:       "default",
			Payload:     invcPayload,
			MaxAttempts: 5,
		})
		jobIDs = append(jobIDs, j4.ID)

		// 5. Analytics
		j5, _ := eng.Submit(&core.Job{
			Type:        "analytics.sync",
			Queue:       "bulk",
			MaxAttempts: 5,
		})
		jobIDs = append(jobIDs, j5.ID)

		// 6. Search
		j6, _ := eng.Submit(&core.Job{
			Type:        "search.index",
			Queue:       "bulk",
			MaxAttempts: 5,
		})
		jobIDs = append(jobIDs, j6.ID)
	}

	// Wait until all 300 jobs reach a terminal state
	deadline := time.Now().Add(15 * time.Second)
	allTerminal := false

	for time.Now().Before(deadline) {
		terminalCount := 0
		for _, id := range jobIDs {
			j, ok := eng.GetJob(id)
			if ok && j.State.IsTerminal() {
				terminalCount++
			}
		}

		if terminalCount == len(jobIDs) {
			allTerminal = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !allTerminal {
		t.Fatalf("not all jobs completed within deadline")
	}

	// VERIFY INVARIANTS:
	// 1. Inventory was reserved for exactly the successful inventory jobs
	successfulInvJobs := 0
	for _, id := range jobIDs {
		j, _ := eng.GetJob(id)
		if j.Type == "inventory.reserve" && j.State == core.StateSucceeded {
			successfulInvJobs++
		}
	}

	expectedStock := initialStock["KOMBUCHA-GINGER"] - successfulInvJobs
	actualStock := invH.Stock("KOMBUCHA-GINGER")
	if actualStock != expectedStock {
		t.Fatalf("inventory double-decrement detected! expected stock %d, got %d", expectedStock, actualStock)
	}

	// 2. Each order's email sent at most once
	for i := 1; i <= numOrders; i++ {
		orderID := fmt.Sprintf("ord-%04d", i)
		count := emailH.SentCount(orderID)
		if count > 1 {
			t.Fatalf("order %s had email sent %d times (expected <= 1)", orderID, count)
		}
	}
}
