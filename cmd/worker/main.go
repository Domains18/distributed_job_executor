package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/domains18/kombucha/worker"
	"github.com/domains18/kombucha/workers/jobs"
)

func main() {
	coordinatorURL := flag.String("coordinator", "http://localhost:8080", "Coordinator HTTP endpoint")
	workerID := flag.String("id", "", "Unique worker identifier")
	queuesFlag := flag.String("queues", "critical,email,default,bulk", "Comma-separated queue names to drain")
	capacity := flag.Int("capacity", 10, "Maximum concurrent in-flight jobs")
	authToken := flag.String("auth-token", "", "Bearer token for coordinator API")
	flag.Parse()

	if *workerID == "" {
		hostname, _ := os.Hostname()
		*workerID = fmt.Sprintf("%s-%d", hostname, os.Getpid())
	}

	queueList := strings.Split(*queuesFlag, ",")
	for i := range queueList {
		queueList[i] = strings.TrimSpace(queueList[i])
	}

	w := worker.NewWorker(worker.Config{
		CoordinatorURL:   *coordinatorURL,
		WorkerID:         *workerID,
		Queues:           queueList,
		Capacity:         *capacity,
		LeaseDuration:    30 * time.Second,
		PollWaitDuration: 15 * time.Second,
		AuthToken:        *authToken,
	})

	// Register Kombucha Co handlers
	stock := map[string]int{
		"KOMBUCHA-GINGER": 1000,
		"KOMBUCHA-BERRY":  1000,
	}
	w.Register("order.confirmation_email", jobs.NewEmailHandler(0.0))
	w.Register("inventory.reserve", jobs.NewInventoryHandler(stock))
	w.Register("payment.capture", jobs.NewPaymentHandler(0.0))
	w.Register("invoice.generate", jobs.NewInvoiceHandler())
	w.Register("analytics.sync", jobs.NewAnalyticsHandler())
	w.Register("search.index", jobs.NewSearchIndexHandler())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh
		slog.Info("received termination signal, initiating graceful drain...")
		cancel()
	}()

	slog.Info("starting worker", "worker_id", *workerID, "queues", queueList, "capacity", *capacity)
	if err := w.Start(ctx); err != nil {
		slog.Error("worker stopped with error", "err", err)
	}

	slog.Info("worker finished cleanly")
}
