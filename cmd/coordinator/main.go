package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/domains18/kombucha/api"
	"github.com/domains18/kombucha/core"
	"github.com/domains18/kombucha/scheduler"
	"github.com/domains18/kombucha/storage/engine"
	"github.com/domains18/kombucha/storage/wal"
)

func main() {
	dataDir := flag.String("data-dir", "./data", "Directory to store WAL and snapshots")
	addr := flag.String("addr", ":8080", "HTTP server address")
	authToken := flag.String("auth-token", "", "Shared bearer authentication token")
	flag.Parse()

	slog.Info("starting Kombucha coordinator", "data_dir", *dataDir, "addr", *addr)

	parking := api.NewParkingLot()

	eng, err := engine.Open(engine.Config{
		DataDir: *dataDir,
		WALOptions: wal.Options{
			Sync: true,
		},
		OnJobEligible: func(queue string) {
			parking.Signal(queue)
		},
	})
	if err != nil {
		slog.Error("failed to start engine", "err", err)
		os.Exit(1)
	}

	sched := scheduler.New(eng, parking, core.RealClock{})
	sched.Start()

	srv := api.NewServer(api.ServerConfig{
		Engine:     eng,
		ParkingLot: parking,
		AuthToken:  *authToken,
	})

	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      40 * time.Second,
	}

	// Graceful shutdown on SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "err", err)
			os.Exit(1)
		}
	}()

	slog.Info("coordinator ready to serve requests", "addr", *addr)
	<-sigCh
	slog.Info("shutting down coordinator...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = httpServer.Shutdown(shutdownCtx)
	sched.Stop()
	if err := eng.Close(); err != nil {
		slog.Error("error closing storage engine", "err", err)
	}

	slog.Info("coordinator stopped cleanly")
	fmt.Println("Kombucha coordinator terminated.")
}
