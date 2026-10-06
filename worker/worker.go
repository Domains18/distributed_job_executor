package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/domains18/kombucha/core"
)

var (
	// ErrPermanent causes the engine to dead-letter the job immediately without retrying.
	ErrPermanent = errors.New("worker: permanent failure, do not retry")
)

// Handler processes a specific job type.
type Handler interface {
	Run(ctx context.Context, job core.Job) error
}

type HandlerFunc func(ctx context.Context, job core.Job) error

func (f HandlerFunc) Run(ctx context.Context, job core.Job) error {
	return f(ctx, job)
}

// Config configures the worker process.
type Config struct {
	CoordinatorURL   string
	WorkerID         string
	Queues           []string
	Capacity         int
	LeaseDuration    time.Duration
	PollWaitDuration time.Duration
	DrainTimeout     time.Duration
	AuthToken        string
}

func (c *Config) defaults() {
	if c.Capacity <= 0 {
		c.Capacity = 10
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.PollWaitDuration <= 0 {
		c.PollWaitDuration = 10 * time.Second
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 30 * time.Second
	}
	if len(c.Queues) == 0 {
		c.Queues = []string{"default"}
	}
	if c.WorkerID == "" {
		c.WorkerID = fmt.Sprintf("worker-%d", time.Now().UnixNano())
	}
}

// Worker polls for jobs, runs registered handlers, maintains heartbeats, and drains cleanly.
type Worker struct {
	cfg      Config
	client   *Client
	handlers map[string]Handler
	mu       sync.RWMutex
	wg       sync.WaitGroup
	sem      chan struct{}
}

// NewWorker creates an initialized Worker.
func NewWorker(cfg Config) *Worker {
	cfg.defaults()
	return &Worker{
		cfg:      cfg,
		client:   NewClient(cfg.CoordinatorURL, cfg.AuthToken, cfg.PollWaitDuration+5*time.Second),
		handlers: make(map[string]Handler),
		sem:      make(chan struct{}, cfg.Capacity),
	}
}

// Register registers a handler for a job type.
func (w *Worker) Register(jobType string, h Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[jobType] = h
}

// Start begins the worker leasing loop and runs until ctx is cancelled.
// Upon ctx cancellation, it initiates graceful draining.
func (w *Worker) Start(ctx context.Context) error {
	slog.Info("worker started", "worker_id", w.cfg.WorkerID, "capacity", w.cfg.Capacity, "queues", w.cfg.Queues)

	for {
		select {
		case <-ctx.Done():
			return w.drain()
		default:
		}

		// Calculate available capacity
		available := cap(w.sem) - len(w.sem)
		if available <= 0 {
			// At full capacity, wait briefly for a slot
			time.Sleep(50 * time.Millisecond)
			continue
		}

		waitSec := int(w.cfg.PollWaitDuration.Seconds())
		leaseSec := int(w.cfg.LeaseDuration.Seconds())

		items, err := w.client.Lease(ctx, w.cfg.WorkerID, w.cfg.Queues, available, leaseSec, waitSec)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return w.drain()
			}
			slog.Warn("lease poll error", "err", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, item := range items {
			w.sem <- struct{}{}
			w.wg.Add(1)
			go w.execute(item)
		}
	}
}

func (w *Worker) execute(item LeaseItem) {
	defer func() {
		<-w.sem
		w.wg.Done()
	}()

	w.mu.RLock()
	handler, exists := w.handlers[item.Job.Type]
	w.mu.RUnlock()

	if !exists {
		slog.Error("no handler registered for job type", "type", item.Job.Type, "id", item.Job.ID)
		_ = w.client.Complete(context.Background(), item.Job.ID, w.cfg.WorkerID, item.LeaseToken, false, "no handler registered", false)
		return
	}

	jobCtx, cancelJob := context.WithCancel(context.Background())
	defer cancelJob()

	// Heartbeat ticker every leaseDuration / 3
	hbInterval := w.cfg.LeaseDuration / 3
	if hbInterval < 500*time.Millisecond {
		hbInterval = 500 * time.Millisecond
	}

	stopHB := make(chan struct{})
	go func() {
		ticker := time.NewTicker(hbInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stopHB:
				return
			case <-jobCtx.Done():
				return
			case <-ticker.C:
				leaseSec := int(w.cfg.LeaseDuration.Seconds())
				res, err := w.client.Heartbeat(jobCtx, item.Job.ID, w.cfg.WorkerID, item.LeaseToken, leaseSec)
				if err != nil {
					if errors.Is(err, ErrStaleToken) {
						slog.Warn("stale lease token on heartbeat, abandoning job", "id", item.Job.ID)
						cancelJob()
						return
					}
					slog.Warn("heartbeat failed", "id", item.Job.ID, "err", err)
					continue
				}

				if res.Cancelled {
					slog.Info("cooperative cancellation received", "id", item.Job.ID)
					cancelJob()
					return
				}
			}
		}
	}()

	// Execute handler with panic recovery
	var execErr error
	var panicked bool
	var panicVal any

	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				panicVal = r
			}
		}()
		execErr = handler.Run(jobCtx, item.Job)
	}()

	close(stopHB)

	// Determine completion parameters
	var success bool
	var errMsg string
	var retryable bool

	if panicked {
		success = false
		errMsg = fmt.Sprintf("handler panic: %v", panicVal)
		retryable = true // panic defaults to retryable
		slog.Error("handler panicked", "id", item.Job.ID, "panic", panicVal)
	} else if execErr != nil {
		success = false
		errMsg = execErr.Error()
		if errors.Is(execErr, ErrPermanent) {
			retryable = false
		} else {
			retryable = true
		}
		slog.Warn("handler returned error", "id", item.Job.ID, "err", execErr, "retryable", retryable)
	} else {
		success = true
	}

	compCtx, compCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer compCancel()

	if err := w.client.Complete(compCtx, item.Job.ID, w.cfg.WorkerID, item.LeaseToken, success, errMsg, retryable); err != nil {
		slog.Warn("complete report error", "id", item.Job.ID, "err", err)
	}
}

func (w *Worker) drain() error {
	slog.Info("draining worker...", "worker_id", w.cfg.WorkerID)

	drainDone := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(drainDone)
	}()

	select {
	case <-drainDone:
		slog.Info("worker drained cleanly", "worker_id", w.cfg.WorkerID)
		return nil
	case <-time.After(w.cfg.DrainTimeout):
		slog.Warn("worker drain deadline reached", "worker_id", w.cfg.WorkerID)
		return errors.New("worker: drain deadline exceeded")
	}
}
