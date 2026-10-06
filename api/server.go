package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/domains18/kombucha/core"
	"github.com/domains18/kombucha/storage/engine"
)

type ServerConfig struct {
	Engine     *engine.Engine
	ParkingLot *ParkingLot
	AuthToken  string
	Clock      core.Clock
}

type Server struct {
	cfg    ServerConfig
	mux    *http.ServeMux
	server *http.Server
}

func NewServer(cfg ServerConfig) *Server {
	if cfg.Clock == nil {
		cfg.Clock = core.RealClock{}
	}

	s := &Server{
		cfg: cfg,
		mux: http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /v1/jobs", s.handleAuth(s.handleSubmitJob))
	s.mux.HandleFunc("GET /v1/jobs/{id}", s.handleAuth(s.handleGetJob))
	s.mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.handleAuth(s.handleCancelJob))
	s.mux.HandleFunc("POST /v1/lease", s.handleAuth(s.handleLease))
	s.mux.HandleFunc("POST /v1/jobs/{id}/heartbeat", s.handleAuth(s.handleHeartbeat))
	s.mux.HandleFunc("POST /v1/jobs/{id}/complete", s.handleAuth(s.handleComplete))
	s.mux.HandleFunc("GET /v1/stats", s.handleAuth(s.handleStats))
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
}

func (s *Server) handleAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.AuthToken != "" {
			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.AuthToken)) != 1 {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
		}
		next(w, r)
	}
}

// ---------------- Handlers ----------------

type SubmitRequest struct {
	Type           string          `json:"type"`
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	RunAt          *time.Time      `json:"run_at,omitempty"`
	Priority       int8            `json:"priority,omitempty"`
	MaxAttempts    uint16          `json:"max_attempts,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

type SubmitResponse struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req SubmitRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"bad request: %v"}`, err), http.StatusBadRequest)
		return
	}

	if req.Type == "" {
		http.Error(w, `{"error":"type is required"}`, http.StatusBadRequest)
		return
	}
	if req.Queue == "" {
		req.Queue = "default"
	}
	if req.MaxAttempts == 0 {
		req.MaxAttempts = 3
	}

	runAt := s.cfg.Clock.Now()
	if req.RunAt != nil && !req.RunAt.IsZero() {
		runAt = *req.RunAt
	}

	job := &core.Job{
		Type:        req.Type,
		Queue:       req.Queue,
		Payload:     req.Payload,
		Priority:    req.Priority,
		MaxAttempts: req.MaxAttempts,
		RunAt:       runAt,
		IdemKey:     req.IdempotencyKey,
	}

	// Check if already exists for idempotency key to determine 201 vs 200
	var existingBefore bool
	if req.IdempotencyKey != "" {
		if _, ok := s.cfg.Engine.Index().GetByIdem(req.IdempotencyKey); ok {
			existingBefore = true
		}
	}

	resJob, err := s.cfg.Engine.Submit(job)
	if err != nil {
		if errors.Is(err, engine.ErrQueueFull) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, `{"error":"queue full, please retry"}`, http.StatusTooManyRequests)
			return
		}
		http.Error(w, fmt.Sprintf(`{"error":"submit failed: %v"}`, err), http.StatusInternalServerError)
		return
	}

	statusCode := http.StatusCreated
	if existingBefore {
		statusCode = http.StatusOK
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(SubmitResponse{
		ID:    resJob.ID.String(),
		State: resJob.State.String(),
	})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := core.ParseJobID(idStr)
	if err != nil {
		http.Error(w, `{"error":"invalid job id"}`, http.StatusBadRequest)
		return
	}

	job, ok := s.cfg.Engine.GetJob(id)
	if !ok {
		http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(job)
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := core.ParseJobID(idStr)
	if err != nil {
		http.Error(w, `{"error":"invalid job id"}`, http.StatusBadRequest)
		return
	}

	job, err := s.cfg.Engine.Cancel(id)
	if err != nil {
		if errors.Is(err, engine.ErrJobNotFound) {
			http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
			return
		}
		if errors.Is(err, engine.ErrTerminalJob) {
			http.Error(w, `{"error":"job is already in terminal state"}`, http.StatusConflict)
			return
		}
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	stateStr := job.State.String()
	if job.State == core.StateLeased && job.CancelReq {
		stateStr = "cancelling"
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"state": stateStr})
}

type LeaseRequest struct {
	WorkerID     string   `json:"worker_id"`
	Queues       []string `json:"queues"`
	Max          int      `json:"max"`
	LeaseSeconds int      `json:"lease_seconds"`
	WaitSeconds  int      `json:"wait_seconds"`
}

type LeasedItem struct {
	Job         *core.Job `json:"job"`
	LeaseToken  uint64    `json:"lease_token"`
	LeaseExpiry time.Time `json:"lease_expiry"`
}

type LeaseResponse struct {
	Jobs []LeasedItem `json:"jobs"`
}

func (s *Server) handleLease(w http.ResponseWriter, r *http.Request) {
	var req LeaseRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"bad request: %v"}`, err), http.StatusBadRequest)
		return
	}

	if req.WorkerID == "" {
		http.Error(w, `{"error":"worker_id is required"}`, http.StatusBadRequest)
		return
	}
	if req.Max <= 0 {
		req.Max = 1
	}
	if req.LeaseSeconds <= 0 {
		req.LeaseSeconds = 30
	}
	if req.WaitSeconds > 30 {
		req.WaitSeconds = 30
	}

	leaseDuration := time.Duration(req.LeaseSeconds) * time.Second
	waitTimeout := time.Duration(req.WaitSeconds) * time.Second

	deadline := s.cfg.Clock.Now().Add(waitTimeout)

	for {
		leasedJobs, err := s.cfg.Engine.Lease(req.Queues, req.WorkerID, req.Max, leaseDuration)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"lease error: %v"}`, err), http.StatusInternalServerError)
			return
		}

		if len(leasedJobs) > 0 {
			items := make([]LeasedItem, len(leasedJobs))
			for i, j := range leasedJobs {
				items[i] = LeasedItem{
					Job:         j,
					LeaseToken:  j.LeaseToken,
					LeaseExpiry: j.LeaseExpiry,
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(LeaseResponse{Jobs: items})
			return
		}

		// Nothing available. Check if wait requested
		remaining := deadline.Sub(s.cfg.Clock.Now())
		if remaining <= 0 || s.cfg.ParkingLot == nil {
			break
		}

		ctx, cancel := coreWithTimeout(r.Context(), remaining)
		woken := s.cfg.ParkingLot.Wait(ctx, req.Queues)
		cancel()

		if !woken {
			break
		}
	}

	// Empty response on timeout
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(LeaseResponse{Jobs: []LeasedItem{}})
}

func coreWithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, d)
}

type HeartbeatRequest struct {
	WorkerID     string `json:"worker_id"`
	LeaseToken   uint64 `json:"lease_token"`
	LeaseSeconds int    `json:"lease_seconds,omitempty"`
}

type HeartbeatResponse struct {
	LeaseExpiry time.Time `json:"lease_expiry"`
	Cancelled   bool      `json:"cancelled"`
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := core.ParseJobID(idStr)
	if err != nil {
		http.Error(w, `{"error":"invalid job id"}`, http.StatusBadRequest)
		return
	}

	var req HeartbeatRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"bad request: %v"}`, err), http.StatusBadRequest)
		return
	}

	duration := 30 * time.Second
	if req.LeaseSeconds > 0 {
		duration = time.Duration(req.LeaseSeconds) * time.Second
	}

	res, err := s.cfg.Engine.Heartbeat(id, req.WorkerID, req.LeaseToken, duration)
	if err != nil {
		if errors.Is(err, engine.ErrStaleToken) || errors.Is(err, engine.ErrJobNotLeased) {
			http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusConflict)
			return
		}
		if errors.Is(err, engine.ErrJobNotFound) {
			http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(HeartbeatResponse{
		LeaseExpiry: res.LeaseExpiry,
		Cancelled:   res.Cancelled,
	})
}

type CompleteRequest struct {
	WorkerID   string `json:"worker_id"`
	LeaseToken uint64 `json:"lease_token"`
	Success    bool   `json:"success"`
	Error      string `json:"error,omitempty"`
	Retryable  bool   `json:"retryable,omitempty"`
}

type CompleteResponse struct {
	State   string    `json:"state"`
	Attempt uint16    `json:"attempt"`
	RunAt   time.Time `json:"run_at"`
}

func (s *Server) handleComplete(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := core.ParseJobID(idStr)
	if err != nil {
		http.Error(w, `{"error":"invalid job id"}`, http.StatusBadRequest)
		return
	}

	var req CompleteRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"bad request: %v"}`, err), http.StatusBadRequest)
		return
	}

	job, err := s.cfg.Engine.Complete(engine.CompleteArgs{
		JobID:      id,
		WorkerID:   req.WorkerID,
		LeaseToken: req.LeaseToken,
		Success:    req.Success,
		Error:      req.Error,
		Retryable:  req.Retryable,
	})
	if err != nil {
		if errors.Is(err, engine.ErrStaleToken) || errors.Is(err, engine.ErrJobNotLeased) {
			http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusConflict)
			return
		}
		if errors.Is(err, engine.ErrJobNotFound) {
			http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(CompleteResponse{
		State:   job.State.String(),
		Attempt: job.Attempt,
		RunAt:   job.RunAt,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.cfg.Engine.Stats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `{"status":"ok"}`)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	stats := s.cfg.Engine.Stats()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	fmt.Fprintf(w, "# HELP kombucha_queue_depth Number of jobs in queue by state\n")
	fmt.Fprintf(w, "# TYPE kombucha_queue_depth gauge\n")
	for q, st := range stats {
		fmt.Fprintf(w, "kombucha_queue_depth{queue=%q,state=\"pending\"} %d\n", q, st.Pending)
		fmt.Fprintf(w, "kombucha_queue_depth{queue=%q,state=\"leased\"} %d\n", q, st.Leased)
		fmt.Fprintf(w, "kombucha_queue_depth{queue=%q,state=\"succeeded\"} %d\n", q, st.Succeeded)
		fmt.Fprintf(w, "kombucha_queue_depth{queue=%q,state=\"dead\"} %d\n", q, st.Dead)
		fmt.Fprintf(w, "kombucha_queue_depth{queue=%q,state=\"cancelled\"} %d\n", q, st.Cancelled)
	}

	fmt.Fprintf(w, "# HELP kombucha_oldest_pending_age_seconds Oldest pending job age in seconds\n")
	fmt.Fprintf(w, "# TYPE kombucha_oldest_pending_age_seconds gauge\n")
	for q, st := range stats {
		fmt.Fprintf(w, "kombucha_oldest_pending_age_seconds{queue=%q} %.3f\n", q, st.OldestPendingAge.Seconds())
	}
}
