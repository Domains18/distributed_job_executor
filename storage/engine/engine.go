package engine

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/domains18/kombucha/core"
	"github.com/domains18/kombucha/storage/index"
	"github.com/domains18/kombucha/storage/snapshot"
	"github.com/domains18/kombucha/storage/wal"
)

var (
	ErrStaleToken   = errors.New("engine: stale lease token (conflict)")
	ErrJobNotFound  = errors.New("engine: job not found")
	ErrJobNotLeased = errors.New("engine: job is not currently leased")
	ErrTerminalJob  = errors.New("engine: job is in terminal state")
	ErrQueueFull    = errors.New("engine: writer command queue full")
	ErrEngineHalted = errors.New("engine: engine is halted")
)

type opcode uint8

const (
	opSubmit opcode = iota + 1
	opLease
	opComplete
	opHeartbeat
	opCancel
	opExpire
	opSnapshot
)

type command struct {
	op    opcode
	args  any
	reply chan result
}

type result struct {
	val any
	err error
}

// Log defines the durability interface consumed by Engine. Satisfied by *wal.Log.
type Log interface {
	Append(recs []core.Record) (core.LSN, error)
	Replay(from core.LSN, fn func(core.Record) error) error
	TruncateBefore(watermark core.LSN) error
	Close() error
}

// Config configures the storage engine.
type Config struct {
	DataDir         string
	MaxBatchSize    int
	CommandQueueCap int
	WALOptions      wal.Options
	Clock           core.Clock
	InitialBackoff  time.Duration
	MaxBackoff      time.Duration
	SnapshotCadence int // Snapshot every N mutations (0 to disable auto-snapshot)
	OnJobEligible   func(queue string)
}

func (c *Config) defaults() {
	if c.MaxBatchSize <= 0 {
		c.MaxBatchSize = 1000
	}
	if c.CommandQueueCap <= 0 {
		c.CommandQueueCap = 10000
	}
	if c.Clock == nil {
		c.Clock = core.RealClock{}
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = time.Second
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = time.Hour
	}
	if c.SnapshotCadence <= 0 {
		c.SnapshotCadence = 10000
	}
}

// Engine coordinates the in-memory index, WAL, and snapshots via a single-writer loop.
type Engine struct {
	cfg     Config
	log     Log
	index   *index.Index
	cmdChan chan command
	stopCh  chan struct{}
	doneCh  chan struct{}
	rng     *rand.Rand
	rngMu   sync.Mutex

	mu             sync.RWMutex
	halted         bool
	haltErr        error
	lastAppliedLSN core.LSN
	mutationsCount int
}

// Open creates or restores an Engine instance from dataDir.
func Open(cfg Config) (*Engine, error) {
	cfg.defaults()

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("engine: failed to create data dir: %w", err)
	}

	snapDir := filepath.Join(cfg.DataDir, "snapshots")
	walDir := filepath.Join(cfg.DataDir, "wal")

	// 1. Load latest valid snapshot
	snapLSN, snapJobs, err := snapshot.LoadNewest(snapDir)
	if err != nil {
		return nil, fmt.Errorf("engine: failed to load snapshot: %w", err)
	}

	idx := index.New()
	for _, j := range snapJobs {
		idx.Put(j)
	}

	// 2. Open WAL
	log, err := wal.Open(walDir, cfg.WALOptions)
	if err != nil {
		return nil, fmt.Errorf("engine: failed to open wal: %w", err)
	}

	e := &Engine{
		cfg:            cfg,
		log:            log,
		index:          idx,
		cmdChan:        make(chan command, cfg.CommandQueueCap),
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
		rng:            rand.New(rand.NewSource(time.Now().UnixNano())),
		lastAppliedLSN: snapLSN,
	}

	// 3. Replay WAL from snapshotLSN + 1
	err = log.Replay(snapLSN+1, func(rec core.Record) error {
		e.applyRecord(rec)
		return nil
	})
	if err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("engine: replay error: %w", err)
	}

	// 4. Lease reconciliation: any job left in StateLeased reverts to StatePending at now
	now := cfg.Clock.Now()
	allJobs := idx.AllJobs()
	for _, j := range allJobs {
		if j.State == core.StateLeased {
			if j.CancelReq {
				j.State = core.StateCancelled
			} else {
				j.State = core.StatePending
				j.RunAt = now
				j.LastError = "lease reconciled on coordinator recovery"
			}
			j.WorkerID = ""
			idx.Put(j)
		}
	}

	// Start single-writer loop
	go e.run()

	return e, nil
}

// Close gracefully stops the engine writer loop and closes the WAL.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.halted {
		e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()

	close(e.stopCh)
	<-e.doneCh

	return e.log.Close()
}

// Index returns the thread-safe index for concurrent readers.
func (e *Engine) Index() *index.Index {
	return e.index
}

// GetJob returns a copy of a job by ID.
func (e *Engine) GetJob(id core.JobID) (*core.Job, bool) {
	return e.index.Get(id)
}

// Stats returns current queue observability statistics.
func (e *Engine) Stats() map[string]index.QueueStats {
	return e.index.Stats(e.cfg.Clock.Now())
}

// LastAppliedLSN returns the most recently durable and applied LSN.
func (e *Engine) LastAppliedLSN() core.LSN {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastAppliedLSN
}

// ---------------- Public Command Methods ----------------

type SubmitArgs struct {
	Job *core.Job
}

func (e *Engine) Submit(job *core.Job) (*core.Job, error) {
	res, err := e.dispatch(command{op: opSubmit, args: job})
	if err != nil {
		return nil, err
	}
	return res.(*core.Job), nil
}

type LeaseArgs struct {
	Queues        []string
	WorkerID      string
	Max           int
	LeaseDuration time.Duration
}

func (e *Engine) Lease(queues []string, workerID string, max int, leaseDuration time.Duration) ([]*core.Job, error) {
	args := LeaseArgs{
		Queues:        queues,
		WorkerID:      workerID,
		Max:           max,
		LeaseDuration: leaseDuration,
	}
	res, err := e.dispatch(command{op: opLease, args: args})
	if err != nil {
		return nil, err
	}
	return res.([]*core.Job), nil
}

type HeartbeatArgs struct {
	JobID         core.JobID
	WorkerID      string
	LeaseToken    uint64
	LeaseDuration time.Duration
}

type HeartbeatResult struct {
	LeaseExpiry time.Time
	Cancelled   bool
}

func (e *Engine) Heartbeat(jobID core.JobID, workerID string, token uint64, duration time.Duration) (HeartbeatResult, error) {
	args := HeartbeatArgs{
		JobID:         jobID,
		WorkerID:      workerID,
		LeaseToken:    token,
		LeaseDuration: duration,
	}
	res, err := e.dispatch(command{op: opHeartbeat, args: args})
	if err != nil {
		return HeartbeatResult{}, err
	}
	return res.(HeartbeatResult), nil
}

type CompleteArgs struct {
	JobID      core.JobID
	WorkerID   string
	LeaseToken uint64
	Success    bool
	Error      string
	Retryable  bool
}

func (e *Engine) Complete(args CompleteArgs) (*core.Job, error) {
	res, err := e.dispatch(command{op: opComplete, args: args})
	if err != nil {
		return nil, err
	}
	return res.(*core.Job), nil
}

func (e *Engine) Cancel(id core.JobID) (*core.Job, error) {
	res, err := e.dispatch(command{op: opCancel, args: id})
	if err != nil {
		return nil, err
	}
	return res.(*core.Job), nil
}

func (e *Engine) ExpireLeases() (int, error) {
	res, err := e.dispatch(command{op: opExpire, args: nil})
	if err != nil {
		return 0, err
	}
	return res.(int), nil
}

func (e *Engine) Snapshot() (core.LSN, error) {
	res, err := e.dispatch(command{op: opSnapshot, args: nil})
	if err != nil {
		return 0, err
	}
	return res.(core.LSN), nil
}

func (e *Engine) dispatch(cmd command) (any, error) {
	e.mu.RLock()
	if e.halted {
		err := e.haltErr
		if err == nil {
			err = ErrEngineHalted
		}
		e.mu.RUnlock()
		return nil, err
	}
	e.mu.RUnlock()

	cmd.reply = make(chan result, 1)
	select {
	case e.cmdChan <- cmd:
	default:
		return nil, ErrQueueFull
	}

	select {
	case res := <-cmd.reply:
		return res.val, res.err
	case <-e.stopCh:
		return nil, ErrEngineHalted
	}
}

// ---------------- Single-Writer Loop ----------------

func (e *Engine) run() {
	defer close(e.doneCh)

	for {
		batch := e.drain()
		if len(batch) == 0 {
			return
		}

		// 1. Validate commands & build WAL records
		recs, validCmds, validationErrs := e.validate(batch)

		// Immediate reply for commands that were rejected during validation (pure, no I/O)
		for idx, err := range validationErrs {
			batch[idx].reply <- result{err: err}
		}

		if len(recs) > 0 {
			// 2. Durably append to WAL
			lsn, err := e.log.Append(recs)
			if err != nil {
				e.failAll(validCmds, err)
				e.halt(err)
				return
			}

			// 3. Apply durable records to in-memory index
			for _, rec := range recs {
				e.applyRecord(rec)
			}

			e.mu.Lock()
			e.lastAppliedLSN = lsn
			e.mutationsCount += len(recs)
			shouldSnap := e.cfg.SnapshotCadence > 0 && e.mutationsCount >= e.cfg.SnapshotCadence
			e.mu.Unlock()

			if shouldSnap {
				_ = e.performSnapshot(lsn)
			}
		}

		// 4. Send successful replies to callers
		for _, cmd := range validCmds {
			e.replyCommand(cmd)
		}
	}
}

func (e *Engine) drain() []command {
	select {
	case <-e.stopCh:
		return nil
	case cmd := <-e.cmdChan:
		batch := []command{cmd}
		for len(batch) < e.cfg.MaxBatchSize {
			select {
			case nextCmd := <-e.cmdChan:
				batch = append(batch, nextCmd)
			default:
				return batch
			}
		}
		return batch
	}
}

func (e *Engine) validate(batch []command) ([]core.Record, []command, map[int]error) {
	var recs []core.Record
	var validCmds []command
	errs := make(map[int]error)
	now := e.cfg.Clock.Now()

	for i, cmd := range batch {
		switch cmd.op {
		case opSubmit:
			j := cmd.args.(*core.Job)
			// Check idempotency key
			if j.IdemKey != "" {
				if _, ok := e.index.GetByIdem(j.IdemKey); ok {
					// Duplicate submission with same idempotency key returns existing job with no error
					validCmds = append(validCmds, cmd)
					continue
				}
			}

			newJob := j.Clone()
			if newJob.ID.IsZero() {
				newJob.ID = core.MustNewJobID()
			}
			if newJob.RunAt.IsZero() {
				newJob.RunAt = now
			}
			newJob.State = core.StatePending
			newJob.CreatedAt = now
			newJob.UpdatedAt = now

			rec := core.Record{
				Type: core.RecSubmit,
				Job:  newJob,
			}
			recs = append(recs, rec)
			cmd.args = newJob
			validCmds = append(validCmds, cmd)

		case opLease:
			args := cmd.args.(LeaseArgs)
			remaining := args.Max
			var leasedIDs []core.JobID
			for _, q := range args.Queues {
				if remaining <= 0 {
					break
				}
				dueJobs := e.index.PopDue(q, now, remaining)
				for _, dj := range dueJobs {
					rec := core.Record{
						Type:        core.RecLease,
						JobID:       dj.ID,
						WorkerID:    args.WorkerID,
						LeaseExpiry: now.Add(args.LeaseDuration),
						LeaseToken:  dj.LeaseToken + 1,
						Attempt:     dj.Attempt + 1,
					}
					recs = append(recs, rec)
					leasedIDs = append(leasedIDs, dj.ID)
					remaining--
				}
			}
			cmd.args = leasedIDs
			validCmds = append(validCmds, cmd)

		case opHeartbeat:
			args := cmd.args.(HeartbeatArgs)
			j, ok := e.index.Get(args.JobID)
			if !ok {
				errs[i] = ErrJobNotFound
				continue
			}
			if j.State != core.StateLeased {
				errs[i] = ErrJobNotLeased
				continue
			}
			if j.LeaseToken != args.LeaseToken {
				errs[i] = ErrStaleToken
				continue
			}
			// Heartbeats mutate memory only (no WAL record created)
			j.LeaseExpiry = now.Add(args.LeaseDuration)
			j.UpdatedAt = now
			e.index.Put(j)
			validCmds = append(validCmds, cmd)

		case opComplete:
			args := cmd.args.(CompleteArgs)
			j, ok := e.index.Get(args.JobID)
			if !ok {
				errs[i] = ErrJobNotFound
				continue
			}
			if j.State != core.StateLeased {
				errs[i] = ErrJobNotLeased
				continue
			}
			if j.LeaseToken != args.LeaseToken {
				errs[i] = ErrStaleToken
				continue
			}

			if args.Success {
				rec := core.Record{
					Type:    core.RecComplete,
					JobID:   j.ID,
					Success: true,
				}
				recs = append(recs, rec)
			} else {
				if args.Retryable && j.Attempt < j.MaxAttempts {
					delay := e.backoff(j.Attempt)
					rec := core.Record{
						Type:  core.RecRetry,
						JobID: j.ID,
						RunAt: now.Add(delay),
						Error: args.Error,
					}
					recs = append(recs, rec)
				} else {
					rec := core.Record{
						Type:  core.RecDead,
						JobID: j.ID,
						Error: args.Error,
					}
					recs = append(recs, rec)
				}
			}
			validCmds = append(validCmds, cmd)

		case opCancel:
			id := cmd.args.(core.JobID)
			j, ok := e.index.Get(id)
			if !ok {
				errs[i] = ErrJobNotFound
				continue
			}
			if j.State.IsTerminal() {
				errs[i] = ErrTerminalJob
				continue
			}
			rec := core.Record{
				Type:  core.RecCancel,
				JobID: id,
			}
			recs = append(recs, rec)
			validCmds = append(validCmds, cmd)

		case opExpire:
			expired := e.index.PopExpiredLeases(now)
			for _, expJob := range expired {
				rec := core.Record{
					Type:       core.RecExpire,
					JobID:      expJob.ID,
					LeaseToken: expJob.LeaseToken,
				}
				recs = append(recs, rec)
			}
			cmd.args = len(expired)
			validCmds = append(validCmds, cmd)

		case opSnapshot:
			validCmds = append(validCmds, cmd)
		}
	}

	return recs, validCmds, errs
}

func (e *Engine) replyCommand(cmd command) {
	switch cmd.op {
	case opSubmit:
		j := cmd.args.(*core.Job)
		if j.IdemKey != "" {
			if existing, ok := e.index.GetByIdem(j.IdemKey); ok {
				cmd.reply <- result{val: existing}
				return
			}
		}
		latest, _ := e.index.Get(j.ID)
		cmd.reply <- result{val: latest}

	case opLease:
		leasedIDs := cmd.args.([]core.JobID)
		var leased []*core.Job
		for _, id := range leasedIDs {
			if j, ok := e.index.Get(id); ok {
				leased = append(leased, j)
			}
		}
		cmd.reply <- result{val: leased}

	case opHeartbeat:
		args := cmd.args.(HeartbeatArgs)
		j, _ := e.index.Get(args.JobID)
		res := HeartbeatResult{
			LeaseExpiry: j.LeaseExpiry,
			Cancelled:   j.CancelReq,
		}
		cmd.reply <- result{val: res}

	case opComplete:
		args := cmd.args.(CompleteArgs)
		j, _ := e.index.Get(args.JobID)
		cmd.reply <- result{val: j}

	case opCancel:
		id := cmd.args.(core.JobID)
		j, _ := e.index.Get(id)
		cmd.reply <- result{val: j}

	case opExpire:
		count := cmd.args.(int)
		cmd.reply <- result{val: count}

	case opSnapshot:
		e.mu.RLock()
		lsn := e.lastAppliedLSN
		e.mu.RUnlock()
		err := e.performSnapshot(lsn)
		cmd.reply <- result{val: lsn, err: err}
	}
}

func (e *Engine) applyRecord(rec core.Record) {
	now := e.cfg.Clock.Now()

	switch rec.Type {
	case core.RecSubmit:
		e.index.Put(rec.Job)
		if e.cfg.OnJobEligible != nil && !rec.Job.RunAt.After(now) {
			e.cfg.OnJobEligible(rec.Job.Queue)
		}

	case core.RecLease:
		if j, ok := e.index.Get(rec.JobID); ok {
			j.State = core.StateLeased
			j.WorkerID = rec.WorkerID
			j.LeaseExpiry = rec.LeaseExpiry
			j.LeaseToken = rec.LeaseToken
			j.Attempt = rec.Attempt
			j.UpdatedAt = now
			e.index.Put(j)
		}

	case core.RecComplete:
		if j, ok := e.index.Get(rec.JobID); ok {
			if j.CancelReq {
				j.State = core.StateCancelled
			} else if rec.Success {
				j.State = core.StateSucceeded
				j.LastError = ""
			} else {
				j.State = core.StateDead
				j.LastError = rec.Error
			}
			j.WorkerID = ""
			j.UpdatedAt = now
			e.index.Put(j)
		}

	case core.RecRetry:
		if j, ok := e.index.Get(rec.JobID); ok {
			if j.CancelReq {
				j.State = core.StateCancelled
			} else {
				j.State = core.StatePending
				j.RunAt = rec.RunAt
				j.LastError = rec.Error
			}
			j.WorkerID = ""
			j.UpdatedAt = now
			e.index.Put(j)
			if e.cfg.OnJobEligible != nil && !j.RunAt.After(now) {
				e.cfg.OnJobEligible(j.Queue)
			}
		}

	case core.RecDead:
		if j, ok := e.index.Get(rec.JobID); ok {
			j.State = core.StateDead
			j.LastError = rec.Error
			j.WorkerID = ""
			j.UpdatedAt = now
			e.index.Put(j)
		}

	case core.RecCancel:
		if j, ok := e.index.Get(rec.JobID); ok {
			if j.State == core.StatePending {
				j.State = core.StateCancelled
			} else if j.State == core.StateLeased {
				j.CancelReq = true
			}
			j.UpdatedAt = now
			e.index.Put(j)
		}

	case core.RecExpire:
		if j, ok := e.index.Get(rec.JobID); ok {
			if j.CancelReq {
				j.State = core.StateCancelled
			} else {
				j.State = core.StatePending
				j.LastError = "lease expired"
			}
			j.WorkerID = ""
			j.UpdatedAt = now
			e.index.Put(j)
			if e.cfg.OnJobEligible != nil {
				e.cfg.OnJobEligible(j.Queue)
			}
		}
	}
}

func (e *Engine) backoff(attempt uint16) time.Duration {
	shift := attempt - 1
	if shift > 16 {
		shift = 16
	}

	base := e.cfg.InitialBackoff * time.Duration(1<<shift)
	if base > e.cfg.MaxBackoff || base <= 0 {
		base = e.cfg.MaxBackoff
	}

	half := base / 2
	if half <= 0 {
		return base
	}

	e.rngMu.Lock()
	jitter := time.Duration(e.rng.Int63n(int64(half)))
	e.rngMu.Unlock()

	return half + jitter
}

func (e *Engine) performSnapshot(lsn core.LSN) error {
	snapDir := filepath.Join(e.cfg.DataDir, "snapshots")
	allJobs := e.index.AllJobs()

	if err := snapshot.Save(snapDir, lsn, allJobs); err != nil {
		return err
	}

	_ = e.log.TruncateBefore(lsn)

	e.mu.Lock()
	e.mutationsCount = 0
	e.mu.Unlock()

	return nil
}

func (e *Engine) failAll(batch []command, err error) {
	for _, cmd := range batch {
		select {
		case cmd.reply <- result{err: err}:
		default:
		}
	}
}

func (e *Engine) halt(err error) {
	e.mu.Lock()
	e.halted = true
	e.haltErr = err
	e.mu.Unlock()
}