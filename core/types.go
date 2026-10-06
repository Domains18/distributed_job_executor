package core

import (
	"encoding/json"
	"fmt"
	"time"
)

// LSN represents a monotonic Log Sequence Number in the Write-Ahead Log.
type LSN uint64

// State represents the current lifecycle state of a Job.
type State uint8

const (
	StatePending   State = iota + 1 // eligible at RunAt
	StateLeased                     // held by a worker until LeaseExpiry
	StateSucceeded                  // terminal
	StateDead                       // attempts exhausted; terminal until requeued
	StateCancelled                  // terminal
)

func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateLeased:
		return "leased"
	case StateSucceeded:
		return "succeeded"
	case StateDead:
		return "dead"
	case StateCancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("state(%d)", s)
	}
}

func (s State) IsTerminal() bool {
	return s == StateSucceeded || s == StateDead || s == StateCancelled
}

func (s State) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s *State) UnmarshalJSON(b []byte) error {
	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return err
	}
	switch str {
	case "pending":
		*s = StatePending
	case "leased":
		*s = StateLeased
	case "succeeded":
		*s = StateSucceeded
	case "dead":
		*s = StateDead
	case "cancelled":
		*s = StateCancelled
	default:
		return fmt.Errorf("core: unknown state: %q", str)
	}
	return nil
}

// Job represents a single execution unit managed by the engine.
type Job struct {
	ID          JobID     `json:"id"`
	Type        string    `json:"type"`
	Queue       string    `json:"queue"`
	Payload     []byte    `json:"payload"`
	Priority    int8      `json:"priority"`
	State       State     `json:"state"`
	Attempt     uint16    `json:"attempt"`
	MaxAttempts uint16    `json:"max_attempts"`
	RunAt       time.Time `json:"run_at"`
	LeaseExpiry time.Time `json:"lease_expiry,omitempty"`
	LeaseToken  uint64    `json:"lease_token,omitempty"`
	WorkerID    string    `json:"worker_id,omitempty"`
	CancelReq   bool      `json:"cancel_req,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
	IdemKey     string    `json:"idempotency_key,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Clone returns a deep copy of the job.
func (j *Job) Clone() *Job {
	if j == nil {
		return nil
	}
	cp := *j
	if j.Payload != nil {
		cp.Payload = make([]byte, len(j.Payload))
		copy(cp.Payload, j.Payload)
	}
	return &cp
}

// RecordType defines the type of mutation recorded in the WAL.
type RecordType uint8

const (
	RecSubmit   RecordType = iota + 1 // full job
	RecLease                          // id, worker, expiry, token, attempt
	RecComplete                       // id, success, error
	RecRetry                          // id, next RunAt, error
	RecDead                           // id, error
	RecCancel                         // id
	RecExpire                         // id (lease expired, back to pending)
)

func (rt RecordType) String() string {
	switch rt {
	case RecSubmit:
		return "submit"
	case RecLease:
		return "lease"
	case RecComplete:
		return "complete"
	case RecRetry:
		return "retry"
	case RecDead:
		return "dead"
	case RecCancel:
		return "cancel"
	case RecExpire:
		return "expire"
	default:
		return fmt.Sprintf("record_type(%d)", rt)
	}
}
