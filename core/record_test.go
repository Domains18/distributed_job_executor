package core

import (
	"bytes"
	"testing"
	"time"
)

func TestRecord_RoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	id := MustNewJobID()

	tests := []struct {
		name string
		rec  Record
	}{
		{
			name: "RecSubmit",
			rec: Record{
				Type: RecSubmit,
				LSN:  10,
				Job: &Job{
					ID:          id,
					Type:        "order.confirmation_email",
					Queue:       "email",
					Payload:     []byte(`{"order_id":1234,"customer":"alice"}`),
					Priority:    5,
					State:       StatePending,
					Attempt:     0,
					MaxAttempts: 3,
					RunAt:       now,
					LeaseExpiry: time.Time{},
					LeaseToken:  0,
					WorkerID:    "",
					CancelReq:   false,
					LastError:   "",
					IdemKey:     "order-1234-confirm",
					CreatedAt:   now,
					UpdatedAt:   now,
				},
			},
		},
		{
			name: "RecLease",
			rec: Record{
				Type:        RecLease,
				LSN:         11,
				JobID:       id,
				WorkerID:    "worker-node-1",
				LeaseExpiry: now.Add(30 * time.Second),
				LeaseToken:  42,
				Attempt:     1,
			},
		},
		{
			name: "RecComplete_Success",
			rec: Record{
				Type:    RecComplete,
				LSN:     12,
				JobID:   id,
				Success: true,
				Error:   "",
			},
		},
		{
			name: "RecComplete_Failure",
			rec: Record{
				Type:    RecComplete,
				LSN:     13,
				JobID:   id,
				Success: false,
				Error:   "smtp: connection reset by peer",
			},
		},
		{
			name: "RecRetry",
			rec: Record{
				Type:  RecRetry,
				LSN:   14,
				JobID: id,
				RunAt: now.Add(5 * time.Second),
				Error: "transient network failure",
			},
		},
		{
			name: "RecDead",
			rec: Record{
				Type:  RecDead,
				LSN:   15,
				JobID: id,
				Error: "max attempts reached",
			},
		},
		{
			name: "RecCancel",
			rec: Record{
				Type:  RecCancel,
				LSN:   16,
				JobID: id,
			},
		},
		{
			name: "RecExpire",
			rec: Record{
				Type:       RecExpire,
				LSN:        17,
				JobID:      id,
				LeaseToken: 42,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := tc.rec.MarshalPayload()
			if err != nil {
				t.Fatalf("marshal error: %v", err)
			}

			var decoded Record
			decoded.LSN = tc.rec.LSN
			decoded.Flag = tc.rec.Flag
			if err := decoded.UnmarshalPayload(tc.rec.Type, payload); err != nil {
				t.Fatalf("unmarshal error: %v", err)
			}

			if decoded.Type != tc.rec.Type {
				t.Fatalf("type mismatch: got %v, want %v", decoded.Type, tc.rec.Type)
			}

			switch tc.rec.Type {
			case RecSubmit:
				orig := tc.rec.Job
				got := decoded.Job
				if got.ID != orig.ID || got.Type != orig.Type || got.Queue != orig.Queue ||
					!bytes.Equal(got.Payload, orig.Payload) || got.Priority != orig.Priority ||
					got.State != orig.State || got.Attempt != orig.Attempt || got.MaxAttempts != orig.MaxAttempts ||
					!got.RunAt.Equal(orig.RunAt) || got.IdemKey != orig.IdemKey ||
					!got.CreatedAt.Equal(orig.CreatedAt) || !got.UpdatedAt.Equal(orig.UpdatedAt) {
					t.Fatalf("submit job mismatch:\ngot: %+v\nwant: %+v", got, orig)
				}
			case RecLease:
				if decoded.JobID != tc.rec.JobID || decoded.WorkerID != tc.rec.WorkerID ||
					!decoded.LeaseExpiry.Equal(tc.rec.LeaseExpiry) || decoded.LeaseToken != tc.rec.LeaseToken ||
					decoded.Attempt != tc.rec.Attempt {
					t.Fatalf("lease mismatch: got %+v, want %+v", decoded, tc.rec)
				}
			case RecComplete:
				if decoded.JobID != tc.rec.JobID || decoded.Success != tc.rec.Success || decoded.Error != tc.rec.Error {
					t.Fatalf("complete mismatch: got %+v, want %+v", decoded, tc.rec)
				}
			case RecRetry:
				if decoded.JobID != tc.rec.JobID || !decoded.RunAt.Equal(tc.rec.RunAt) || decoded.Error != tc.rec.Error {
					t.Fatalf("retry mismatch: got %+v, want %+v", decoded, tc.rec)
				}
			case RecDead:
				if decoded.JobID != tc.rec.JobID || decoded.Error != tc.rec.Error {
					t.Fatalf("dead mismatch: got %+v, want %+v", decoded, tc.rec)
				}
			case RecCancel:
				if decoded.JobID != tc.rec.JobID {
					t.Fatalf("cancel mismatch: got %v, want %v", decoded.JobID, tc.rec.JobID)
				}
			case RecExpire:
				if decoded.JobID != tc.rec.JobID || decoded.LeaseToken != tc.rec.LeaseToken {
					t.Fatalf("expire mismatch: got %+v, want %+v", decoded, tc.rec)
				}
			}
		})
	}
}

func TestRecord_CorruptedPayload(t *testing.T) {
	var rec Record
	// Short payload
	if err := rec.UnmarshalPayload(RecSubmit, []byte{1, 2, 3}); err == nil {
		t.Error("expected error for truncated payload, got nil")
	}

	if err := rec.UnmarshalPayload(RecLease, []byte{1, 2, 3}); err == nil {
		t.Error("expected error for truncated payload, got nil")
	}
}
