package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

var (
	ErrMalformedPayload = errors.New("core: malformed record payload")
	ErrUnknownRecordType = errors.New("core: unknown record type")
)

// Record represents a single state mutation to be durably written to the WAL.
type Record struct {
	Type RecordType
	LSN  LSN
	Flag uint8

	// Fields populated per RecordType:
	Job         *Job      // RecSubmit
	JobID       JobID     // RecLease, RecComplete, RecRetry, RecDead, RecCancel, RecExpire
	WorkerID    string    // RecLease
	LeaseExpiry time.Time // RecLease
	LeaseToken  uint64    // RecLease, RecExpire
	Attempt     uint16    // RecLease
	Success     bool      // RecComplete
	RunAt       time.Time // RecRetry
	Error       string    // RecComplete, RecRetry, RecDead
}

// MarshalPayload serializes the record's payload into a compact binary slice.
func (r *Record) MarshalPayload() ([]byte, error) {
	buf := new(bytes.Buffer)
	var err error

	switch r.Type {
	case RecSubmit:
		if r.Job == nil {
			return nil, errors.New("core: RecSubmit requires non-nil Job")
		}
		j := r.Job
		_, err = buf.Write(j.ID[:])
		if err == nil {
			err = writeString(buf, j.Type)
		}
		if err == nil {
			err = writeString(buf, j.Queue)
		}
		if err == nil {
			err = writeBytes(buf, j.Payload)
		}
		if err == nil {
			err = binary.Write(buf, binary.LittleEndian, j.Priority)
		}
		if err == nil {
			err = binary.Write(buf, binary.LittleEndian, uint8(j.State))
		}
		if err == nil {
			err = binary.Write(buf, binary.LittleEndian, j.Attempt)
		}
		if err == nil {
			err = binary.Write(buf, binary.LittleEndian, j.MaxAttempts)
		}
		if err == nil {
			err = writeTime(buf, j.RunAt)
		}
		if err == nil {
			err = writeTime(buf, j.LeaseExpiry)
		}
		if err == nil {
			err = binary.Write(buf, binary.LittleEndian, j.LeaseToken)
		}
		if err == nil {
			err = writeString(buf, j.WorkerID)
		}
		if err == nil {
			var cancelFlag uint8
			if j.CancelReq {
				cancelFlag = 1
			}
			err = binary.Write(buf, binary.LittleEndian, cancelFlag)
		}
		if err == nil {
			err = writeString(buf, j.LastError)
		}
		if err == nil {
			err = writeString(buf, j.IdemKey)
		}
		if err == nil {
			err = writeTime(buf, j.CreatedAt)
		}
		if err == nil {
			err = writeTime(buf, j.UpdatedAt)
		}

	case RecLease:
		_, err = buf.Write(r.JobID[:])
		if err == nil {
			err = writeString(buf, r.WorkerID)
		}
		if err == nil {
			err = writeTime(buf, r.LeaseExpiry)
		}
		if err == nil {
			err = binary.Write(buf, binary.LittleEndian, r.LeaseToken)
		}
		if err == nil {
			err = binary.Write(buf, binary.LittleEndian, r.Attempt)
		}

	case RecComplete:
		_, err = buf.Write(r.JobID[:])
		if err == nil {
			var succ uint8
			if r.Success {
				succ = 1
			}
			err = binary.Write(buf, binary.LittleEndian, succ)
		}
		if err == nil {
			err = writeString(buf, r.Error)
		}

	case RecRetry:
		_, err = buf.Write(r.JobID[:])
		if err == nil {
			err = writeTime(buf, r.RunAt)
		}
		if err == nil {
			err = writeString(buf, r.Error)
		}

	case RecDead:
		_, err = buf.Write(r.JobID[:])
		if err == nil {
			err = writeString(buf, r.Error)
		}

	case RecCancel:
		_, err = buf.Write(r.JobID[:])

	case RecExpire:
		_, err = buf.Write(r.JobID[:])
		if err == nil {
			err = binary.Write(buf, binary.LittleEndian, r.LeaseToken)
		}

	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownRecordType, r.Type)
	}

	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalPayload populates record fields from a binary payload according to typ.
func (r *Record) UnmarshalPayload(typ RecordType, b []byte) error {
	r.Type = typ
	reader := bytes.NewReader(b)

	switch typ {
	case RecSubmit:
		j := &Job{}
		if _, err := io.ReadFull(reader, j.ID[:]); err != nil {
			return ErrMalformedPayload
		}
		var err error
		if j.Type, err = readString(reader); err != nil {
			return ErrMalformedPayload
		}
		if j.Queue, err = readString(reader); err != nil {
			return ErrMalformedPayload
		}
		if j.Payload, err = readBytes(reader); err != nil {
			return ErrMalformedPayload
		}
		if err = binary.Read(reader, binary.LittleEndian, &j.Priority); err != nil {
			return ErrMalformedPayload
		}
		var st uint8
		if err = binary.Read(reader, binary.LittleEndian, &st); err != nil {
			return ErrMalformedPayload
		}
		j.State = State(st)
		if err = binary.Read(reader, binary.LittleEndian, &j.Attempt); err != nil {
			return ErrMalformedPayload
		}
		if err = binary.Read(reader, binary.LittleEndian, &j.MaxAttempts); err != nil {
			return ErrMalformedPayload
		}
		if j.RunAt, err = readTime(reader); err != nil {
			return ErrMalformedPayload
		}
		if j.LeaseExpiry, err = readTime(reader); err != nil {
			return ErrMalformedPayload
		}
		if err = binary.Read(reader, binary.LittleEndian, &j.LeaseToken); err != nil {
			return ErrMalformedPayload
		}
		if j.WorkerID, err = readString(reader); err != nil {
			return ErrMalformedPayload
		}
		var cancelFlag uint8
		if err = binary.Read(reader, binary.LittleEndian, &cancelFlag); err != nil {
			return ErrMalformedPayload
		}
		j.CancelReq = (cancelFlag == 1)
		if j.LastError, err = readString(reader); err != nil {
			return ErrMalformedPayload
		}
		if j.IdemKey, err = readString(reader); err != nil {
			return ErrMalformedPayload
		}
		if j.CreatedAt, err = readTime(reader); err != nil {
			return ErrMalformedPayload
		}
		if j.UpdatedAt, err = readTime(reader); err != nil {
			return ErrMalformedPayload
		}
		r.Job = j
		r.JobID = j.ID

	case RecLease:
		if _, err := io.ReadFull(reader, r.JobID[:]); err != nil {
			return ErrMalformedPayload
		}
		var err error
		if r.WorkerID, err = readString(reader); err != nil {
			return ErrMalformedPayload
		}
		if r.LeaseExpiry, err = readTime(reader); err != nil {
			return ErrMalformedPayload
		}
		if err = binary.Read(reader, binary.LittleEndian, &r.LeaseToken); err != nil {
			return ErrMalformedPayload
		}
		if err = binary.Read(reader, binary.LittleEndian, &r.Attempt); err != nil {
			return ErrMalformedPayload
		}

	case RecComplete:
		if _, err := io.ReadFull(reader, r.JobID[:]); err != nil {
			return ErrMalformedPayload
		}
		var succ uint8
		if err := binary.Read(reader, binary.LittleEndian, &succ); err != nil {
			return ErrMalformedPayload
		}
		r.Success = (succ == 1)
		var err error
		if r.Error, err = readString(reader); err != nil {
			return ErrMalformedPayload
		}

	case RecRetry:
		if _, err := io.ReadFull(reader, r.JobID[:]); err != nil {
			return ErrMalformedPayload
		}
		var err error
		if r.RunAt, err = readTime(reader); err != nil {
			return ErrMalformedPayload
		}
		if r.Error, err = readString(reader); err != nil {
			return ErrMalformedPayload
		}

	case RecDead:
		if _, err := io.ReadFull(reader, r.JobID[:]); err != nil {
			return ErrMalformedPayload
		}
		var err error
		if r.Error, err = readString(reader); err != nil {
			return ErrMalformedPayload
		}

	case RecCancel:
		if _, err := io.ReadFull(reader, r.JobID[:]); err != nil {
			return ErrMalformedPayload
		}

	case RecExpire:
		if _, err := io.ReadFull(reader, r.JobID[:]); err != nil {
			return ErrMalformedPayload
		}
		// LeaseToken is optional for backwards compatibility
		if reader.Len() > 0 {
			if err := binary.Read(reader, binary.LittleEndian, &r.LeaseToken); err != nil {
				return ErrMalformedPayload
			}
		}

	default:
		return fmt.Errorf("%w: %d", ErrUnknownRecordType, typ)
	}

	return nil
}

func writeString(w io.Writer, s string) error {
	b := []byte(s)
	if err := binary.Write(w, binary.LittleEndian, uint32(len(b))); err != nil {
		return err
	}
	if len(b) > 0 {
		_, err := w.Write(b)
		return err
	}
	return nil
}

func readString(r io.Reader) (string, error) {
	var l uint32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return "", err
	}
	if l == 0 {
		return "", nil
	}
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func writeBytes(w io.Writer, b []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(b))); err != nil {
		return err
	}
	if len(b) > 0 {
		_, err := w.Write(b)
		return err
	}
	return nil
}

func readBytes(r io.Reader) ([]byte, error) {
	var l uint32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return nil, err
	}
	if l == 0 {
		return nil, nil
	}
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

func writeTime(w io.Writer, t time.Time) error {
	var nanos int64
	if !t.IsZero() {
		nanos = t.UTC().UnixNano()
	}
	return binary.Write(w, binary.LittleEndian, nanos)
}

func readTime(r io.Reader) (time.Time, error) {
	var nanos int64
	if err := binary.Read(r, binary.LittleEndian, &nanos); err != nil {
		return time.Time{}, err
	}
	if nanos == 0 {
		return time.Time{}, nil
	}
	return time.Unix(0, nanos).UTC(), nil
}
