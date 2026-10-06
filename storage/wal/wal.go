package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/domains18/kombucha/core"
)

var (
	ErrClosed = errors.New("wal: log is closed")
)

// Options configures the write-ahead log.
type Options struct {
	// SegmentSize is the target size at which a new segment is started.
	// Segments are the unit of deletion. Defaults to 64MB.
	SegmentSize int64
	// Sync controls whether every batch is fsynced to disk before returning.
	// Defaults to true.
	Sync bool
}

func (o *Options) defaults() {
	if o.SegmentSize <= 0 {
		o.SegmentSize = 64 << 20 // 64 MB
	}
}

// Log implements a segmented, checksummed write-ahead log with Castagnoli CRC32C.
type Log struct {
	dir     string
	opts    Options
	mu      sync.Mutex
	segs    []*segment
	nextLSN core.LSN
	buf     []byte
	closed  bool

	// err is sticky. If a write fails partway through a batch the file may
	// contain a partial frame, so the in-memory notion of where the log ends
	// can no longer be trusted. Rather than guess, we refuse all further
	// appends and let the next Open repair the tail.
	err error
}

// Open opens an existing log in dir or initializes a new one.
func Open(dir string, opts Options) (*Log, error) {
	opts.defaults()

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: cannot create dir %s: %w", dir, err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: cannot read dir %s: %w", dir, err)
	}

	var segFiles []string
	for _, e := range entries {
		if !e.IsDir() && parseSegmentNameValid(e.Name()) {
			segFiles = append(segFiles, e.Name())
		}
	}
	sort.Strings(segFiles)

	l := &Log{
		dir:     dir,
		opts:    opts,
		nextLSN: 1,
	}

	if len(segFiles) == 0 {
		// Initialize brand new log with segment 1
		seg, err := createSegmentFile(dir, 1)
		if err != nil {
			return nil, err
		}
		l.segs = []*segment{seg}
		l.nextLSN = 1
		return l, nil
	}

	// Scan existing segments
	var highestLSN core.LSN
	for i, fname := range segFiles {
		isLast := (i == len(segFiles)-1)
		path := filepath.Join(dir, fname)
		baseLSN, ok := parseSegmentLSN(fname)
		if !ok {
			return nil, fmt.Errorf("wal: invalid segment file name %s", fname)
		}

		firstLSN, lastLSN, validBytes, err := scanSegment(path, isLast)
		if err != nil {
			return nil, fmt.Errorf("wal: segment scan error in %s: %w", fname, err)
		}

		if firstLSN > 0 && baseLSN != firstLSN && i > 0 {
			// baseLSN from filename should match first record LSN if non-empty
		}

		if lastLSN > highestLSN {
			highestLSN = lastLSN
		}

		seg := &segment{
			path:    path,
			baseLSN: baseLSN,
			lastLSN: lastLSN,
			size:    validBytes,
		}

		if isLast {
			f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
			if err != nil {
				return nil, fmt.Errorf("wal: cannot open active segment %s: %w", path, err)
			}
			seg.file = f
		}

		l.segs = append(l.segs, seg)
	}

	if highestLSN > 0 {
		l.nextLSN = highestLSN + 1
	} else if len(l.segs) > 0 {
		l.nextLSN = l.segs[0].baseLSN
		if l.nextLSN == 0 {
			l.nextLSN = 1
		}
	}

	return l, nil
}

func parseSegmentNameValid(name string) bool {
	_, ok := parseSegmentLSN(name)
	return ok
}

// NextLSN returns the next LSN that will be assigned.
func (l *Log) NextLSN() core.LSN {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextLSN
}

// Append writes a batch of records durably to the WAL.
// Returns the LSN of the last record in the batch.
func (l *Log) Append(recs []core.Record) (core.LSN, error) {
	if len(recs) == 0 {
		l.mu.Lock()
		cur := l.nextLSN - 1
		l.mu.Unlock()
		return cur, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return 0, ErrClosed
	}
	if l.err != nil {
		return 0, fmt.Errorf("wal: sticky error: %w", l.err)
	}

	active := l.segs[len(l.segs)-1]

	// Reset write buffer
	l.buf = l.buf[:0]

	// Pre-encode frames into buffer and assign LSNs
	firstBatchLSN := l.nextLSN
	for i := range recs {
		recs[i].LSN = l.nextLSN
		l.nextLSN++

		payload, err := recs[i].MarshalPayload()
		if err != nil {
			// Roll back assigned LSNs on marshaling error
			l.nextLSN = firstBatchLSN
			return 0, fmt.Errorf("wal: marshal payload error: %w", err)
		}

		frameBytes, err := encodeFrame(recs[i], payload)
		if err != nil {
			l.nextLSN = firstBatchLSN
			return 0, fmt.Errorf("wal: encode frame error: %w", err)
		}

		l.buf = append(l.buf, frameBytes...)
	}

	// Check if active segment should roll over before writing
	if active.size > headerSize && (active.size+int64(len(l.buf))) > l.opts.SegmentSize {
		// Close active segment
		if l.opts.Sync {
			if err := active.file.Sync(); err != nil {
				l.err = err
				return 0, err
			}
		}
		if err := active.file.Close(); err != nil {
			l.err = err
			return 0, err
		}
		active.file = nil

		// Create new segment
		newSeg, err := createSegmentFile(l.dir, firstBatchLSN)
		if err != nil {
			l.err = err
			return 0, err
		}
		l.segs = append(l.segs, newSeg)
		active = newSeg
	}

	// Single Write
	n, err := active.file.Write(l.buf)
	if err != nil {
		l.err = err
		return 0, fmt.Errorf("wal: write failed: %w", err)
	}

	active.size += int64(n)
	lastLSN := recs[len(recs)-1].LSN
	active.lastLSN = lastLSN

	// Single Sync
	if l.opts.Sync {
		if err := active.file.Sync(); err != nil {
			l.err = err
			return 0, fmt.Errorf("wal: fsync failed: %w", err)
		}
	}

	return lastLSN, nil
}

// Replay reads records starting from LSN 'from' in sequential order.
// Calls fn for each record. Stops if fn returns an error.
func (l *Log) Replay(from core.LSN, fn func(core.Record) error) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ErrClosed
	}
	// Copy segment descriptors so we don't hold lock during iteration
	segsCopy := make([]*segment, len(l.segs))
	copy(segsCopy, l.segs)
	l.mu.Unlock()

	for _, seg := range segsCopy {
		// Skip segment if its last LSN is strictly less than 'from'
		if seg.lastLSN > 0 && seg.lastLSN < from {
			continue
		}

		if err := replaySegment(seg.path, from, fn); err != nil {
			return err
		}
	}

	return nil
}

func replaySegment(path string, from core.LSN, fn func(core.Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("wal: cannot open segment for replay %s: %w", path, err)
	}
	defer f.Close()

	if err := readSegmentHeader(f); err != nil {
		return err
	}

	for {
		frm, _, err := decodeFrame(f)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		if frm.lsn < from {
			continue
		}

		var rec core.Record
		rec.LSN = frm.lsn
		rec.Flag = frm.flag
		if err := rec.UnmarshalPayload(frm.typ, frm.payload); err != nil {
			return fmt.Errorf("wal: unmarshal record %d error: %w", frm.lsn, err)
		}

		if err := fn(rec); err != nil {
			return err
		}
	}

	return nil
}

// TruncateBefore deletes whole segments whose last LSN is strictly below watermark.
// It never deletes the active segment.
func (l *Log) TruncateBefore(watermark core.LSN) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrClosed
	}

	if len(l.segs) <= 1 {
		return nil
	}

	active := l.segs[len(l.segs)-1]
	var remaining []*segment
	var deletedAny bool

	for _, seg := range l.segs {
		if seg == active {
			remaining = append(remaining, seg)
			continue
		}

		if seg.lastLSN > 0 && seg.lastLSN < watermark {
			if seg.file != nil {
				_ = seg.file.Close()
				seg.file = nil
			}
			if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("wal: failed to delete segment %s: %w", seg.path, err)
			}
			deletedAny = true
		} else {
			remaining = append(remaining, seg)
		}
	}

	l.segs = remaining

	if deletedAny {
		// Fsync directory to ensure file deletions are durable on disk
		if dirFile, err := os.Open(l.dir); err == nil {
			_ = dirFile.Sync()
			_ = dirFile.Close()
		}
	}

	return nil
}

// Close closes the write-ahead log cleanly.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	l.closed = true

	active := l.segs[len(l.segs)-1]
	if active != nil && active.file != nil {
		if l.opts.Sync {
			_ = active.file.Sync()
		}
		err := active.file.Close()
		active.file = nil
		return err
	}

	return nil
}