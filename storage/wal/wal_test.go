package wal

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/domains18/kombucha/core"
)

func createTestRecords(count int) []core.Record {
	recs := make([]core.Record, count)
	now := time.Now().UTC()
	for i := 0; i < count; i++ {
		id := core.MustNewJobID()
		recs[i] = core.Record{
			Type: core.RecSubmit,
			Job: &core.Job{
				ID:          id,
				Type:        fmt.Sprintf("job.type.%d", i),
				Queue:       "default",
				Payload:     []byte(fmt.Sprintf(`{"index":%d}`, i)),
				Priority:    int8(i % 10),
				State:       core.StatePending,
				MaxAttempts: 3,
				RunAt:       now,
				CreatedAt:   now,
				UpdatedAt:   now,
			},
		}
	}
	return recs
}

func TestWAL_BasicAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir, Options{Sync: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer log.Close()

	recs := createTestRecords(10)
	lastLSN, err := log.Append(recs)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if lastLSN != 10 {
		t.Fatalf("expected lastLSN 10, got %d", lastLSN)
	}

	var replayed []core.Record
	err = log.Replay(1, func(r core.Record) error {
		replayed = append(replayed, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(replayed) != 10 {
		t.Fatalf("expected 10 records, got %d", len(replayed))
	}

	for i, r := range replayed {
		if r.LSN != core.LSN(i+1) {
			t.Errorf("record %d: expected LSN %d, got %d", i, i+1, r.LSN)
		}
		if r.Job.Type != fmt.Sprintf("job.type.%d", i) {
			t.Errorf("record %d: unexpected type %s", i, r.Job.Type)
		}
	}

	// Replay from LSN 6
	var partial []core.Record
	err = log.Replay(6, func(r core.Record) error {
		partial = append(partial, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Partial replay failed: %v", err)
	}
	if len(partial) != 5 {
		t.Fatalf("expected 5 records starting from LSN 6, got %d", len(partial))
	}
	if partial[0].LSN != 6 || partial[4].LSN != 10 {
		t.Fatalf("unexpected partial LSN range: %d..%d", partial[0].LSN, partial[4].LSN)
	}
}

func TestWAL_SegmentRollover(t *testing.T) {
	dir := t.TempDir()
	// Set small SegmentSize (256 bytes) to force frequent rollovers
	opts := Options{
		SegmentSize: 256,
		Sync:        true,
	}

	log, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	totalRecs := 30
	for i := 0; i < totalRecs; i++ {
		r := createTestRecords(1)
		if _, err := log.Append(r); err != nil {
			t.Fatalf("Append failed at %d: %v", i, err)
		}
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify multiple segments were created
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	var segCount int
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			segCount++
		}
	}
	if segCount <= 1 {
		t.Fatalf("expected multiple segments, got %d", segCount)
	}

	// Reopen and ensure all records replay cleanly across segments
	reopened, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer reopened.Close()

	var replayed []core.Record
	err = reopened.Replay(1, func(r core.Record) error {
		replayed = append(replayed, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay failed after reopen: %v", err)
	}

	if len(replayed) != totalRecs {
		t.Fatalf("expected %d replayed records, got %d", totalRecs, len(replayed))
	}
}

func TestWAL_TornTailRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Sync: true}

	log, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	recs := createTestRecords(5)
	_, err = log.Append(recs)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	log.Close()

	segPath := filepath.Join(dir, segmentName(1))
	fi, err := os.Stat(segPath)
	if err != nil {
		t.Fatal(err)
	}
	origSize := fi.Size()

	// Append corrupt/partial garbage bytes to the tail
	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	// Write half of a frame header (e.g. 5 bytes)
	if _, err := f.Write([]byte{0x20, 0x00, 0x00, 0x00, 0xFF}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Reopen should detect the torn tail, truncate it back to origSize, and recover all 5 records
	reopened, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed on torn tail: %v", err)
	}
	defer reopened.Close()

	fiAfter, err := os.Stat(segPath)
	if err != nil {
		t.Fatal(err)
	}
	if fiAfter.Size() != origSize {
		t.Fatalf("expected segment size %d after torn tail truncation, got %d", origSize, fiAfter.Size())
	}

	var count int
	err = reopened.Replay(1, func(r core.Record) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay failed after torn tail repair: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected 5 records, got %d", count)
	}

	// Appending after torn tail recovery should work seamlessly
	nextRec := createTestRecords(1)
	nextLSN, err := reopened.Append(nextRec)
	if err != nil {
		t.Fatalf("Append after torn tail repair failed: %v", err)
	}
	if nextLSN != 6 {
		t.Fatalf("expected nextLSN 6, got %d", nextLSN)
	}
}

func TestWAL_AppendCrashReplayProperty(t *testing.T) {
	// Property test: For any random cut point in the last segment,
	// replay yields exactly the valid prefix of records prior to that cut point.
	dir := t.TempDir()
	opts := Options{Sync: true}

	log, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	totalRecs := 20
	recs := createTestRecords(totalRecs)
	_, err = log.Append(recs)
	if err != nil {
		t.Fatal(err)
	}
	log.Close()

	segPath := filepath.Join(dir, segmentName(1))
	data, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatal(err)
	}

	// We test 100 random cut points across the segment length
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 100; i++ {
		testDir := t.TempDir()
		testSegPath := filepath.Join(testDir, segmentName(1))

		cutPoint := rng.Intn(len(data) - headerSize) + headerSize
		cutData := data[:cutPoint]

		if err := os.WriteFile(testSegPath, cutData, 0644); err != nil {
			t.Fatal(err)
		}

		recoveredLog, err := Open(testDir, opts)
		if err != nil {
			t.Fatalf("iteration %d: Open failed on truncated segment at %d: %v", i, cutPoint, err)
		}

		var recovered []core.Record
		err = recoveredLog.Replay(1, func(r core.Record) error {
			recovered = append(recovered, r)
			return nil
		})
		if err != nil {
			t.Fatalf("iteration %d: Replay failed on truncated segment at %d: %v", i, cutPoint, err)
		}

		// Ensure every recovered record is an exact prefix of the original records
		for idx, r := range recovered {
			if r.LSN != recs[idx].LSN {
				t.Fatalf("iteration %d: LSN mismatch at %d: got %d, want %d", i, idx, r.LSN, recs[idx].LSN)
			}
			if r.Job.Type != recs[idx].Job.Type {
				t.Fatalf("iteration %d: Job Type mismatch at %d", i, idx)
			}
		}

		recoveredLog.Close()
	}
}

func TestWAL_EarlierSegmentCorruption(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		SegmentSize: 200, // force rollover
		Sync:        true,
	}

	log, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Write enough records to generate at least 2 segments
	for i := 0; i < 15; i++ {
		_, err := log.Append(createTestRecords(1))
		if err != nil {
			t.Fatal(err)
		}
	}
	log.Close()

	entries, _ := os.ReadDir(dir)
	var segFiles []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			segFiles = append(segFiles, filepath.Join(dir, e.Name()))
		}
	}
	if len(segFiles) < 2 {
		t.Fatalf("expected at least 2 segments, got %d", len(segFiles))
	}

	// Corrupt a byte in the first (sealed) segment
	firstSeg := segFiles[0]
	segData, err := os.ReadFile(firstSeg)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt a byte in the middle of a frame in the sealed segment
	segData[headerSize+10] ^= 0xFF
	if err := os.WriteFile(firstSeg, segData, 0644); err != nil {
		t.Fatal(err)
	}

	// Open should fail with ErrCorruptedLog because corruption in a sealed segment is fatal
	_, err = Open(dir, opts)
	if err == nil {
		t.Fatal("expected ErrCorruptedLog for corrupted sealed segment, got nil")
	}
}

func TestWAL_TruncateBefore(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		SegmentSize: 200, // force rollover
		Sync:        true,
	}

	log, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 15; i++ {
		_, err := log.Append(createTestRecords(1))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Truncate before LSN 8
	if err := log.TruncateBefore(8); err != nil {
		t.Fatalf("TruncateBefore failed: %v", err)
	}

	// Replay from 8 should succeed
	var count int
	err = log.Replay(8, func(r core.Record) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay after truncation failed: %v", err)
	}
	if count != 8 { // LSN 8 through 15 = 8 records
		t.Fatalf("expected 8 records, got %d", count)
	}

	log.Close()
}

func BenchmarkWAL_BatchedAppend(b *testing.B) {
	dir := b.TempDir()
	// Disable fsync for raw log encoding & throughput benchmarking
	opts := Options{
		SegmentSize: 64 << 20,
		Sync:        false,
	}

	log, err := Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer log.Close()

	batchSize := 100
	recs := createTestRecords(batchSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := log.Append(recs); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "records/sec")
}
