package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/domains18/kombucha/core"
)

const (
	snapMagic       = "KMBS" // 4 bytes
	snapEndMagic    = "KMSE" // 4 bytes
	snapVersion     = uint32(1)
	snapHeaderSize  = 12 // 4 magic + 4 version + 4 count
	snapFooterSize  = 16 // 8 lsn + 4 crc32c + 4 endMagic
)

var (
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
	ErrCorruptSnapshot = errors.New("snapshot: corrupted snapshot file")
)

// Save atomically writes a snapshot of all active jobs at snapshotLSN.
func Save(dir string, snapshotLSN core.LSN, jobs []*core.Job) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("snapshot: cannot create dir %s: %w", dir, err)
	}

	tmpName := fmt.Sprintf("snapshot.%020d.tmp", snapshotLSN)
	finalName := fmt.Sprintf("snapshot.%020d.snap", snapshotLSN)
	tmpPath := filepath.Join(dir, tmpName)
	finalPath := filepath.Join(dir, finalName)

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("snapshot: cannot create tmp file: %w", err)
	}
	defer func() {
		f.Close()
		_ = os.Remove(tmpPath) // cleaned up if rename didn't happen
	}()

	hasher := crc32.New(castagnoliTable)
	w := io.MultiWriter(f, hasher)

	// 1. Header: magic (4), version (4), count (4)
	var header [snapHeaderSize]byte
	copy(header[0:4], snapMagic)
	binary.LittleEndian.PutUint32(header[4:8], snapVersion)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(jobs)))

	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("snapshot: header write error: %w", err)
	}

	// 2. Records
	for _, j := range jobs {
		rec := core.Record{
			Type: core.RecSubmit,
			Job:  j,
		}
		payload, err := rec.MarshalPayload()
		if err != nil {
			return fmt.Errorf("snapshot: marshal job %s error: %w", j.ID, err)
		}

		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
		if _, err := w.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}

	// 3. Footer: lsn (8), crc32c (4), endMagic (4)
	// Write LSN to writer (covered by CRC)
	var lsnBuf [8]byte
	binary.LittleEndian.PutUint64(lsnBuf[:], uint64(snapshotLSN))
	if _, err := w.Write(lsnBuf[:]); err != nil {
		return err
	}

	calculatedCRC := hasher.Sum32()

	var footerTail [8]byte
	binary.LittleEndian.PutUint32(footerTail[0:4], calculatedCRC)
	copy(footerTail[4:8], snapEndMagic)

	// Write CRC and endMagic directly to file (not covered by CRC)
	if _, err := f.Write(footerTail[:]); err != nil {
		return err
	}

	// Fsync file before rename
	if err := f.Sync(); err != nil {
		return fmt.Errorf("snapshot: file sync error: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}

	// Atomic rename
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("snapshot: rename error: %w", err)
	}

	// Fsync directory
	if dirFile, err := os.Open(dir); err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}

	// Prune older snapshots, keeping the 2 most recent
	_ = pruneOldSnapshots(dir, 2)

	return nil
}

// LoadNewest loads the newest valid snapshot from dir.
// If no valid snapshots exist, returns 0, nil, nil.
func LoadNewest(dir string) (core.LSN, []*core.Job, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return 0, nil, nil
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, nil, err
	}

	var snapFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "snapshot.") && strings.HasSuffix(e.Name(), ".snap") {
			snapFiles = append(snapFiles, e.Name())
		}
	}

	// Sort descending
	sort.Sort(sort.Reverse(sort.StringSlice(snapFiles)))

	for _, fname := range snapFiles {
		path := filepath.Join(dir, fname)
		lsn, jobs, err := loadSnapshotFile(path)
		if err == nil {
			return lsn, jobs, nil
		}
		// If corrupted, fallback to older snapshot
	}

	return 0, nil, nil
}

func loadSnapshotFile(path string) (core.LSN, []*core.Job, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, nil, err
	}

	totalLen := len(data)
	if totalLen < snapHeaderSize+snapFooterSize {
		return 0, nil, ErrCorruptSnapshot
	}

	// Check header
	if string(data[0:4]) != snapMagic {
		return 0, nil, ErrCorruptSnapshot
	}
	version := binary.LittleEndian.Uint32(data[4:8])
	if version != snapVersion {
		return 0, nil, ErrCorruptSnapshot
	}
	count := binary.LittleEndian.Uint32(data[8:12])

	// Check footer endMagic
	if string(data[totalLen-4:totalLen]) != snapEndMagic {
		return 0, nil, ErrCorruptSnapshot
	}

	// Verify CRC
	expectedCRC := binary.LittleEndian.Uint32(data[totalLen-8 : totalLen-4])
	actualCRC := crc32.Checksum(data[:totalLen-8], castagnoliTable)
	if actualCRC != expectedCRC {
		return 0, nil, fmt.Errorf("%w: crc mismatch", ErrCorruptSnapshot)
	}

	snapshotLSN := core.LSN(binary.LittleEndian.Uint64(data[totalLen-16 : totalLen-8]))

	// Read jobs
	reader := bytes.NewReader(data[snapHeaderSize : totalLen-16])
	jobs := make([]*core.Job, 0, count)

	for i := uint32(0); i < count; i++ {
		var l uint32
		if err := binary.Read(reader, binary.LittleEndian, &l); err != nil {
			return 0, nil, ErrCorruptSnapshot
		}

		payload := make([]byte, l)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return 0, nil, ErrCorruptSnapshot
		}

		var rec core.Record
		if err := rec.UnmarshalPayload(core.RecSubmit, payload); err != nil {
			return 0, nil, err
		}
		if rec.Job == nil {
			return 0, nil, ErrCorruptSnapshot
		}
		jobs = append(jobs, rec.Job)
	}

	return snapshotLSN, jobs, nil
}

func pruneOldSnapshots(dir string, keep int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	var snapFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "snapshot.") && strings.HasSuffix(e.Name(), ".snap") {
			snapFiles = append(snapFiles, e.Name())
		}
	}

	if len(snapFiles) <= keep {
		return nil
	}

	sort.Sort(sort.Reverse(sort.StringSlice(snapFiles)))
	for _, fname := range snapFiles[keep:] {
		_ = os.Remove(filepath.Join(dir, fname))
	}
	return nil
}

func parseSnapshotLSN(fname string) (core.LSN, bool) {
	if !strings.HasPrefix(fname, "snapshot.") || !strings.HasSuffix(fname, ".snap") {
		return 0, false
	}
	trimmed := strings.TrimPrefix(fname, "snapshot.")
	trimmed = strings.TrimSuffix(trimmed, ".snap")
	val, err := strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		return 0, false
	}
	return core.LSN(val), true
}
