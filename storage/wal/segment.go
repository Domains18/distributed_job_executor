package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/domains18/kombucha/core"
)

type segment struct {
	path    string
	baseLSN core.LSN
	lastLSN core.LSN
	size    int64
	file    *os.File
}

func segmentName(baseLSN core.LSN) string {
	return fmt.Sprintf("%020d.wal", baseLSN)
}

func parseSegmentLSN(name string) (core.LSN, bool) {
	if !strings.HasSuffix(name, ".wal") || len(name) != 24 {
		return 0, false
	}
	numStr := strings.TrimSuffix(name, ".wal")
	val, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, false
	}
	return core.LSN(val), true
}

// createSegmentFile creates a new segment file with the 16-byte header written and synced.
func createSegmentFile(dir string, baseLSN core.LSN) (*segment, error) {
	filename := segmentName(baseLSN)
	path := filepath.Join(dir, filename)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to create segment %s: %w", path, err)
	}

	header := make([]byte, headerSize)
	copy(header[0:4], headerMagic)
	binary.LittleEndian.PutUint32(header[4:8], headerVersion)
	binary.LittleEndian.PutUint64(header[8:16], 0) // reserved

	if _, err := f.Write(header); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: failed to write header: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: failed to sync new segment: %w", err)
	}

	// fsync the directory after creating file
	if dirFile, err := os.Open(dir); err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}

	return &segment{
		path:    path,
		baseLSN: baseLSN,
		lastLSN: 0,
		size:    headerSize,
		file:    f,
	}, nil
}

// readSegmentHeader reads and validates the 16-byte header from r.
func readSegmentHeader(r io.Reader) error {
	var header [headerSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return fmt.Errorf("%w: cannot read header: %v", ErrInvalidHeader, err)
	}

	if string(header[0:4]) != headerMagic {
		return fmt.Errorf("%w: invalid magic %q", ErrInvalidHeader, header[0:4])
	}

	version := binary.LittleEndian.Uint32(header[4:8])
	if version != headerVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrInvalidHeader, version)
	}

	return nil
}

// scanSegment scans a segment from disk, recording the records and handling torn tails.
// If isLast is true, any torn frame or bad CRC at the tail causes the file to be truncated at that offset.
// If isLast is false, any read error or CRC mismatch is treated as hard corruption.
func scanSegment(path string, isLast bool) (baseLSN core.LSN, lastLSN core.LSN, validBytes int64, err error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return 0, 0, 0, err
	}
	defer f.Close()

	if err := readSegmentHeader(f); err != nil {
		return 0, 0, 0, err
	}

	offset := int64(headerSize)
	var firstLSN core.LSN
	var highestLSN core.LSN

	for {
		frameStart := offset
		frm, bytesRead, err := decodeFrame(f)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Clean EOF
				break
			}

			// Partial or corrupted frame
			if isLast {
				// Repair torn tail by truncating
				if errTrunc := f.Truncate(frameStart); errTrunc != nil {
					return 0, 0, 0, fmt.Errorf("wal: failed to truncate torn tail at %d: %w", frameStart, errTrunc)
				}
				_ = f.Sync()
				offset = frameStart
				break
			}

			// In earlier segments, this is fatal corruption
			return 0, 0, 0, fmt.Errorf("%w: error at offset %d in sealed segment %s: %v", ErrCorruptedLog, frameStart, path, err)
		}

		offset += int64(bytesRead)
		if firstLSN == 0 {
			firstLSN = frm.lsn
		}
		highestLSN = frm.lsn
	}

	return firstLSN, highestLSN, offset, nil
}
