package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/domains18/kombucha/core"
)

const (
	headerMagic   = "KMBW"
	headerVersion = uint32(1)
	headerSize    = 16 // 4 magic + 4 version + 8 reserved

	// Frame on disk:
	// len u32 (4) | crc32c u32 (4) | lsn u64 (8) | type u8 (1) | flag u8 (1) | payload (len - 14)
	frameHeaderWithoutLen = 14 // crc32c (4) + lsn (8) + type (1) + flag (1)
	minFrameLen           = frameHeaderWithoutLen
)

var (
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

	ErrCorruptedLog    = errors.New("wal: corrupted log frame")
	ErrInvalidHeader   = errors.New("wal: invalid segment header")
	ErrUnexpectedEOF   = errors.New("wal: unexpected end of file")
)

type frame struct {
	len     uint32
	crc     uint32
	lsn     core.LSN
	typ     core.RecordType
	flag    uint8
	payload []byte
}

// encodeFrame serializes a record into disk frame bytes.
func encodeFrame(rec core.Record, payload []byte) ([]byte, error) {
	framePayloadLen := uint32(len(payload))
	lenField := minFrameLen + framePayloadLen

	totalLen := 4 + lenField
	buf := make([]byte, totalLen)

	// [0:4] lenField
	binary.LittleEndian.PutUint32(buf[0:4], lenField)

	// [8:16] lsn
	binary.LittleEndian.PutUint64(buf[8:16], uint64(rec.LSN))
	// [16:17] type
	buf[16] = byte(rec.Type)
	// [17:18] flag
	buf[17] = rec.Flag
	// [18:] payload
	if len(payload) > 0 {
		copy(buf[18:], payload)
	}

	// crc32c covers lsn..payload (bytes from index 8 onwards)
	crc := crc32.Checksum(buf[8:totalLen], castagnoliTable)
	// [4:8] crc32c
	binary.LittleEndian.PutUint32(buf[4:8], crc)

	return buf, nil
}

// decodeFrame reads one frame from r.
// Returns (frame, bytesRead, error).
// If r is at EOF, returns (frame{}, 0, io.EOF).
// If an unexpected EOF or CRC mismatch occurs, returns the specific error.
func decodeFrame(r io.Reader) (frame, int, error) {
	var lenBuf [4]byte
	n, err := io.ReadFull(r, lenBuf[:])
	if err != nil {
		if errors.Is(err, io.EOF) {
			return frame{}, 0, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return frame{}, n, ErrUnexpectedEOF
		}
		return frame{}, n, err
	}

	lenField := binary.LittleEndian.Uint32(lenBuf[:])
	if lenField < minFrameLen {
		return frame{}, 4, fmt.Errorf("%w: invalid frame length %d", ErrCorruptedLog, lenField)
	}

	// Read remainder of frame: lenField bytes
	frameData := make([]byte, lenField)
	nRest, err := io.ReadFull(r, frameData)
	bytesRead := 4 + nRest
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return frame{}, bytesRead, ErrUnexpectedEOF
		}
		return frame{}, bytesRead, err
	}

	expectedCRC := binary.LittleEndian.Uint32(frameData[0:4])
	// CRC covers data starting from index 4 in frameData (which is lsn..payload)
	actualCRC := crc32.Checksum(frameData[4:], castagnoliTable)
	if actualCRC != expectedCRC {
		return frame{}, bytesRead, fmt.Errorf("%w: crc mismatch (expected %08x, got %08x)", ErrCorruptedLog, expectedCRC, actualCRC)
	}

	lsn := core.LSN(binary.LittleEndian.Uint64(frameData[4:12]))
	typ := core.RecordType(frameData[12])
	flg := frameData[13]
	payload := frameData[14:]

	return frame{
		len:     lenField,
		crc:     expectedCRC,
		lsn:     lsn,
		typ:     typ,
		flag:    flg,
		payload: payload,
	}, bytesRead, nil
}
