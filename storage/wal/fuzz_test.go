package wal

import (
	"bytes"
	"testing"
)

func FuzzDecodeFrame(f *testing.F) {
	// Seed with valid frame
	rec := createTestRecords(1)[0]
	rec.LSN = 1
	payload, err := rec.MarshalPayload()
	if err != nil {
		f.Fatal(err)
	}
	validFrameBytes, err := encodeFrame(rec, payload)
	if err != nil {
		f.Fatal(err)
	}

	f.Add(validFrameBytes)
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{14, 0, 0, 0, 1, 2, 3, 4})
	f.Add(validFrameBytes[:len(validFrameBytes)/2]) // partial frame

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		// decodeFrame should never panic, only return frame or error
		_, _, _ = decodeFrame(r)
	})
}
