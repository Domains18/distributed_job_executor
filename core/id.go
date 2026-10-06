package core

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"
)

// JobID is a 128-bit sortable identifier:
// - 48-bit UNIX timestamp in milliseconds (high 6 bytes)
// - 80-bit cryptographically secure randomness (low 10 bytes)
// Encoded as 26 characters using Crockford Base32.
type JobID [16]byte

const (
	crockfordAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	encodedLength     = 26
)

var (
	ErrInvalidJobID = errors.New("core: invalid job id")

	decTable = [256]byte{
		'0': 0, '1': 1, '2': 2, '3': 3, '4': 4,
		'5': 5, '6': 6, '7': 7, '8': 8, '9': 9,
		'A': 10, 'B': 11, 'C': 12, 'D': 13, 'E': 14,
		'F': 15, 'G': 16, 'H': 17, 'J': 18, 'K': 19,
		'M': 20, 'N': 21, 'P': 22, 'Q': 23, 'R': 24,
		'S': 25, 'T': 26, 'V': 27, 'W': 28, 'X': 29,
		'Y': 30, 'Z': 31,
		// Lowercase
		'a': 10, 'b': 11, 'c': 12, 'd': 13, 'e': 14,
		'f': 15, 'g': 16, 'h': 17, 'j': 18, 'k': 19,
		'm': 20, 'n': 21, 'p': 22, 'q': 23, 'r': 24,
		's': 25, 't': 26, 'v': 27, 'w': 28, 'x': 29,
		'y': 30, 'z': 31,
		// Crockford aliases: i/I/l/L -> 1, o/O -> 0
		'I': 1, 'i': 1, 'L': 1, 'l': 1,
		'O': 0, 'o': 0,
	}
	validDecChar [256]bool
)

func init() {
	for i := 0; i < len(crockfordAlphabet); i++ {
		c := crockfordAlphabet[i]
		validDecChar[c] = true
		if c >= 'A' && c <= 'Z' {
			validDecChar[c+32] = true // lowercase
		}
	}
	validDecChar['I'] = true
	validDecChar['i'] = true
	validDecChar['L'] = true
	validDecChar['l'] = true
	validDecChar['O'] = true
	validDecChar['o'] = true
}

// NewJobID creates a new JobID for the current time using crypto/rand.
func NewJobID() (JobID, error) {
	return NewJobIDWithTime(time.Now(), rand.Reader)
}

// NewJobIDWithTime creates a new JobID with a specific timestamp and randomness reader.
func NewJobIDWithTime(t time.Time, r io.Reader) (JobID, error) {
	var id JobID
	ms := uint64(t.UnixMilli())
	if ms > 0xFFFFFFFFFFFF {
		return id, errors.New("core: timestamp exceeds 48 bits")
	}

	// 48-bit timestamp (big endian)
	id[0] = byte(ms >> 40)
	id[1] = byte(ms >> 32)
	id[2] = byte(ms >> 24)
	id[3] = byte(ms >> 16)
	id[4] = byte(ms >> 8)
	id[5] = byte(ms)

	// 80-bit randomness
	if _, err := io.ReadFull(r, id[6:]); err != nil {
		return id, fmt.Errorf("core: failed to read random bytes: %w", err)
	}

	return id, nil
}

// MustNewJobID creates a JobID and panics on error.
func MustNewJobID() JobID {
	id, err := NewJobID()
	if err != nil {
		panic(err)
	}
	return id
}

// Time returns the timestamp embedded in the JobID.
func (id JobID) Time() time.Time {
	ms := (uint64(id[0]) << 40) |
		(uint64(id[1]) << 32) |
		(uint64(id[2]) << 24) |
		(uint64(id[3]) << 16) |
		(uint64(id[4]) << 8) |
		uint64(id[5])
	return time.UnixMilli(int64(ms)).UTC()
}

// String encodes the JobID to Crockford Base32 (26 characters).
func (id JobID) String() string {
	var dst [encodedLength]byte

	// 10 characters for timestamp (48 bits -> 50 bits with 2 padding bits)
	dst[0] = crockfordAlphabet[(id[0]&224)>>5]
	dst[1] = crockfordAlphabet[id[0]&31]
	dst[2] = crockfordAlphabet[(id[1]&248)>>3]
	dst[3] = crockfordAlphabet[((id[1]&7)<<2)|((id[2]&192)>>6)]
	dst[4] = crockfordAlphabet[(id[2]&62)>>1]
	dst[5] = crockfordAlphabet[((id[2]&1)<<4)|((id[3]&240)>>4)]
	dst[6] = crockfordAlphabet[((id[3]&15)<<1)|((id[4]&128)>>7)]
	dst[7] = crockfordAlphabet[(id[4]&124)>>2]
	dst[8] = crockfordAlphabet[((id[4]&3)<<3)|((id[5]&224)>>5)]
	dst[9] = crockfordAlphabet[id[5]&31]

	// 16 characters for entropy (80 bits)
	dst[10] = crockfordAlphabet[(id[6]&248)>>3]
	dst[11] = crockfordAlphabet[((id[6]&7)<<2)|((id[7]&192)>>6)]
	dst[12] = crockfordAlphabet[(id[7]&62)>>1]
	dst[13] = crockfordAlphabet[((id[7]&1)<<4)|((id[8]&240)>>4)]
	dst[14] = crockfordAlphabet[((id[8]&15)<<1)|((id[9]&128)>>7)]
	dst[15] = crockfordAlphabet[(id[9]&124)>>2]
	dst[16] = crockfordAlphabet[((id[9]&3)<<3)|((id[10]&224)>>5)]
	dst[17] = crockfordAlphabet[id[10]&31]
	dst[18] = crockfordAlphabet[(id[11]&248)>>3]
	dst[19] = crockfordAlphabet[((id[11]&7)<<2)|((id[12]&192)>>6)]
	dst[20] = crockfordAlphabet[(id[12]&62)>>1]
	dst[21] = crockfordAlphabet[((id[12]&1)<<4)|((id[13]&240)>>4)]
	dst[22] = crockfordAlphabet[((id[13]&15)<<1)|((id[14]&128)>>7)]
	dst[23] = crockfordAlphabet[(id[14]&124)>>2]
	dst[24] = crockfordAlphabet[((id[14]&3)<<3)|((id[15]&224)>>5)]
	dst[25] = crockfordAlphabet[id[15]&31]

	return string(dst[:])
}

// ParseJobID parses a 26-character Crockford Base32 string into a JobID.
func ParseJobID(s string) (JobID, error) {
	var id JobID
	if len(s) != encodedLength {
		return id, ErrInvalidJobID
	}

	for i := 0; i < len(s); i++ {
		if !validDecChar[s[i]] {
			return id, ErrInvalidJobID
		}
	}

	// First character must not overflow 128 bits: top 3 bits must be 000..111
	// Specifically, in standard Crockford 26-char encoding:
	// dst[0] = (id[0] & 224) >> 5, which can be 0..7.
	if decTable[s[0]] > 7 {
		return id, ErrInvalidJobID
	}

	id[0] = (decTable[s[0]] << 5) | decTable[s[1]]
	id[1] = (decTable[s[2]] << 3) | (decTable[s[3]] >> 2)
	id[2] = (decTable[s[3]] << 6) | (decTable[s[4]] << 1) | (decTable[s[5]] >> 4)
	id[3] = (decTable[s[5]] << 4) | (decTable[s[6]] >> 1)
	id[4] = (decTable[s[6]] << 7) | (decTable[s[7]] << 2) | (decTable[s[8]] >> 3)
	id[5] = (decTable[s[8]] << 5) | decTable[s[9]]

	id[6] = (decTable[s[10]] << 3) | (decTable[s[11]] >> 2)
	id[7] = (decTable[s[11]] << 6) | (decTable[s[12]] << 1) | (decTable[s[13]] >> 4)
	id[8] = (decTable[s[13]] << 4) | (decTable[s[14]] >> 1)
	id[9] = (decTable[s[14]] << 7) | (decTable[s[15]] << 2) | (decTable[s[16]] >> 3)
	id[10] = (decTable[s[16]] << 5) | decTable[s[17]]
	id[11] = (decTable[s[18]] << 3) | (decTable[s[19]] >> 2)
	id[12] = (decTable[s[19]] << 6) | (decTable[s[20]] << 1) | (decTable[s[21]] >> 4)
	id[13] = (decTable[s[21]] << 4) | (decTable[s[22]] >> 1)
	id[14] = (decTable[s[22]] << 7) | (decTable[s[23]] << 2) | (decTable[s[24]] >> 3)
	id[15] = (decTable[s[24]] << 5) | decTable[s[25]]

	return id, nil
}

// Compare returns -1 if id < other, 1 if id > other, and 0 if id == other.
func (id JobID) Compare(other JobID) int {
	for i := 0; i < 16; i++ {
		if id[i] < other[i] {
			return -1
		} else if id[i] > other[i] {
			return 1
		}
	}
	return 0
}

// IsZero returns true if the JobID is all zeros.
func (id JobID) IsZero() bool {
	return id == JobID{}
}

// MarshalText implements encoding.TextMarshaler.
func (id JobID) MarshalText() ([]byte, error) {
	return []byte(id.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (id *JobID) UnmarshalText(text []byte) error {
	parsed, err := ParseJobID(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// MarshalJSON implements json.Marshaler.
func (id JobID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + id.String() + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (id *JobID) UnmarshalJSON(b []byte) error {
	if len(b) < 2 || b[0] != '"' || b[len(b)-1] != '"' {
		return ErrInvalidJobID
	}
	return id.UnmarshalText(b[1 : len(b)-1])
}
