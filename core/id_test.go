package core

import (
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestJobID_GenerationAndParse(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	id, err := NewJobIDWithTime(now, rand.New(rand.NewSource(42)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	str := id.String()
	if len(str) != 26 {
		t.Fatalf("expected 26 chars, got %d: %s", len(str), str)
	}

	parsed, err := ParseJobID(str)
	if err != nil {
		t.Fatalf("failed to parse job id: %v", err)
	}

	if parsed != id {
		t.Fatalf("parsed ID %v != original ID %v", parsed, id)
	}

	extractedTime := parsed.Time()
	if !extractedTime.Equal(now) {
		t.Fatalf("extracted time %v != original time %v", extractedTime, now)
	}
}

func TestJobID_Sorting(t *testing.T) {
	t1 := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 10, 6, 11, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

	r := rand.New(rand.NewSource(1))
	id1, _ := NewJobIDWithTime(t1, r)
	id2, _ := NewJobIDWithTime(t2, r)
	id3, _ := NewJobIDWithTime(t3, r)

	ids := []JobID{id3, id1, id2}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].Compare(ids[j]) < 0
	})

	if ids[0] != id1 || ids[1] != id2 || ids[2] != id3 {
		t.Fatalf("unexpected sort order: %v, %v, %v", ids[0], ids[1], ids[2])
	}

	// Also verify string sort matches byte sort
	strs := []string{id3.String(), id1.String(), id2.String()}
	sort.Strings(strs)
	if strs[0] != id1.String() || strs[1] != id2.String() || strs[2] != id3.String() {
		t.Fatalf("unexpected string sort order: %v", strs)
	}
}

func TestJobID_InvalidStrings(t *testing.T) {
	invalid := []string{
		"",
		"123",
		"0123456789012345678901234567",    // 28 chars
		"0123456789012345678901234",      // 25 chars
		"81234567890123456789012345",      // '8' > 7 (overflows 128 bits)
		"0123456789012345678901234U",      // 'U' is invalid in Crockford
	}

	for _, s := range invalid {
		if _, err := ParseJobID(s); err == nil {
			t.Errorf("expected error for invalid string %q, got nil", s)
		}
	}
}

func TestJobID_JSONMarshaling(t *testing.T) {
	id, err := NewJobID()
	if err != nil {
		t.Fatal(err)
	}

	b, err := id.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	var parsed JobID
	if err := parsed.UnmarshalJSON(b); err != nil {
		t.Fatal(err)
	}

	if parsed != id {
		t.Fatalf("parsed %v != original %v", parsed, id)
	}
}

func TestJobID_CrockfordAliases(t *testing.T) {
	// Crockford allows i/I/l/L -> 1, o/O -> 0
	orig := "01J00000000000000000000001"
	alias := "o1joooooooooooooooooooooo1" // lowercase and 'o' instead of '0'

	idOrig, err := ParseJobID(orig)
	if err != nil {
		t.Fatalf("orig failed: %v", err)
	}
	idAlias, err := ParseJobID(alias)
	if err != nil {
		t.Fatalf("alias failed: %v", err)
	}

	if idOrig != idAlias {
		t.Fatalf("expected alias to parse to same ID, got %v and %v", idOrig, idAlias)
	}
}
