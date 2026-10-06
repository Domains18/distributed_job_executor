package core

import (
	"sync"
	"time"
)

// Clock abstracts system time to make timing, leasing, and scheduling deterministic in tests.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
}

// RealClock implements Clock using the standard time package.
type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now().UTC()
}

func (RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (RealClock) Sleep(d time.Duration) {
	time.Sleep(d)
}

// FakeClock provides a deterministic controllable Clock for testing.
type FakeClock struct {
	mu      sync.Mutex
	current time.Time
	waiters []*fakeWaiter
}

type fakeWaiter struct {
	target time.Time
	ch     chan time.Time
}

// NewFakeClock creates a FakeClock initialized to the given time.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{
		current: t.UTC(),
	}
}

// Now returns the current simulated time.
func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current
}

// Set explicitly sets the simulated time and fires any elapsed timers.
func (f *FakeClock) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advanceToLocked(t.UTC())
}

// Advance moves the simulated time forward by d and fires any elapsed timers.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advanceToLocked(f.current.Add(d))
}

func (f *FakeClock) advanceToLocked(t time.Time) {
	f.current = t
	remaining := f.waiters[:0]
	for _, w := range f.waiters {
		if !w.target.After(f.current) {
			select {
			case w.ch <- f.current:
			default:
			}
		} else {
			remaining = append(remaining, w)
		}
	}
	f.waiters = remaining
}

// After returns a channel that receives the simulated time once current >= target.
func (f *FakeClock) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan time.Time, 1)
	target := f.current.Add(d)
	if !target.After(f.current) {
		ch <- f.current
		return ch
	}
	f.waiters = append(f.waiters, &fakeWaiter{
		target: target,
		ch:     ch,
	})
	return ch
}

// Sleep blocks until simulated time reaches current + d.
func (f *FakeClock) Sleep(d time.Duration) {
	<-f.After(d)
}
