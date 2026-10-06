package api

import (
	"context"
	"sync"
)

// ParkingLot implements long-poll parking with single-waiter wakeup to prevent thundering herds.
type ParkingLot struct {
	mu      sync.Mutex
	waiters map[string][]chan struct{}
}

// NewParkingLot creates an initialized ParkingLot.
func NewParkingLot() *ParkingLot {
	return &ParkingLot{
		waiters: make(map[string][]chan struct{}),
	}
}

// Wait blocks until one of the specified queues is signaled or ctx is cancelled.
// Returns true if woken by a signal, false if timed out or cancelled.
func (p *ParkingLot) Wait(ctx context.Context, queues []string) bool {
	if len(queues) == 0 {
		return false
	}

	ch := make(chan struct{}, 1)

	p.mu.Lock()
	for _, q := range queues {
		p.waiters[q] = append(p.waiters[q], ch)
	}
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		for _, q := range queues {
			waiters := p.waiters[q]
			for i, w := range waiters {
				if w == ch {
					// Remove without order preservation
					p.waiters[q] = append(waiters[:i], waiters[i+1:]...)
					break
				}
			}
			if len(p.waiters[q]) == 0 {
				delete(p.waiters, q)
			}
		}
		p.mu.Unlock()
	}()

	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// Signal wakes exactly ONE waiting worker listening on queue.
func (p *ParkingLot) Signal(queue string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	waiters := p.waiters[queue]
	for len(waiters) > 0 {
		w := waiters[0]
		p.waiters[queue] = waiters[1:]

		select {
		case w <- struct{}{}:
			// Successfully signaled one waiter
			return
		default:
			// Waiter channel already had a signal or was abandoned
			waiters = p.waiters[queue]
		}
	}
}
