package scheduler

import (
	"sync"
	"time"

	"github.com/domains18/kombucha/core"
	"github.com/domains18/kombucha/storage/engine"
)

type ParkingLot interface {
	Signal(queue string)
}

// Scheduler wakes the engine periodically to expire leases and notify waiting workers.
type Scheduler struct {
	eng     *engine.Engine
	parking ParkingLot
	clock   core.Clock
	stopCh  chan struct{}
	doneCh  chan struct{}
	mu      sync.Mutex
	running bool
}

// New creates an initialized Scheduler.
func New(eng *engine.Engine, parking ParkingLot, clock core.Clock) *Scheduler {
	if clock == nil {
		clock = core.RealClock{}
	}
	return &Scheduler{
		eng:     eng,
		parking: parking,
		clock:   clock,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

// Start runs the scheduler loop in a background goroutine.
func (s *Scheduler) Start() {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.mu.Unlock()

	go s.loop()
}

// Stop terminates the scheduler.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	s.mu.Unlock()

	close(s.stopCh)
	<-s.doneCh
}

func (s *Scheduler) loop() {
	defer close(s.doneCh)

	floorInterval := 100 * time.Millisecond

	for {
		now := s.clock.Now()

		// 1. Expire leases
		expiredCount, err := s.eng.ExpireLeases()
		if err == nil && expiredCount > 0 && s.parking != nil {
			// Leases expired, jobs returned to pending
			// parking lot will be notified per queue via OnJobEligible in engine
		}

		// 2. Compute sleep duration
		sleepDuration := floorInterval
		nextWake, ok := s.eng.Index().NextWakeTime()
		if ok {
			diff := nextWake.Sub(now)
			if diff > 0 && diff < floorInterval {
				sleepDuration = diff
			} else if diff <= 0 {
				sleepDuration = 10 * time.Millisecond
			}
		}

		select {
		case <-s.stopCh:
			return
		case <-s.clock.After(sleepDuration):
		}
	}
}
