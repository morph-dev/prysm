package slots

import (
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
)

var _ Ticker = (*SlotTicker)(nil)

func TestSlotTicker(t *testing.T) {
	ticker := &SlotTicker{
		c:    make(chan primitives.Slot),
		done: make(chan struct{}),
	}
	defer ticker.Done()

	var untilDuration time.Duration
	until := func(time.Time) time.Duration {
		return untilDuration
	}

	var tick chan time.Time
	after := func(time.Duration) <-chan time.Time {
		return tick
	}

	genesisTime := time.Now()

	// Test when the ticker starts immediately after genesis time.
	untilDuration = time.Duration(params.BeaconConfig().SecondsPerSlot-1) * time.Second
	// Make this a buffered channel to prevent a deadlock since
	// the other goroutine calls a function in this goroutine.
	tick = make(chan time.Time, 2)
	ticker.start(genesisTime, until, after)

	// Tick once.
	tick <- time.Now()
	slot := <-ticker.C()
	if slot != 0 {
		t.Fatalf("Expected %d, got %d", 0, slot)
	}

	// Tick twice.
	tick <- time.Now()
	slot = <-ticker.C()
	if slot != 1 {
		t.Fatalf("Expected %d, got %d", 1, slot)
	}

	// Tick thrice.
	tick <- time.Now()
	slot = <-ticker.C()
	if slot != 2 {
		t.Fatalf("Expected %d, got %d", 2, slot)
	}
}

func TestSlotTickerBeforeGenesis(t *testing.T) {
	ticker := &SlotTicker{
		c:    make(chan primitives.Slot),
		done: make(chan struct{}),
	}
	defer ticker.Done()

	var untilDuration time.Duration
	until := func(time.Time) time.Duration {
		return untilDuration
	}

	var tick chan time.Time
	after := func(time.Duration) <-chan time.Time {
		return tick
	}

	genesisTime := time.Now().Add(3 * time.Second)

	// Test when the ticker starts before genesis time.
	untilDuration = 1 * time.Second
	// Make this a buffered channel to prevent a deadlock since
	// the other goroutine calls a function in this goroutine.
	tick = make(chan time.Time, 2)
	ticker.start(genesisTime, until, after)

	// Tick once.
	tick <- time.Now()
	slot := <-ticker.C()
	if slot != 0 {
		t.Fatalf("Expected %d, got %d", 0, slot)
	}

	// Tick twice.
	tick <- time.Now()
	slot = <-ticker.C()
	if slot != 1 {
		t.Fatalf("Expected %d, got %d", 1, slot)
	}
}

func TestSlotTickerAfterGenesis(t *testing.T) {
	ticker := &SlotTicker{
		c:    make(chan primitives.Slot),
		done: make(chan struct{}),
	}
	defer ticker.Done()

	var untilDuration time.Duration
	until := func(time.Time) time.Duration {
		return untilDuration
	}

	var tick chan time.Time
	after := func(time.Duration) <-chan time.Time {
		return tick
	}

	secondsPerSlot := params.BeaconConfig().SecondsPerSlot
	genesisTime := time.Now().Add(-time.Duration(secondsPerSlot+1) * time.Second)

	// Test when the ticker starts after genesis time.
	untilDuration = 1 * time.Second
	// Make this a buffered channel to prevent a deadlock since
	// the other goroutine calls a function in this goroutine.
	tick = make(chan time.Time, 2)
	ticker.start(genesisTime, until, after)

	// Tick once.
	tick <- time.Now()
	slot := <-ticker.C()
	if slot != 2 {
		t.Fatalf("Expected %d, got %d", 2, slot)
	}

	// Tick twice.
	tick <- time.Now()
	slot = <-ticker.C()
	if slot != 3 {
		t.Fatalf("Expected %d, got %d", 3, slot)
	}
}

func TestGetSlotTickerWithOffset_OK(t *testing.T) {
	genesisTime := time.Now()

	offsetTicker := NewSlotTicker(genesisTime, AttestationThreshold)
	normalTicker := NewSlotTicker(genesisTime, SlotStart)

	firstTicked := 0
	for {
		select {
		case <-offsetTicker.C():
			if firstTicked != 1 {
				t.Fatal("Expected other ticker to tick first")
			}
			return
		case <-normalTicker.C():
			if firstTicked != 0 {
				t.Fatal("Expected normal ticker to tick first")
			}
			firstTicked = 1
		}
	}
}

func TestGetSlotTickerWitIntervals(t *testing.T) {
	genesisTime := time.Now()

	intervalTicker := NewSlotTickerWithIntervals(genesisTime, AttestationAggregation)
	normalTicker := NewSlotTicker(genesisTime, SlotStart)

	firstTicked := 0
	for {
		select {
		case <-intervalTicker.C():
			// interval ticks starts in second slot
			if firstTicked < 2 {
				t.Fatal("Expected other ticker to tick first")
			}
			return
		case <-normalTicker.C():
			if firstTicked > 1 {
				t.Fatal("Expected normal ticker to tick first")
			}
			firstTicked++
		}
	}
}
