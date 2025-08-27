// Package slots includes ticker and timer-related functions for Ethereum consensus.
package slots

import (
	"time"

	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	prysmTime "github.com/OffchainLabs/prysm/v6/time"
)

// The Ticker interface defines a type which can expose a
// receive-only channel firing slot events.
type Ticker interface {
	C() <-chan primitives.Slot
	Done()
}

// The types of ticker that can indicate different timings.
type (
	SlotTickerType         int
	SlotIntervalTickerType int
)

const (
	SlotStart SlotTickerType = iota
	AttestationThreshold

	BlockchainReorg SlotIntervalTickerType = iota
	AttestationAggregation
)

// SlotInterval is a wrapper that contains a slot and the interval index that
// triggered the ticker
type SlotInterval struct {
	Slot     primitives.Slot
	Interval int
}

// The IntervalTicker is similar to the Ticker interface but
// exposes also the interval along with the slot number
type IntervalTicker interface {
	C() <-chan SlotInterval
	Done()
}

// SlotTicker is a special ticker for the beacon chain block.
// The channel emits over the slot interval, and ensures that
// the ticks are in line with the genesis time. This means that
// the duration between the ticks and the genesis time are always a
// multiple of the slot duration.
// In addition, the channel returns the new slot number.
type SlotTicker struct {
	c          chan primitives.Slot
	done       chan struct{}
	tickerType SlotTickerType
}

// SlotIntervalTicker is similar to a slot ticker but it returns also
// the index of the interval that triggered the event
type SlotIntervalTicker struct {
	c          chan SlotInterval
	done       chan struct{}
	tickerType SlotIntervalTickerType
}

// C returns the ticker channel. Call Cancel afterwards to ensure
// that the goroutine exits cleanly.
func (s *SlotTicker) C() <-chan primitives.Slot {
	return s.c
}

// C returns the ticker channel. Call Cancel afterwards to ensure
// that the goroutine exits cleanly.
func (s *SlotIntervalTicker) C() <-chan SlotInterval {
	return s.c
}

// Done should be called to clean up the ticker.
func (s *SlotTicker) Done() {
	go func() {
		s.done <- struct{}{}
	}()
}

// Done should be called to clean up the ticker.
func (s *SlotIntervalTicker) Done() {
	go func() {
		s.done <- struct{}{}
	}()
}

// Offset takes the given slot to determine the offset of the slot for the ticker type.
func (t SlotTickerType) Offset(slot primitives.Slot) time.Duration {
	fuluForkSlot := params.BeaconConfig().SlotsPerEpoch.Mul(uint64(params.BeaconConfig().FuluForkEpoch))
	isPostFulu := slot >= fuluForkSlot

	var secondsPerSlot uint64
	if isPostFulu {
		secondsPerSlot = uint64(6)
	} else {
		secondsPerSlot = uint64(12)
	}

	// TODO: Use configs depending on ticker type.
	switch t {
	case AttestationThreshold:
		return time.Duration(secondsPerSlot/3) * time.Second
	default:
		return 0
	}
}

// Intervals takes the given slot to determine the intervals of the slot for the ticker type.
// This method could panic if the interval is not well-formed.
// lint:nopanic -- Communicated panic in godoc commentary.
func (t SlotIntervalTickerType) Intervals(slot primitives.Slot) []time.Duration {
	fuluForkSlot := params.BeaconConfig().SlotsPerEpoch.Mul(uint64(params.BeaconConfig().FuluForkEpoch))
	isPostFulu := slot >= fuluForkSlot

	var secondsPerSlot uint64
	if isPostFulu {
		secondsPerSlot = uint64(6)
	} else {
		secondsPerSlot = uint64(12)
	}

	// TODO: Use configs depending on ticker type.
	var intervals []time.Duration
	switch t {
	case BlockchainReorg:
		if isPostFulu {
			intervals = []time.Duration{0, time.Duration(secondsPerSlot-1) * time.Second}
		} else {
			intervals = []time.Duration{0, time.Duration(secondsPerSlot-2) * time.Second}
		}
	case AttestationAggregation:
		if isPostFulu {
			intervals = []time.Duration{4000 * time.Millisecond, 5250 * time.Millisecond, 5800 * time.Millisecond}
		} else {
			intervals = []time.Duration{7000 * time.Millisecond, 9500 * time.Millisecond, 11800 * time.Millisecond}
		}
	default:
		panic("unsupported ticker type for intervals")
	}

	if len(intervals) == 0 {
		panic("at least one interval has to be entered")
	}
	slotDuration := time.Duration(secondsPerSlot) * time.Second
	lastOffset := time.Duration(0)
	for _, offset := range intervals {
		if offset < lastOffset {
			panic("invalid decreasing offsets")
		}
		if offset >= slotDuration {
			panic("invalid ticker offset")
		}
		lastOffset = offset
	}

	return intervals
}

// NewSlotTicker starts and returns a new SlotTicker instance.
// This method panics if genesis time is zero.
// lint:nopanic -- Communicated panic in godoc commentary.
func NewSlotTicker(genesisTime time.Time, tickerType SlotTickerType) *SlotTicker {
	if genesisTime.IsZero() {
		panic("zero genesis time")
	}
	ticker := &SlotTicker{
		c:          make(chan primitives.Slot),
		done:       make(chan struct{}),
		tickerType: tickerType,
	}
	ticker.start(genesisTime, prysmTime.Until, time.After)
	return ticker
}

func (s *SlotTicker) start(
	genesisTime time.Time,
	until func(time.Time) time.Duration,
	after func(time.Duration) <-chan time.Time) {
	go func() {
		slot := CurrentSlot(genesisTime)
		if slot > 0 {
			// Tick for the next slot unless the current time is before the genesis time.
			slot++
		}
		nextOffset := s.tickerType.Offset(slot)
		nextTickTime := UnsafeStartTime(genesisTime, slot).Add(nextOffset)

		for {
			waitTime := until(nextTickTime)
			select {
			case <-after(waitTime):
				s.c <- slot
				slot++
				nextOffset = s.tickerType.Offset(slot)
				nextTickTime = UnsafeStartTime(genesisTime, slot).Add(nextOffset)
			case <-s.done:
				return
			}
		}
	}()
}

// startWithIntervals starts a ticker that emits a tick every slot at the
// prescribed intervals. The caller is responsible to make these intervals increasing and
// less than secondsPerSlot
func (s *SlotIntervalTicker) startWithIntervals(
	genesisTime time.Time,
	until func(time.Time) time.Duration,
	after func(time.Duration) <-chan time.Time) {
	go func() {
		slot := CurrentSlot(genesisTime) + 1
		interval := 0
		intervals := s.tickerType.Intervals(slot)
		nextTickTime := UnsafeStartTime(genesisTime, slot).Add(intervals[0])

		for {
			waitTime := until(nextTickTime)
			select {
			case <-after(waitTime):
				s.c <- SlotInterval{Slot: slot, Interval: interval}
				interval++
				if interval == len(intervals) {
					slot++
					interval = 0
					intervals = s.tickerType.Intervals(slot)
				}
				nextTickTime = UnsafeStartTime(genesisTime, slot).Add(intervals[interval])
			case <-s.done:
				return
			}
		}
	}()
}

// NewSlotTickerWithIntervals starts and returns a SlotTicker instance that allows
// several offsets of time from genesis,
// Caller is responsible to configure the intervals in increasing order and none bigger or equal than
// SecondsPerSlot
// This method will panic if genesis time is zero, intervals is 0 length, or offsets are invalid.
// lint:nopanic -- Communicated panic in godoc commentary.
func NewSlotTickerWithIntervals(genesisTime time.Time, tickerType SlotIntervalTickerType) *SlotIntervalTicker {
	if genesisTime.Unix() == 0 {
		panic("zero genesis time")
	}
	ticker := &SlotIntervalTicker{
		c:          make(chan SlotInterval),
		done:       make(chan struct{}),
		tickerType: tickerType,
	}
	ticker.startWithIntervals(genesisTime, prysmTime.Until, time.After)
	return ticker
}
