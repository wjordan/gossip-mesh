package engine

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// OrderedApplier ensures in-order delivery of gossip entries per slot.
// Out-of-order entries are buffered up to maxGapBuffer. If the gap isn't
// filled within gapTimeout, the applier skips ahead (repair will catch up).
type OrderedApplier struct {
	nextSeq [maxSlots]atomic.Uint64
	pending [maxSlots]sync.Map // seq → []byte (payload)
	deliver func(topic uint16, seq uint64, payload []byte)

	maxGapBuffer int
	gapTimeout   time.Duration

	// Active gap timers — one per slot. We track them to avoid spawning
	// duplicate timers.
	gapTimers [maxSlots]atomic.Int32 // 1 = timer active, 0 = idle
}

// NewOrderedApplier creates an OrderedApplier that calls deliver for
// each entry in sequence order.
func NewOrderedApplier(deliver func(uint16, uint64, []byte)) *OrderedApplier {
	return &OrderedApplier{
		deliver:      deliver,
		maxGapBuffer: 64,
		gapTimeout:   500 * time.Millisecond,
	}
}

// Receive processes an incoming entry. If it's the expected next seq,
// it is delivered immediately along with any buffered successors.
// If it's ahead of the expected seq, it's buffered for later.
func (o *OrderedApplier) Receive(topic uint16, seq uint64, payload []byte) {
	expected := o.nextSeq[topic].Load()

	if seq < expected {
		return // duplicate, drop
	}

	if seq > expected {
		count := seq - expected
		if count <= uint64(o.maxGapBuffer) {
			// Small gap — buffer and wait for missing entries.
			buf := make([]byte, len(payload))
			copy(buf, payload)
			o.pending[topic].Store(seq, buf)
			if o.gapTimers[topic].CompareAndSwap(0, 1) {
				go o.gapTimer(topic, expected)
			}
		} else {
			// Gap too large to buffer — deliver immediately and skip ahead.
			// The downstream consumer must handle out-of-order delivery.
			o.deliver(topic, seq, payload)
			o.nextSeq[topic].Store(seq + 1)
			o.drainPending(topic)
		}
		return
	}

	// seq == expected: apply and drain buffered successors.
	o.deliver(topic, seq, payload)
	o.nextSeq[topic].Add(1)
	o.drainPending(topic)
}

// drainPending delivers consecutive buffered entries starting from nextSeq.
func (o *OrderedApplier) drainPending(topic uint16) {
	for {
		next := o.nextSeq[topic].Load()
		val, ok := o.pending[topic].LoadAndDelete(next)
		if !ok {
			break
		}
		o.deliver(topic, next, val.([]byte))
		o.nextSeq[topic].Add(1)
	}
}

// gapTimer waits for gapTimeout and then skips the gap if it wasn't filled.
func (o *OrderedApplier) gapTimer(topic uint16, expectedAtStart uint64) {
	defer o.gapTimers[topic].Store(0)
	time.Sleep(o.gapTimeout)

	current := o.nextSeq[topic].Load()
	if current == expectedAtStart {
		// Gap not filled — skip ahead to next available.
		o.skipGap(topic)
	}
}

// skipGap finds the lowest pending seq and advances to it.
func (o *OrderedApplier) skipGap(topic uint16) {
	var minPending uint64 = math.MaxUint64
	o.pending[topic].Range(func(key, _ any) bool {
		seq := key.(uint64)
		if seq < minPending {
			minPending = seq
		}
		return true
	})
	if minPending != math.MaxUint64 {
		o.nextSeq[topic].Store(minPending)
		o.drainPending(topic)
	}
}

// SetNextSeq resets the expected seq for a slot. Used during rollback
// or initial sync.
func (o *OrderedApplier) SetNextSeq(topic uint16, seq uint64) {
	o.nextSeq[topic].Store(seq)
	// Clear all pending for this slot.
	o.pending[topic].Range(func(key, _ any) bool {
		o.pending[topic].Delete(key)
		return true
	})
}

// NextSeq returns the next expected sequence for a slot.
func (o *OrderedApplier) NextSeq(topic uint16) uint64 {
	return o.nextSeq[topic].Load()
}
