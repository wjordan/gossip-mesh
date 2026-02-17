package engine

import (
	"sync"
	"sync/atomic"
)

// maxSlots matches the 16-bit node ID space.
const maxSlots = 65536

// SeenTracker tracks the highest processed sequence number per slot for
// O(1) dedup. Since sequences are monotonic per slot, we only need to
// compare against the high-water mark.
//
// An activeSlots set tracks which slots have been seen, so HighWaterMarks
// iterates only active slots (O(active)) instead of the full 65536 array.
type SeenTracker struct {
	slotSeqs [maxSlots]atomic.Uint64

	activeMu    sync.RWMutex
	activeSlots map[uint16]struct{}
}

// ShouldProcess returns true if the (topic, seq) pair hasn't been seen.
// It atomically advances the high-water mark if the seq is new.
func (s *SeenTracker) ShouldProcess(topic uint16, seq uint64) bool {
	for {
		old := s.slotSeqs[topic].Load()
		if seq <= old {
			return false // already seen or older
		}
		if s.slotSeqs[topic].CompareAndSwap(old, seq) {
			if old == 0 {
				s.markActive(topic)
			}
			return true
		}
		// CAS failed, retry (concurrent update)
	}
}

// CurrentSeq returns the current high-water mark for a slot.
func (s *SeenTracker) CurrentSeq(topic uint16) uint64 {
	return s.slotSeqs[topic].Load()
}

// TopicSeq pairs a topic with its sequence number.
type TopicSeq struct {
	Topic uint16
	Seq   uint64
}

// HighWaterMarks returns a snapshot of all non-zero slot sequences.
// Used for piggybacking on lazy batches so the receiver knows what
// the sender has already seen.
func (s *SeenTracker) HighWaterMarks() []TopicSeq {
	s.activeMu.RLock()
	marks := make([]TopicSeq, 0, len(s.activeSlots))
	for topic := range s.activeSlots {
		seq := s.slotSeqs[topic].Load()
		if seq > 0 {
			marks = append(marks, TopicSeq{Topic: topic, Seq: seq})
		}
	}
	s.activeMu.RUnlock()
	return marks
}

// SetTopicSeq sets the high-water mark for a slot. Used during
// initialization or rollback.
func (s *SeenTracker) SetTopicSeq(topic uint16, seq uint64) {
	s.slotSeqs[topic].Store(seq)
	if seq > 0 {
		s.markActive(topic)
	}
}

// markActive adds a slot to the active set. Called on the first non-zero
// sequence for a slot.
func (s *SeenTracker) markActive(topic uint16) {
	s.activeMu.RLock()
	_, ok := s.activeSlots[topic]
	s.activeMu.RUnlock()
	if ok {
		return
	}
	s.activeMu.Lock()
	if s.activeSlots == nil {
		s.activeSlots = make(map[uint16]struct{})
	}
	s.activeSlots[topic] = struct{}{}
	s.activeMu.Unlock()
}
