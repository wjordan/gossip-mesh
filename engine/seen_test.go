package engine

import (
	"sync"
	"testing"
)

func TestSeenTracker_ShouldProcess_Monotonic(t *testing.T) {
	var s SeenTracker

	// First seq for slot 0 should be processed.
	if !s.ShouldProcess(0, 1) {
		t.Fatal("seq 1 should be new")
	}
	if s.CurrentSeq(0) != 1 {
		t.Fatalf("expected high-water 1, got %d", s.CurrentSeq(0))
	}

	// Older/same seq should be rejected.
	if s.ShouldProcess(0, 1) {
		t.Fatal("duplicate seq 1 should be rejected")
	}
	if s.ShouldProcess(0, 0) {
		t.Fatal("older seq 0 should be rejected")
	}

	// Higher seq advances the mark.
	if !s.ShouldProcess(0, 5) {
		t.Fatal("seq 5 should be new")
	}
	if s.CurrentSeq(0) != 5 {
		t.Fatalf("expected high-water 5, got %d", s.CurrentSeq(0))
	}

	// Intervening seq (3) is lower than high-water.
	if s.ShouldProcess(0, 3) {
		t.Fatal("seq 3 should be rejected (below high-water 5)")
	}
}

func TestSeenTracker_IndependentSlots(t *testing.T) {
	var s SeenTracker

	s.ShouldProcess(0, 10)
	s.ShouldProcess(1, 20)

	if s.CurrentSeq(0) != 10 {
		t.Fatalf("slot 0: expected 10, got %d", s.CurrentSeq(0))
	}
	if s.CurrentSeq(1) != 20 {
		t.Fatalf("slot 1: expected 20, got %d", s.CurrentSeq(1))
	}

	// Slot 0 should not be affected by slot 1.
	if !s.ShouldProcess(0, 15) {
		t.Fatal("slot 0 seq 15 should be new")
	}
	if s.ShouldProcess(1, 15) {
		t.Fatal("slot 1 seq 15 should be rejected (below 20)")
	}
}

func TestSeenTracker_ConcurrentAccess(t *testing.T) {
	var s SeenTracker
	const goroutines = 8
	const seqsPerGoroutine = 1000

	var wg sync.WaitGroup
	accepted := make([]int, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for seq := uint64(1); seq <= seqsPerGoroutine; seq++ {
				if s.ShouldProcess(0, seq) {
					accepted[id]++
				}
			}
		}(g)
	}
	wg.Wait()

	// Each seq should be accepted exactly once across all goroutines.
	total := 0
	for _, n := range accepted {
		total += n
	}
	if total != seqsPerGoroutine {
		t.Fatalf("expected exactly %d accepted (one per seq), got %d", seqsPerGoroutine, total)
	}
	if s.CurrentSeq(0) != seqsPerGoroutine {
		t.Fatalf("expected final high-water %d, got %d", seqsPerGoroutine, s.CurrentSeq(0))
	}
}

func TestSeenTracker_HighWaterMarks(t *testing.T) {
	var s SeenTracker

	s.ShouldProcess(0, 5)
	s.ShouldProcess(3, 10)
	s.ShouldProcess(100, 1)

	marks := s.HighWaterMarks()
	if len(marks) != 3 {
		t.Fatalf("expected 3 marks, got %d", len(marks))
	}

	// Build map for easy lookup.
	m := make(map[uint16]uint64)
	for _, mark := range marks {
		m[mark.Topic] = mark.Seq
	}
	if m[0] != 5 || m[3] != 10 || m[100] != 1 {
		t.Fatalf("unexpected marks: %v", m)
	}
}

func TestSeenTracker_SetTopicSeq(t *testing.T) {
	var s SeenTracker

	s.ShouldProcess(0, 10)
	// Reset (e.g., rollback) to lower value.
	s.SetTopicSeq(0, 5)
	if s.CurrentSeq(0) != 5 {
		t.Fatalf("expected 5 after SetTopicSeq, got %d", s.CurrentSeq(0))
	}
	// Can now accept seqs above 5 again.
	if !s.ShouldProcess(0, 7) {
		t.Fatal("seq 7 should be accepted after reset to 5")
	}
}
