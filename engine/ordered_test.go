package engine

import (
	"sync"
	"testing"
	"time"
)

func TestOrderedApplier_InOrderDelivery(t *testing.T) {
	var delivered []uint64
	var mu sync.Mutex

	oa := NewOrderedApplier(func(topic uint16, seq uint64, payload []byte) {
		mu.Lock()
		delivered = append(delivered, seq)
		mu.Unlock()
	})
	oa.SetNextSeq(0, 1)

	oa.Receive(0, 1, []byte("a"))
	oa.Receive(0, 2, []byte("b"))
	oa.Receive(0, 3, []byte("c"))

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != 3 {
		t.Fatalf("expected 3 deliveries, got %d", len(delivered))
	}
	for i, seq := range delivered {
		if seq != uint64(i+1) {
			t.Fatalf("delivery %d: expected seq %d, got %d", i, i+1, seq)
		}
	}
}

func TestOrderedApplier_OutOfOrderBuffering(t *testing.T) {
	var delivered []uint64
	var mu sync.Mutex

	oa := NewOrderedApplier(func(topic uint16, seq uint64, payload []byte) {
		mu.Lock()
		delivered = append(delivered, seq)
		mu.Unlock()
	})
	oa.SetNextSeq(0, 1)

	// Deliver out of order: 3, 2, 1.
	oa.Receive(0, 3, []byte("c"))
	oa.Receive(0, 2, []byte("b"))

	mu.Lock()
	if len(delivered) != 0 {
		mu.Unlock()
		t.Fatal("nothing should be delivered yet (gap at seq 1)")
	}
	mu.Unlock()

	// Now deliver seq 1 — should drain 1, 2, 3.
	oa.Receive(0, 1, []byte("a"))

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != 3 {
		t.Fatalf("expected 3 deliveries, got %d", len(delivered))
	}
	for i, seq := range delivered {
		if seq != uint64(i+1) {
			t.Fatalf("delivery %d: expected seq %d, got %d", i, i+1, seq)
		}
	}
}

func TestOrderedApplier_DuplicateRejected(t *testing.T) {
	deliveryCount := 0
	oa := NewOrderedApplier(func(topic uint16, seq uint64, payload []byte) {
		deliveryCount++
	})
	oa.SetNextSeq(0, 1)

	oa.Receive(0, 1, []byte("a"))
	oa.Receive(0, 1, []byte("a")) // duplicate

	if deliveryCount != 1 {
		t.Fatalf("expected 1 delivery, got %d", deliveryCount)
	}
}

func TestOrderedApplier_GapTimeout(t *testing.T) {
	var delivered []uint64
	var mu sync.Mutex

	oa := NewOrderedApplier(func(topic uint16, seq uint64, payload []byte) {
		mu.Lock()
		delivered = append(delivered, seq)
		mu.Unlock()
	})
	oa.gapTimeout = 20 * time.Millisecond // fast timeout for testing
	oa.SetNextSeq(0, 1)

	// Send seq 3, skipping 1 and 2.
	oa.Receive(0, 3, []byte("c"))

	mu.Lock()
	if len(delivered) != 0 {
		mu.Unlock()
		t.Fatal("nothing should be delivered yet")
	}
	mu.Unlock()

	// Wait for gap timeout to fire.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != 1 || delivered[0] != 3 {
		t.Fatalf("expected [3] after gap timeout, got %v", delivered)
	}
	if oa.NextSeq(0) != 4 {
		t.Fatalf("expected nextSeq=4 after skip, got %d", oa.NextSeq(0))
	}
}

func TestOrderedApplier_IndependentSlots(t *testing.T) {
	slot0Count := 0
	slot1Count := 0

	oa := NewOrderedApplier(func(topic uint16, seq uint64, payload []byte) {
		if topic == 0 {
			slot0Count++
		} else {
			slot1Count++
		}
	})
	oa.SetNextSeq(0, 1)
	oa.SetNextSeq(1, 1)

	oa.Receive(0, 1, []byte("a"))
	oa.Receive(1, 1, []byte("x"))
	oa.Receive(1, 2, []byte("y"))

	if slot0Count != 1 {
		t.Fatalf("slot 0: expected 1, got %d", slot0Count)
	}
	if slot1Count != 2 {
		t.Fatalf("slot 1: expected 2, got %d", slot1Count)
	}
}

func TestOrderedApplier_SetNextSeq_ClearsPending(t *testing.T) {
	deliveryCount := 0
	oa := NewOrderedApplier(func(topic uint16, seq uint64, payload []byte) {
		deliveryCount++
	})
	oa.SetNextSeq(0, 1)

	// Buffer seq 3 (gap at 1, 2).
	oa.Receive(0, 3, []byte("c"))

	// Reset to seq 10 (simulating rollback).
	oa.SetNextSeq(0, 10)

	// Old buffered seq 3 should be gone.
	oa.Receive(0, 10, []byte("j"))
	if deliveryCount != 1 {
		t.Fatalf("expected 1 delivery (seq 10), got %d", deliveryCount)
	}
}

func TestOrderedApplier_SecondaryGapAfterSkip(t *testing.T) {
	// Regression test: after skipGap drains pending and hits a secondary gap,
	// entries beyond the second gap must eventually be delivered (not stranded).
	var delivered []uint64
	var mu sync.Mutex

	oa := NewOrderedApplier(func(topic uint16, seq uint64, payload []byte) {
		mu.Lock()
		delivered = append(delivered, seq)
		mu.Unlock()
	})
	oa.gapTimeout = 50 * time.Millisecond
	oa.SetNextSeq(0, 1)

	// Buffer seq 3 and seq 7 (two gaps: missing 1-2 and 4-6).
	oa.Receive(0, 3, []byte("c"))
	oa.Receive(0, 7, []byte("g"))

	// Wait for first gap timeout to fire and skip to seq 3.
	// drainPending delivers 3, then hits secondary gap at 4-6.
	// The fix ensures a new gap timer starts for seq 7.
	time.Sleep(80 * time.Millisecond)

	mu.Lock()
	if len(delivered) != 1 || delivered[0] != 3 {
		mu.Unlock()
		t.Fatalf("after first gap timeout: expected [3], got %v", delivered)
	}
	mu.Unlock()

	// Wait for second gap timeout to fire and skip to seq 7.
	time.Sleep(80 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != 2 {
		t.Fatalf("expected 2 deliveries after both gap timeouts, got %d: %v", len(delivered), delivered)
	}
	if delivered[1] != 7 {
		t.Fatalf("expected second delivery to be seq 7, got %d", delivered[1])
	}
	if oa.NextSeq(0) != 8 {
		t.Fatalf("expected nextSeq=8, got %d", oa.NextSeq(0))
	}
}

func TestOrderedApplier_PayloadPreserved(t *testing.T) {
	var got []byte
	oa := NewOrderedApplier(func(topic uint16, seq uint64, payload []byte) {
		got = payload
	})
	oa.SetNextSeq(0, 1)

	payload := []byte("hello world")
	oa.Receive(0, 1, payload)

	if string(got) != "hello world" {
		t.Fatalf("expected 'hello world', got %q", string(got))
	}
}
