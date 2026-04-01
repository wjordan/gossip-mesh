package engine

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"
	"testing"
	"time"
)

func TestRepairManager_StoreAndLoad(t *testing.T) {
	rm := &RepairManager{cache: make(map[TopicSeq][]byte)}

	rm.CacheEntry(GossipEntry{Topic: 0, Seq: 1, Payload: []byte("hello")})
	rm.CacheEntry(GossipEntry{Topic: 0, Seq: 2, Payload: []byte("world")})
	rm.CacheEntry(GossipEntry{Topic: 3, Seq: 10, Payload: []byte("other")})

	rm.cacheMu.RLock()
	defer rm.cacheMu.RUnlock()

	if p, ok := rm.cache[TopicSeq{0, 1}]; !ok || string(p) != "hello" {
		t.Fatalf("expected 'hello', got %q (ok=%v)", p, ok)
	}
	if p, ok := rm.cache[TopicSeq{0, 2}]; !ok || string(p) != "world" {
		t.Fatalf("expected 'world', got %q (ok=%v)", p, ok)
	}
	if p, ok := rm.cache[TopicSeq{3, 10}]; !ok || string(p) != "other" {
		t.Fatalf("expected 'other', got %q (ok=%v)", p, ok)
	}
}

func TestRepairManager_Eviction(t *testing.T) {
	rm := &RepairManager{cache: make(map[TopicSeq][]byte)}

	for i := 0; i < maxRepairCache+1; i++ {
		rm.CacheEntry(GossipEntry{Topic: 0, Seq: uint64(i), Payload: []byte("x")})
	}

	rm.cacheMu.RLock()
	defer rm.cacheMu.RUnlock()

	if len(rm.cache) > maxRepairCache {
		t.Fatalf("cache should be bounded at %d, got %d", maxRepairCache, len(rm.cache))
	}
}

func TestRepairManager_PayloadIsolation(t *testing.T) {
	rm := &RepairManager{cache: make(map[TopicSeq][]byte)}

	payload := []byte("original")
	rm.CacheEntry(GossipEntry{Topic: 0, Seq: 1, Payload: payload})
	payload[0] = 'X'

	rm.cacheMu.RLock()
	defer rm.cacheMu.RUnlock()
	if string(rm.cache[TopicSeq{0, 1}]) != "original" {
		t.Fatal("cache payload should be independent of caller's buffer")
	}
}

// pipeStream is a bidirectional in-memory stream for testing HandleRequest.
// Close shuts down the write side so the other end's reads get EOF.
type pipeStream struct {
	r *io.PipeReader
	w *io.PipeWriter
}

func (p *pipeStream) Read(b []byte) (int, error)  { return p.r.Read(b) }
func (p *pipeStream) Write(b []byte) (int, error) { return p.w.Write(b) }
func (p *pipeStream) Close() error {
	p.w.Close()
	return p.r.Close()
}

func newPipeStreamPair() (client, server *pipeStream) {
	cr, sw := io.Pipe()
	sr, cw := io.Pipe()
	client = &pipeStream{r: cr, w: cw}
	server = &pipeStream{r: sr, w: sw}
	return
}

func TestRepairManager_HandleRequestHit(t *testing.T) {
	rm := &RepairManager{cache: make(map[TopicSeq][]byte)}
	rm.CacheEntry(GossipEntry{Topic: 7, Seq: 42, Payload: []byte("repaired-data")})

	client, server := newPipeStreamPair()
	defer client.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rm.HandleRequest("peer-1", server)
	}()

	var req [10]byte
	binary.BigEndian.PutUint16(req[:2], 7)
	binary.BigEndian.PutUint64(req[2:], 42)
	if _, err := client.Write(req[:]); err != nil {
		t.Fatalf("write request: %v", err)
	}

	var status [1]byte
	if _, err := io.ReadFull(client, status[:]); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status[0] != 0x01 {
		t.Fatalf("expected hit (0x01), got 0x%02x", status[0])
	}

	payload, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if !bytes.Equal(payload, []byte("repaired-data")) {
		t.Fatalf("expected 'repaired-data', got %q", payload)
	}

	wg.Wait()
}

func TestRepairManager_HandleRequestMiss(t *testing.T) {
	rm := &RepairManager{cache: make(map[TopicSeq][]byte)}

	client, server := newPipeStreamPair()
	defer client.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rm.HandleRequest("peer-1", server)
	}()

	var req [10]byte
	binary.BigEndian.PutUint16(req[:2], 99)
	binary.BigEndian.PutUint64(req[2:], 999)
	if _, err := client.Write(req[:]); err != nil {
		t.Fatalf("write request: %v", err)
	}

	var status [1]byte
	if _, err := io.ReadFull(client, status[:]); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status[0] != 0x00 {
		t.Fatalf("expected miss (0x00), got 0x%02x", status[0])
	}

	wg.Wait()
}

func TestOrderedApplier_OnGapRepairSuccess(t *testing.T) {
	var delivered []uint64
	var mu sync.Mutex

	oa := NewOrderedApplier(func(topic uint16, seq uint64, payload []byte) {
		mu.Lock()
		delivered = append(delivered, seq)
		mu.Unlock()
	})
	oa.gapTimeout = 20 * time.Millisecond
	oa.SetNextSeq(0, 1)

	// When the gap callback fires for the missing seq, deliver it
	// synchronously via Receive — simulating a successful repair pull.
	oa.onGap = func(topic uint16, seq uint64) bool {
		oa.Receive(topic, seq, []byte("repaired"))
		return true
	}

	oa.Receive(0, 2, []byte("b"))

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Both seq 1 (repaired) and seq 2 (buffered) should be delivered in order.
	if len(delivered) != 2 {
		t.Fatalf("expected 2 deliveries, got %d: %v", len(delivered), delivered)
	}
	if delivered[0] != 1 || delivered[1] != 2 {
		t.Fatalf("expected [1, 2], got %v", delivered)
	}
	if oa.NextSeq(0) != 3 {
		t.Fatalf("expected nextSeq=3, got %d", oa.NextSeq(0))
	}
}

func TestOrderedApplier_OnGapRepairFailure(t *testing.T) {
	var delivered []uint64
	var mu sync.Mutex

	oa := NewOrderedApplier(func(topic uint16, seq uint64, payload []byte) {
		mu.Lock()
		delivered = append(delivered, seq)
		mu.Unlock()
	})
	oa.gapTimeout = 20 * time.Millisecond
	oa.SetNextSeq(0, 1)

	oa.onGap = func(topic uint16, seq uint64) bool {
		return false
	}

	oa.Receive(0, 2, []byte("b"))

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(delivered) != 1 || delivered[0] != 2 {
		t.Fatalf("expected [2] after failed repair + skip, got %v", delivered)
	}
	if oa.NextSeq(0) != 3 {
		t.Fatalf("expected nextSeq=3, got %d", oa.NextSeq(0))
	}
}
