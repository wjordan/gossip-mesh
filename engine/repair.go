package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/wjordan/gossip-mesh/overlay"
	"github.com/wjordan/gossip-mesh/transport"
)

const maxRepairCache = 1024

// RepairManager handles gap repair by pulling missing entries from peers
// and serving repair requests from its local entry cache.
type RepairManager struct {
	overlay   *overlay.Overlay
	transport *transport.Transport
	applier   *OrderedApplier

	cacheMu sync.RWMutex
	cache   map[TopicSeq][]byte
}

// NewRepairManager creates a RepairManager.
func NewRepairManager(o *overlay.Overlay, t *transport.Transport, applier *OrderedApplier) *RepairManager {
	return &RepairManager{
		overlay:   o,
		transport: t,
		applier:   applier,
		cache:     make(map[TopicSeq][]byte),
	}
}

// CacheEntry stores an entry so it can be served to peers requesting repair.
func (r *RepairManager) CacheEntry(entry GossipEntry) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	if len(r.cache) >= maxRepairCache {
		// Evict one arbitrary entry.
		for k := range r.cache {
			delete(r.cache, k)
			break
		}
	}
	buf := make([]byte, len(entry.Payload))
	copy(buf, entry.Payload)
	r.cache[TopicSeq{entry.Topic, entry.Seq}] = buf
}

// HandleRequest serves an inbound repair request. The stream carries
// [topic(2B)][seq(8B)] (type byte already consumed by transport dispatch)
// and the response is [status(1B)][payload if hit].
func (r *RepairManager) HandleRequest(from string, stream io.ReadWriteCloser) {
	defer stream.Close()

	var req [10]byte
	if _, err := io.ReadFull(stream, req[:]); err != nil {
		return
	}
	topic := binary.BigEndian.Uint16(req[:2])
	seq := binary.BigEndian.Uint64(req[2:])

	r.cacheMu.RLock()
	payload, ok := r.cache[TopicSeq{topic, seq}]
	r.cacheMu.RUnlock()

	if !ok {
		stream.Write([]byte{0x00}) // miss
		return
	}
	if _, err := stream.Write([]byte{0x01}); err != nil {
		return
	}
	stream.Write(payload)
}

// RequestRepair attempts to fetch a missing gossip entry from random peers.
// Returns true if the entry was found and delivered.
func (r *RepairManager) RequestRepair(topic uint16, missingSeq uint64) bool {
	peers := r.overlay.RandomPeers(3)

	for _, peer := range peers {
		entry, err := r.pullFromPeer(peer, topic, missingSeq)
		if err != nil {
			continue
		}
		if entry != nil {
			r.applier.Receive(entry.Topic, entry.Seq, entry.Payload)
			return true
		}
	}

	return false
}

// pullFromPeer requests a specific entry from a peer via QUIC stream.
func (r *RepairManager) pullFromPeer(peer overlay.OverlayPeer, topic uint16, seq uint64) (*GossipEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream, err := r.transport.OpenStream(ctx, peer.Addr)
	if err != nil {
		return nil, fmt.Errorf("open repair stream to %s: %w", peer.NodeID, err)
	}
	defer stream.Close()

	// Write: [streamTypeRepair][topic][seq]
	var req [11]byte
	req[0] = transport.StreamTypeRepair
	binary.BigEndian.PutUint16(req[1:], topic)
	binary.BigEndian.PutUint64(req[3:], seq)
	if _, err := stream.Write(req[:]); err != nil {
		return nil, err
	}

	// Read: [1B status][payload if hit]
	var status [1]byte
	if _, err := io.ReadFull(stream, status[:]); err != nil {
		return nil, err
	}
	if status[0] == 0x00 {
		return nil, nil // miss
	}

	payload, err := io.ReadAll(stream)
	if err != nil {
		return nil, err
	}

	return &GossipEntry{Topic: topic, Seq: seq, Payload: payload}, nil
}
