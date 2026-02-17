package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/wjordan/gossip-mesh/overlay"
	"github.com/wjordan/gossip-mesh/transport"
)

// RepairManager handles gap repair by pulling missing entries from peers.
type RepairManager struct {
	overlay   *overlay.Overlay
	transport *transport.Transport
	applier   *OrderedApplier
}

// NewRepairManager creates a RepairManager.
func NewRepairManager(o *overlay.Overlay, t *transport.Transport, applier *OrderedApplier) *RepairManager {
	return &RepairManager{
		overlay:   o,
		transport: t,
		applier:   applier,
	}
}

// RequestRepair attempts to fetch a missing gossip entry from random peers.
func (r *RepairManager) RequestRepair(topic uint16, missingSeq uint64) {
	peers := r.overlay.RandomPeers(3)

	for _, peer := range peers {
		entry, err := r.pullFromPeer(peer, topic, missingSeq)
		if err != nil {
			continue
		}
		if entry != nil {
			r.applier.Receive(entry.Topic, entry.Seq, entry.Payload)
			return
		}
	}

	// Repair failed — the SyncEngine's existing S3 poll fallback
	// will handle this: stale cache entries are evicted, pages
	// re-fault on demand.
}

// pullFromPeer requests a specific entry from a peer via QUIC stream.
func (r *RepairManager) pullFromPeer(peer overlay.OverlayPeer, topic uint16, seq uint64) (*GossipEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s, err := r.transport.OpenStream(ctx, peer.Addr)
	if err != nil {
		return nil, fmt.Errorf("open repair stream to %s: %w", peer.NodeID, err)
	}
	defer s.Close()
	stream := s

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
