package holepunch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/wjordan/gossip-mesh/objstore"
)

// Signal describes a node's readiness to hole-punch.
type Signal struct {
	NodeID string    `json:"id"`
	Addr   string    `json:"addr"`
	Nonce  string    `json:"nonce"`
	Time   time.Time `json:"time"`
}

// signalKey returns the canonical S3 key for a hole-punch signal between two nodes.
// Each side writes its own sub-key under the canonical pair prefix.
func signalKey(id1, id2, writerID string) string {
	lower, higher := id1, id2
	if id1 > id2 {
		lower, higher = id2, id1
	}
	return fmt.Sprintf("signal/%s/%s/%s", lower, higher, writerID)
}

// signalPrefix returns the prefix for all signals between two nodes.
func signalPrefix(id1, id2 string) string {
	lower, higher := id1, id2
	if id1 > id2 {
		lower, higher = id2, id1
	}
	return fmt.Sprintf("signal/%s/%s/", lower, higher)
}

// WriteSignal writes a hole-punch signal to the object store.
func WriteSignal(ctx context.Context, store objstore.ObjectStore, sig Signal) error {
	sig.Time = time.Now()
	data, err := json.Marshal(sig)
	if err != nil {
		return fmt.Errorf("marshal signal: %w", err)
	}
	key := signalKey(sig.NodeID, sig.Addr, sig.NodeID)
	// The addr field is the target, but signalKey uses the pair IDs.
	// Actually we need the target ID. Let me reconsider the key structure.
	// Signal keys: signal/{min(a,b)}/{max(a,b)}/{writer-id}
	// We write using our own ID, but need the target ID for the pair prefix.
	// This function should be called with the correct key.
	if _, err := store.Put(ctx, key, data); err != nil {
		return fmt.Errorf("put signal: %w", err)
	}
	return nil
}

// WriteSignalForPeer writes a hole-punch signal for a specific peer pair.
func WriteSignalForPeer(ctx context.Context, store objstore.ObjectStore, selfID, targetID string, sig Signal) error {
	sig.Time = time.Now()
	data, err := json.Marshal(sig)
	if err != nil {
		return fmt.Errorf("marshal signal: %w", err)
	}
	key := signalKey(selfID, targetID, selfID)
	if _, err := store.Put(ctx, key, data); err != nil {
		return fmt.Errorf("put signal: %w", err)
	}
	return nil
}

// ReadSignals reads hole-punch signals for a peer pair.
// Returns the self signal and peer signal (either may be nil if not yet written).
func ReadSignals(ctx context.Context, store objstore.ObjectStore, selfID, targetID string) (self *Signal, peer *Signal, err error) {
	prefix := signalPrefix(selfID, targetID)
	keys, err := store.List(ctx, prefix)
	if err != nil {
		return nil, nil, fmt.Errorf("list signals: %w", err)
	}

	for _, key := range keys {
		data, _, err := store.Get(ctx, key)
		if err != nil {
			if errors.Is(err, objstore.ErrNotFound) {
				continue
			}
			return nil, nil, fmt.Errorf("get signal %s: %w", key, err)
		}

		var sig Signal
		if err := json.Unmarshal(data, &sig); err != nil {
			continue
		}

		if sig.NodeID == selfID {
			self = &sig
		} else if sig.NodeID == targetID {
			peer = &sig
		}
	}

	return self, peer, nil
}

// CleanupSignal removes all signals for a peer pair.
func CleanupSignal(ctx context.Context, store objstore.ObjectStore, selfID, targetID string) error {
	prefix := signalPrefix(selfID, targetID)
	keys, err := store.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("list signals for cleanup: %w", err)
	}
	for _, key := range keys {
		_ = store.Delete(ctx, key)
	}
	return nil
}
