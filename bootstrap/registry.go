package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/wjordan/gossip-mesh/objstore"
)

const nodePrefix = "nodes/"

// NodeRegistration describes a node's presence in the cluster.
type NodeRegistration struct {
	NodeID     string    `json:"id"`
	Addrs      []string  `json:"addrs"`       // bind + public addresses
	PublicAddr string    `json:"public_addr"`  // reflected public addr (may be empty)
	IsRelay    bool      `json:"is_relay"`     // node has public IP, can relay
	Heartbeat  time.Time `json:"heartbeat"`
}

// Register writes the node registration to the object store.
func Register(ctx context.Context, store objstore.ObjectStore, reg NodeRegistration) error {
	reg.Heartbeat = time.Now()
	data, err := json.Marshal(reg)
	if err != nil {
		return fmt.Errorf("marshal registration: %w", err)
	}
	key := nodePrefix + reg.NodeID
	if _, err := store.Put(ctx, key, data); err != nil {
		return fmt.Errorf("put registration: %w", err)
	}
	return nil
}

// Deregister removes the node registration from the object store.
func Deregister(ctx context.Context, store objstore.ObjectStore, nodeID string) error {
	return store.Delete(ctx, nodePrefix+nodeID)
}

// DiscoverPeers lists all registered nodes and returns those with a heartbeat
// newer than maxAge. Stale entries are cleaned up.
func DiscoverPeers(ctx context.Context, store objstore.ObjectStore, maxAge time.Duration) ([]NodeRegistration, error) {
	keys, err := store.List(ctx, nodePrefix)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	now := time.Now()
	var peers []NodeRegistration

	for _, key := range keys {
		data, _, err := store.Get(ctx, key)
		if err != nil {
			if errors.Is(err, objstore.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("get node %s: %w", key, err)
		}

		var reg NodeRegistration
		if err := json.Unmarshal(data, &reg); err != nil {
			continue // skip malformed entries
		}

		if now.Sub(reg.Heartbeat) > maxAge {
			// Clean up stale entry.
			nodeID := strings.TrimPrefix(key, nodePrefix)
			_ = store.Delete(ctx, nodePrefix+nodeID)
			continue
		}

		peers = append(peers, reg)
	}

	return peers, nil
}

// HeartbeatLoop periodically re-registers the node to keep its heartbeat fresh.
// It blocks until ctx is cancelled.
func HeartbeatLoop(ctx context.Context, store objstore.ObjectStore, reg NodeRegistration, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = Register(ctx, store, reg)
		}
	}
}
