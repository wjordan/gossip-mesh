package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/wjordan/gossip-mesh/objstore"
)

const nodePrefix = "nodes/"

// NodeRegistration describes a node's presence in the cluster.
type NodeRegistration struct {
	NodeID     string   `json:"id"`
	Addrs      []string `json:"addrs"`       // bind + public addresses
	PublicAddr string   `json:"public_addr"`  // reflected public addr (may be empty)
	IsRelay    bool     `json:"is_relay"`     // node has public IP, can relay
	RegisteredAt time.Time `json:"registered_at"`
}

// Register writes the node registration to the object store.
func Register(ctx context.Context, store objstore.ObjectStore, reg NodeRegistration) error {
	if reg.RegisteredAt.IsZero() {
		reg.RegisteredAt = time.Now()
	}
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

// DiscoverPeers lists all registered nodes and returns their registrations.
func DiscoverPeers(ctx context.Context, store objstore.ObjectStore) ([]NodeRegistration, error) {
	keys, err := store.List(ctx, nodePrefix)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

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

		peers = append(peers, reg)
	}

	return peers, nil
}
