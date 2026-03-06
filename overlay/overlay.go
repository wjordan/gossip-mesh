// Package overlay provides the eager/lazy peer classification for the
// Plumtree-inspired gossip dissemination protocol. Peers are classified
// as eager (low RTT, same-DC — immediate QUIC datagram forward) or lazy
// (high RTT, cross-region — batched QUIC stream forward) based on Vivaldi
// coordinate distances.
package overlay

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// rttGapThreshold is the minimum RTT gap considered significant for
// distinguishing between RTT clusters (e.g., same-DC vs cross-region).
const rttGapThreshold = 5 * time.Millisecond

// OverlayConfig tunes the eager/lazy peer classification.
type OverlayConfig struct {
	MinEagerPeers int // default 4
	MaxEagerPeers int // default 8
	MinLazyPeers  int // default 2
	MaxLazyPeers  int // default 6
}

func (c *OverlayConfig) withDefaults() {
	if c.MinEagerPeers <= 0 {
		c.MinEagerPeers = 4
	}
	if c.MaxEagerPeers <= 0 {
		c.MaxEagerPeers = 8
	}
	if c.MinLazyPeers <= 0 {
		c.MinLazyPeers = 2
	}
	if c.MaxLazyPeers <= 0 {
		c.MaxLazyPeers = 6
	}
}

// PeerInfo is the minimal peer information needed for overlay classification.
type PeerInfo struct {
	NodeID string
	Addr   string // QUIC address (host:port)
	RTT    time.Duration
}

// OverlayPeer is a classified peer in the overlay.
type OverlayPeer struct {
	NodeID  string
	Addr    string
	RTT     time.Duration
	IsEager bool
}

// Overlay classifies peers into eager and lazy sets based on RTT.
type Overlay struct {
	mu   sync.RWMutex
	cfg  OverlayConfig
	self string

	eagerPeers []OverlayPeer
	lazyPeers  []OverlayPeer
}

// New creates a new Overlay with the given config and local node ID.
func New(selfNodeID string, cfg OverlayConfig) *Overlay {
	cfg.withDefaults()
	return &Overlay{
		cfg:  cfg,
		self: selfNodeID,
	}
}

// Reclassify re-sorts peers into eager/lazy based on RTT.
// Called on membership change or periodic RTT update.
func (o *Overlay) Reclassify(peers []PeerInfo) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Filter out self.
	filtered := make([]PeerInfo, 0, len(peers))
	for _, p := range peers {
		if p.NodeID != o.self {
			filtered = append(filtered, p)
		}
	}
	peers = filtered

	if len(peers) == 0 {
		o.eagerPeers = nil
		o.lazyPeers = nil
		return
	}

	// Sort by RTT.
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].RTT < peers[j].RTT
	})

	// Cap total peer count, preserving cross-region diversity.
	// Without cluster-aware selection, the lowest-RTT truncation
	// drops all cross-region peers when many same-region peers exist.
	totalMax := o.cfg.MaxEagerPeers + o.cfg.MaxLazyPeers
	if len(peers) > totalMax {
		peers = selectDiversePeers(peers, totalMax)
	}

	// Classify peers as eager/lazy with cross-region diversity.
	isEager := o.selectEager(peers)

	// Build classified peer lists.
	eagerCount := 0
	for _, e := range isEager {
		if e {
			eagerCount++
		}
	}
	o.eagerPeers = make([]OverlayPeer, 0, eagerCount)
	o.lazyPeers = make([]OverlayPeer, 0, len(peers)-eagerCount)
	for i, p := range peers {
		op := OverlayPeer{
			NodeID:  p.NodeID,
			Addr:    p.Addr,
			RTT:     p.RTT,
			IsEager: isEager[i],
		}
		if isEager[i] {
			o.eagerPeers = append(o.eagerPeers, op)
		} else {
			o.lazyPeers = append(o.lazyPeers, op)
		}
	}
}

// selectDiversePeers selects totalMax peers from an RTT-sorted list,
// ensuring at least one peer per RTT cluster survives the cut.
// This prevents large same-region populations from drowning out
// cross-region peers at scale (e.g. 200 same-region peers would
// otherwise fill all 14 slots before any cross-region peer is seen).
func selectDiversePeers(peers []PeerInfo, totalMax int) []PeerInfo {
	clusters := findRTTClusters(peers)
	if len(clusters) <= 1 {
		return peers[:totalMax]
	}

	selected := make([]bool, len(peers))
	count := 0

	// Reserve one slot per cluster, sampled evenly if more clusters than budget.
	reserved := totalMax
	if reserved > len(clusters) {
		reserved = len(clusters)
	}
	for i := range reserved {
		ci := i * len(clusters) / reserved
		selected[clusters[ci][0]] = true
		count++
	}

	// Fill remaining slots by lowest RTT.
	for i := range peers {
		if count >= totalMax {
			break
		}
		if !selected[i] {
			selected[i] = true
			count++
		}
	}

	// Build result preserving RTT sort order.
	result := make([]PeerInfo, 0, count)
	for i, p := range peers {
		if selected[i] {
			result = append(result, p)
		}
	}
	return result
}

// selectEager determines which peers should be eager, ensuring at least one
// eager peer per RTT cluster for cross-region gossip tree connectivity.
func (o *Overlay) selectEager(peers []PeerInfo) []bool {
	isEager := make([]bool, len(peers))

	clusters := findRTTClusters(peers)

	// Single cluster (all same-DC): use classic gap-based split.
	if len(clusters) <= 1 {
		gapIdx := o.findRTTGap(peers)
		if gapIdx < o.cfg.MinEagerPeers {
			gapIdx = o.cfg.MinEagerPeers
		}
		if gapIdx > o.cfg.MaxEagerPeers {
			gapIdx = o.cfg.MaxEagerPeers
		}
		// Cap to available peers before enforcing MinLazyPeers.
		if gapIdx > len(peers) {
			gapIdx = len(peers)
		}
		// Enforce MinLazyPeers only when there are enough total peers
		// for both constraints. In small clusters (≤ MinEager+MinLazy),
		// prefer eager delivery for lower latency.
		if len(peers)-gapIdx < o.cfg.MinLazyPeers && len(peers) > o.cfg.MinEagerPeers+o.cfg.MinLazyPeers {
			gapIdx = len(peers) - o.cfg.MinLazyPeers
		}
		if gapIdx < 0 {
			gapIdx = 0
		}
		for i := 0; i < gapIdx; i++ {
			isEager[i] = true
		}
		return isEager
	}

	// Multiple clusters: ensure eager peers are spread across the RTT
	// range so the gossip tree has cross-region connectivity. When there
	// are more clusters than eager slots, sample evenly across the full
	// range rather than greedily filling the nearest clusters — this
	// ensures distant regions are reachable in bounded hops.
	maxEager := o.cfg.MaxEagerPeers
	if lazyBudget := len(peers) - o.cfg.MinLazyPeers; lazyBudget < maxEager {
		maxEager = lazyBudget
	}
	if maxEager < 0 {
		maxEager = 0
	}

	eagerCount := 0

	// Phase 1: reserve 1 eager slot per cluster, sampled evenly when
	// there are more clusters than budget. E.g. 30 clusters / 8 budget
	// → stride 3.75 → picks clusters 0, 3, 7, 11, 15, 18, 22, 26.
	// The nearest cluster (index 0) is always included.
	reserved := maxEager
	if reserved > len(clusters) {
		reserved = len(clusters)
	}
	for i := range reserved {
		// Map [0..reserved) evenly onto [0..len(clusters)).
		ci := i * len(clusters) / reserved
		isEager[clusters[ci][0]] = true
		eagerCount++
	}

	// Phase 2: fill remaining eager slots by lowest RTT.
	for i := range peers {
		if eagerCount >= maxEager {
			break
		}
		if !isEager[i] {
			isEager[i] = true
			eagerCount++
		}
	}

	return isEager
}

// findRTTClusters groups RTT-sorted peers into clusters separated by
// gaps larger than rttGapThreshold. Each cluster is a slice of peer indices.
func findRTTClusters(peers []PeerInfo) [][]int {
	if len(peers) == 0 {
		return nil
	}
	clusters := [][]int{{0}}
	for i := 1; i < len(peers); i++ {
		if peers[i].RTT-peers[i-1].RTT > rttGapThreshold {
			clusters = append(clusters, []int{i})
		} else {
			clusters[len(clusters)-1] = append(clusters[len(clusters)-1], i)
		}
	}
	return clusters
}

// findRTTGap finds the largest gap in sorted RTT list.
// Used only for the single-cluster (single-DC) case.
func (o *Overlay) findRTTGap(peers []PeerInfo) int {
	if len(peers) <= o.cfg.MinEagerPeers+o.cfg.MinLazyPeers {
		return min(o.cfg.MinEagerPeers, len(peers))
	}

	maxGap := time.Duration(0)
	gapIdx := o.cfg.MinEagerPeers

	lo := o.cfg.MinEagerPeers
	if lo < 1 {
		lo = 1
	}
	hi := len(peers) - o.cfg.MinLazyPeers
	if hi > len(peers) {
		hi = len(peers)
	}

	for i := lo; i < hi; i++ {
		gap := peers[i].RTT - peers[i-1].RTT
		if gap > maxGap {
			maxGap = gap
			gapIdx = i
		}
	}

	// If no significant gap found (all same datacenter), use maxEagerPeers.
	if maxGap < rttGapThreshold {
		if len(peers) <= o.cfg.MaxEagerPeers {
			return len(peers) // all eager in single-DC deployments
		}
		return o.cfg.MaxEagerPeers
	}

	return gapIdx
}

// EagerPeers returns a snapshot of eager peers.
func (o *Overlay) EagerPeers() []OverlayPeer {
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make([]OverlayPeer, len(o.eagerPeers))
	copy(out, o.eagerPeers)
	return out
}

// LazyPeers returns a snapshot of lazy peers.
func (o *Overlay) LazyPeers() []OverlayPeer {
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make([]OverlayPeer, len(o.lazyPeers))
	copy(out, o.lazyPeers)
	return out
}

// AllPeers returns a snapshot of all classified peers (eager then lazy).
func (o *Overlay) AllPeers() []OverlayPeer {
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make([]OverlayPeer, 0, len(o.eagerPeers)+len(o.lazyPeers))
	out = append(out, o.eagerPeers...)
	out = append(out, o.lazyPeers...)
	return out
}

// RandomPeers returns up to n random peers from the full set.
// Used by RepairManager to select peers for gap repair.
func (o *Overlay) RandomPeers(n int) []OverlayPeer {
	all := o.AllPeers()
	if len(all) <= n {
		return all
	}
	// Simple selection: take first n from a shuffled list.
	// Since the list is already sorted by RTT, taking the first n
	// biases towards lower-RTT peers, which is desirable for repair.
	return all[:n]
}

// PeerCount returns the total number of classified peers.
func (o *Overlay) PeerCount() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return len(o.eagerPeers) + len(o.lazyPeers)
}

// Summary returns a human-readable summary of the overlay state.
func (o *Overlay) Summary() string {
	o.mu.RLock()
	defer o.mu.RUnlock()

	var parts []string
	for _, p := range o.eagerPeers {
		parts = append(parts, fmt.Sprintf("%s(eager,%.0fms)", p.NodeID, float64(p.RTT)/float64(time.Millisecond)))
	}
	for _, p := range o.lazyPeers {
		parts = append(parts, fmt.Sprintf("%s(lazy,%.0fms)", p.NodeID, float64(p.RTT)/float64(time.Millisecond)))
	}
	if len(parts) == 0 {
		return "overlay: no peers"
	}
	return fmt.Sprintf("overlay: eager=%d lazy=%d [%s]",
		len(o.eagerPeers), len(o.lazyPeers), strings.Join(parts, " "))
}
