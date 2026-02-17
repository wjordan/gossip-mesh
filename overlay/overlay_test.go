package overlay

import (
	"fmt"
	"testing"
	"time"
)

func TestOverlay_Reclassify_BimodalRTT(t *testing.T) {
	o := New("self", OverlayConfig{
		MinEagerPeers: 2,
		MaxEagerPeers: 6,
		MinLazyPeers:  2,
		MaxLazyPeers:  6,
	})

	// Bimodal: 4 same-DC peers (1-4ms), 4 cross-region peers (50-53ms).
	peers := []PeerInfo{
		{NodeID: "a", RTT: 1 * time.Millisecond},
		{NodeID: "b", RTT: 2 * time.Millisecond},
		{NodeID: "c", RTT: 3 * time.Millisecond},
		{NodeID: "d", RTT: 4 * time.Millisecond},
		{NodeID: "e", RTT: 50 * time.Millisecond},
		{NodeID: "f", RTT: 51 * time.Millisecond},
		{NodeID: "g", RTT: 52 * time.Millisecond},
		{NodeID: "h", RTT: 53 * time.Millisecond},
	}

	o.Reclassify(peers)

	eager := o.EagerPeers()
	lazy := o.LazyPeers()

	// With diversity: 1 reserved per cluster + 4 fill = 6 eager, 2 lazy.
	// Eager includes all 4 same-DC + 2 cross-region (cluster representative + fill).
	if len(eager) != 6 {
		t.Fatalf("expected 6 eager peers, got %d", len(eager))
	}
	if len(lazy) != 2 {
		t.Fatalf("expected 2 lazy peers, got %d", len(lazy))
	}

	// At least 1 cross-region eager peer (diversity guarantee).
	crossRegionEager := 0
	for _, p := range eager {
		if p.RTT > 10*time.Millisecond {
			crossRegionEager++
		}
		if !p.IsEager {
			t.Fatalf("eager peer %s has IsEager=false", p.NodeID)
		}
	}
	if crossRegionEager < 1 {
		t.Fatal("expected at least 1 cross-region eager peer")
	}

	for _, p := range lazy {
		if p.IsEager {
			t.Fatalf("lazy peer %s has IsEager=true", p.NodeID)
		}
	}
}

func TestOverlay_Reclassify_TrimodalRTT(t *testing.T) {
	o := New("self", OverlayConfig{
		MinEagerPeers: 2,
		MaxEagerPeers: 8,
		MinLazyPeers:  2,
		MaxLazyPeers:  6,
	})

	// Trimodal: US-East (1-4ms), EU-West (80-83ms), AP-SE (200-203ms).
	// This is the scenario that breaks the single-gap algorithm.
	peers := []PeerInfo{
		{NodeID: "us1", RTT: 1 * time.Millisecond},
		{NodeID: "us2", RTT: 2 * time.Millisecond},
		{NodeID: "us3", RTT: 3 * time.Millisecond},
		{NodeID: "us4", RTT: 4 * time.Millisecond},
		{NodeID: "eu1", RTT: 80 * time.Millisecond},
		{NodeID: "eu2", RTT: 81 * time.Millisecond},
		{NodeID: "eu3", RTT: 82 * time.Millisecond},
		{NodeID: "eu4", RTT: 83 * time.Millisecond},
		{NodeID: "ap1", RTT: 200 * time.Millisecond},
		{NodeID: "ap2", RTT: 201 * time.Millisecond},
		{NodeID: "ap3", RTT: 202 * time.Millisecond},
		{NodeID: "ap4", RTT: 203 * time.Millisecond},
	}

	o.Reclassify(peers)

	eager := o.EagerPeers()
	lazy := o.LazyPeers()

	// 14 total cap (8+6), but only 12 peers, so all included.
	// 3 clusters → 3 reserved + 5 fill = 8 eager, 4 lazy.
	if len(eager) != 8 {
		t.Fatalf("expected 8 eager peers, got %d", len(eager))
	}
	if len(lazy) != 4 {
		t.Fatalf("expected 4 lazy peers, got %d", len(lazy))
	}

	// Verify cross-region diversity: at least 1 eager from each region.
	regionCounts := map[string]int{"us": 0, "eu": 0, "ap": 0}
	for _, p := range eager {
		switch {
		case p.RTT < 10*time.Millisecond:
			regionCounts["us"]++
		case p.RTT < 100*time.Millisecond:
			regionCounts["eu"]++
		default:
			regionCounts["ap"]++
		}
	}

	for region, count := range regionCounts {
		if count < 1 {
			t.Fatalf("expected at least 1 eager peer in %s, got %d", region, count)
		}
	}
	t.Logf("eager distribution: US=%d EU=%d AP=%d", regionCounts["us"], regionCounts["eu"], regionCounts["ap"])
}

func TestOverlay_Reclassify_SingleDC(t *testing.T) {
	o := New("self", OverlayConfig{
		MinEagerPeers: 2,
		MaxEagerPeers: 6,
		MinLazyPeers:  2,
		MaxLazyPeers:  6,
	})

	// All same-DC: RTTs within 5ms of each other (no significant gap).
	peers := []PeerInfo{
		{NodeID: "a", RTT: 1 * time.Millisecond},
		{NodeID: "b", RTT: 2 * time.Millisecond},
		{NodeID: "c", RTT: 2 * time.Millisecond},
		{NodeID: "d", RTT: 3 * time.Millisecond},
		{NodeID: "e", RTT: 3 * time.Millisecond},
		{NodeID: "f", RTT: 4 * time.Millisecond},
		{NodeID: "g", RTT: 4 * time.Millisecond},
		{NodeID: "h", RTT: 5 * time.Millisecond},
	}

	o.Reclassify(peers)

	eager := o.EagerPeers()
	lazy := o.LazyPeers()

	// With no significant gap and 8 peers, should hit MaxEagerPeers=6.
	if len(eager) != 6 {
		t.Fatalf("expected 6 eager peers (maxEagerPeers), got %d", len(eager))
	}
	if len(lazy) != 2 {
		t.Fatalf("expected 2 lazy peers (remaining), got %d", len(lazy))
	}
}

func TestOverlay_Reclassify_FiltersSelf(t *testing.T) {
	o := New("self-node", OverlayConfig{})

	peers := []PeerInfo{
		{NodeID: "self-node", RTT: 0},
		{NodeID: "other", RTT: 1 * time.Millisecond},
	}

	o.Reclassify(peers)

	if o.PeerCount() != 1 {
		t.Fatalf("expected 1 peer (self filtered), got %d", o.PeerCount())
	}
}

func TestOverlay_Reclassify_Empty(t *testing.T) {
	o := New("self", OverlayConfig{})

	o.Reclassify(nil)

	if o.PeerCount() != 0 {
		t.Fatalf("expected 0 peers, got %d", o.PeerCount())
	}
	if len(o.EagerPeers()) != 0 {
		t.Fatal("expected no eager peers")
	}
	if len(o.LazyPeers()) != 0 {
		t.Fatal("expected no lazy peers")
	}
}

func TestOverlay_Reclassify_MinConstraints(t *testing.T) {
	o := New("self", OverlayConfig{
		MinEagerPeers: 3,
		MaxEagerPeers: 6,
		MinLazyPeers:  2,
		MaxLazyPeers:  6,
	})

	// 5 peers: with MinEager=3 and MinLazy=2, gap detection result
	// is constrained to ensure minimums are met.
	peers := make([]PeerInfo, 5)
	for i := range peers {
		peers[i] = PeerInfo{
			NodeID: fmt.Sprintf("n%d", i),
			RTT:    time.Duration(i+1) * time.Millisecond,
		}
	}

	o.Reclassify(peers)

	eager := o.EagerPeers()
	lazy := o.LazyPeers()

	if len(eager) < 3 {
		t.Fatalf("expected at least 3 eager (min), got %d", len(eager))
	}
	if len(lazy) < 2 {
		t.Fatalf("expected at least 2 lazy (min), got %d", len(lazy))
	}
}

func TestOverlay_Reclassify_MaxConstraints(t *testing.T) {
	o := New("self", OverlayConfig{
		MinEagerPeers: 2,
		MaxEagerPeers: 4,
		MinLazyPeers:  2,
		MaxLazyPeers:  4,
	})

	// 20 peers: should be capped to MaxEager + MaxLazy = 8.
	peers := make([]PeerInfo, 20)
	for i := range peers {
		peers[i] = PeerInfo{
			NodeID: fmt.Sprintf("n%d", i),
			RTT:    time.Duration(i+1) * time.Millisecond,
		}
	}

	o.Reclassify(peers)

	total := o.PeerCount()
	if total > 8 {
		t.Fatalf("expected at most 8 total peers, got %d", total)
	}
}

func TestOverlay_AllPeers(t *testing.T) {
	o := New("self", OverlayConfig{
		MinEagerPeers: 2,
		MaxEagerPeers: 4,
		MinLazyPeers:  2,
		MaxLazyPeers:  4,
	})

	peers := []PeerInfo{
		{NodeID: "a", RTT: 1 * time.Millisecond},
		{NodeID: "b", RTT: 2 * time.Millisecond},
		{NodeID: "c", RTT: 50 * time.Millisecond},
		{NodeID: "d", RTT: 51 * time.Millisecond},
	}

	o.Reclassify(peers)

	all := o.AllPeers()
	if len(all) != 4 {
		t.Fatalf("expected 4 peers, got %d", len(all))
	}

	// Eager first, then lazy.
	eagerCount := 0
	for _, p := range all {
		if p.IsEager {
			eagerCount++
		}
	}
	if eagerCount < 2 {
		t.Fatalf("expected at least 2 eager in AllPeers, got %d", eagerCount)
	}
}

func TestOverlay_RandomPeers(t *testing.T) {
	o := New("self", OverlayConfig{})

	peers := []PeerInfo{
		{NodeID: "a", RTT: 1 * time.Millisecond},
		{NodeID: "b", RTT: 2 * time.Millisecond},
		{NodeID: "c", RTT: 3 * time.Millisecond},
		{NodeID: "d", RTT: 4 * time.Millisecond},
		{NodeID: "e", RTT: 5 * time.Millisecond},
	}
	o.Reclassify(peers)

	// Request fewer than total.
	got := o.RandomPeers(2)
	if len(got) != 2 {
		t.Fatalf("expected 2 random peers, got %d", len(got))
	}

	// Request more than total.
	got = o.RandomPeers(100)
	if len(got) != o.PeerCount() {
		t.Fatalf("expected %d peers (all), got %d", o.PeerCount(), len(got))
	}
}

func TestFindRTTClusters(t *testing.T) {
	tests := []struct {
		name     string
		rtts     []time.Duration
		wantN    int // number of clusters
	}{
		{
			name:  "empty",
			rtts:  nil,
			wantN: 0,
		},
		{
			name:  "single_dc",
			rtts:  []time.Duration{1, 2, 3, 4},
			wantN: 1,
		},
		{
			name:  "bimodal",
			rtts:  []time.Duration{1, 2, 3, 50, 51, 52},
			wantN: 2,
		},
		{
			name:  "trimodal",
			rtts:  []time.Duration{1, 2, 80, 81, 200, 201},
			wantN: 3,
		},
		{
			name:  "four_regions",
			rtts:  []time.Duration{1, 2, 30, 31, 100, 101, 250, 251},
			wantN: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peers := make([]PeerInfo, len(tt.rtts))
			for i, rtt := range tt.rtts {
				peers[i] = PeerInfo{
					NodeID: fmt.Sprintf("n%d", i),
					RTT:    rtt * time.Millisecond,
				}
			}
			clusters := findRTTClusters(peers)
			if len(clusters) != tt.wantN {
				t.Fatalf("expected %d clusters, got %d", tt.wantN, len(clusters))
			}
		})
	}
}

func TestOverlay_Reclassify_FiveRegions(t *testing.T) {
	o := New("self", OverlayConfig{
		MinEagerPeers: 2,
		MaxEagerPeers: 8,
		MinLazyPeers:  2,
		MaxLazyPeers:  6,
	})

	// 5 regions, 3 peers each = 15 total (capped to 14).
	// All 5 clusters should get at least 1 eager.
	peers := []PeerInfo{
		{NodeID: "r1a", RTT: 1 * time.Millisecond},
		{NodeID: "r1b", RTT: 2 * time.Millisecond},
		{NodeID: "r1c", RTT: 3 * time.Millisecond},
		{NodeID: "r2a", RTT: 30 * time.Millisecond},
		{NodeID: "r2b", RTT: 31 * time.Millisecond},
		{NodeID: "r2c", RTT: 32 * time.Millisecond},
		{NodeID: "r3a", RTT: 80 * time.Millisecond},
		{NodeID: "r3b", RTT: 81 * time.Millisecond},
		{NodeID: "r3c", RTT: 82 * time.Millisecond},
		{NodeID: "r4a", RTT: 150 * time.Millisecond},
		{NodeID: "r4b", RTT: 151 * time.Millisecond},
		{NodeID: "r4c", RTT: 152 * time.Millisecond},
		{NodeID: "r5a", RTT: 250 * time.Millisecond},
		{NodeID: "r5b", RTT: 251 * time.Millisecond},
		{NodeID: "r5c", RTT: 252 * time.Millisecond},
	}

	o.Reclassify(peers)

	eager := o.EagerPeers()
	lazy := o.LazyPeers()

	t.Logf("eager=%d lazy=%d total=%d", len(eager), len(lazy), o.PeerCount())

	// Verify each region has at least 1 eager peer.
	regionBounds := []struct {
		name string
		lo   time.Duration
		hi   time.Duration
	}{
		{"r1", 0, 10 * time.Millisecond},
		{"r2", 20 * time.Millisecond, 40 * time.Millisecond},
		{"r3", 70 * time.Millisecond, 90 * time.Millisecond},
		{"r4", 140 * time.Millisecond, 160 * time.Millisecond},
		{"r5", 240 * time.Millisecond, 260 * time.Millisecond},
	}

	for _, rb := range regionBounds {
		found := false
		for _, p := range eager {
			if p.RTT >= rb.lo && p.RTT <= rb.hi {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no eager peer in region %s", rb.name)
		}
	}
}

func TestOverlay_Reclassify_ScaleCrossRegion(t *testing.T) {
	o := New("self", OverlayConfig{})

	// Simulate scale scenario: 200 same-region peers (1-3ms RTT) plus
	// 4 cross-region peers (50-200ms). Before the fix, peers[:14] took
	// only the 200 same-region peers and dropped all cross-region.
	var peers []PeerInfo
	for i := 0; i < 200; i++ {
		peers = append(peers, PeerInfo{
			NodeID: fmt.Sprintf("local-%d", i),
			RTT:    time.Duration(1+i%3) * time.Millisecond,
		})
	}
	crossRegionRTTs := []time.Duration{50, 80, 150, 200}
	for i, rtt := range crossRegionRTTs {
		peers = append(peers, PeerInfo{
			NodeID: fmt.Sprintf("remote-%d", i),
			RTT:    rtt * time.Millisecond,
		})
	}

	o.Reclassify(peers)

	all := o.AllPeers()

	// Cross-region peers must survive the totalMax cap.
	crossRegion := 0
	for _, p := range all {
		if p.RTT > 10*time.Millisecond {
			crossRegion++
		}
	}
	if crossRegion == 0 {
		t.Fatal("no cross-region peers in overlay — totalMax cap dropped them all")
	}
	t.Logf("cross-region peers in overlay: %d/%d", crossRegion, len(crossRegionRTTs))

	// At least 1 cross-region peer must be eager for gossip to cross regions.
	eagerCross := 0
	for _, p := range o.EagerPeers() {
		if p.RTT > 10*time.Millisecond {
			eagerCross++
		}
	}
	if eagerCross == 0 {
		t.Fatal("no cross-region eager peers — gossip won't cross regions")
	}
}

func TestOverlay_Reclassify_ManyRegions(t *testing.T) {
	o := New("self", OverlayConfig{
		MinEagerPeers: 2,
		MaxEagerPeers: 8,
		MinLazyPeers:  2,
		MaxLazyPeers:  6,
	})

	// 30 regions at 10ms spacing. Total peer cap = 14, so only 14 of 30
	// peers survive the cap. With 8 eager slots across 14 clusters,
	// the even-sampling should spread across the RTT range rather than
	// greedily filling only the nearest clusters.
	peers := make([]PeerInfo, 30)
	for i := range peers {
		peers[i] = PeerInfo{
			NodeID: fmt.Sprintf("r%02d", i),
			RTT:    time.Duration(i*10+1) * time.Millisecond,
		}
	}

	o.Reclassify(peers)

	eager := o.EagerPeers()
	lazy := o.LazyPeers()

	t.Logf("eager=%d lazy=%d total=%d", len(eager), len(lazy), o.PeerCount())
	for _, p := range eager {
		t.Logf("  eager: %s RTT=%v", p.NodeID, p.RTT)
	}

	if len(eager) != 8 {
		t.Fatalf("expected 8 eager peers, got %d", len(eager))
	}
	if len(lazy) != 6 {
		t.Fatalf("expected 6 lazy peers, got %d", len(lazy))
	}

	// The farthest eager peer should be from the distant end of the range,
	// not just the 8 nearest clusters. After total cap to 14, peers have
	// RTTs 1ms..131ms. Eager should include at least one peer > 100ms.
	maxEagerRTT := time.Duration(0)
	for _, p := range eager {
		if p.RTT > maxEagerRTT {
			maxEagerRTT = p.RTT
		}
	}
	if maxEagerRTT < 100*time.Millisecond {
		t.Errorf("most distant eager peer RTT=%v, expected >100ms for range coverage", maxEagerRTT)
	}
}
