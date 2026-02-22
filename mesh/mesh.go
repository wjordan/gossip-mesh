// Package mesh provides a unified entry point for joining a gossip-mesh cluster.
//
// Seeds and gossip are the primary discovery mechanism. S3 is a side-channel
// for situations where the mesh can't self-heal: cold starts with zero known
// peers, partitioned NATs, or peers behind different NATs with no shared
// gossip neighbor. Both can be used together — seeds provide the fast path,
// S3 fills in what seeds can't reach.
package mesh

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/wjordan/gossip-mesh/bootstrap"
	"github.com/wjordan/gossip-mesh/engine"
	"github.com/wjordan/gossip-mesh/holepunch"
	"github.com/wjordan/gossip-mesh/membership"
	"github.com/wjordan/gossip-mesh/natutil"
	"github.com/wjordan/gossip-mesh/objstore"
	"github.com/wjordan/gossip-mesh/overlay"
	"github.com/wjordan/gossip-mesh/relay"
	"github.com/wjordan/gossip-mesh/transport"
)

// Config configures the mesh.
type Config struct {
	NodeID    string // generated if empty
	BindAddr  string
	BindPort  int // memberlist port; app = BindPort+1

	// ObjectStore enables S3-coordinated bootstrap (TLS, peer discovery,
	// NAT traversal). Can be combined with SeedAddrs — seeds provide the
	// fast path, S3 discovers peers that seeds can't reach.
	ObjectStore objstore.ObjectStore

	// SeedAddrs are memberlist addresses to join on startup. Always used
	// when provided, regardless of whether ObjectStore is set.
	SeedAddrs []string

	// TLS is the TLS config for direct connectivity mode. When ObjectStore
	// is set, TLS is bootstrapped from the object store and this field is
	// ignored.
	TLS *tls.Config

	AdvertiseAddr string
	AdvertisePort int
	Meta          membership.NodeMeta
	Logger        *log.Logger

	Overlay overlay.OverlayConfig
	Engine  engine.EngineConfig

	HeartbeatInterval       time.Duration // default 30s
	StaleNodeAge            time.Duration // default 2min
	SteadyStatePollInterval time.Duration // default 60s
}

func (c *Config) withDefaults() {
	if c.Logger == nil {
		c.Logger = log.Default()
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 30 * time.Second
	}
	if c.StaleNodeAge == 0 {
		c.StaleNodeAge = 2 * time.Minute
	}
	if c.SteadyStatePollInterval == 0 {
		c.SteadyStatePollInterval = 60 * time.Second
	}
	if c.NodeID == "" {
		c.NodeID = generateNodeID()
	}
}

// Mesh holds all components of a running gossip-mesh node.
type Mesh struct {
	Membership *membership.Membership
	Transport  *transport.Transport
	Overlay    *overlay.Overlay
	Engine     *engine.GossipEngine

	cfg         Config
	store       objstore.ObjectStore
	reg         *bootstrap.NodeRegistration
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	relayServer *relay.Server
	logger      *log.Logger
}

// Join starts a gossip-mesh node. Seeds are always joined immediately when
// provided. When ObjectStore is also set, S3-based discovery runs in parallel
// to find additional peers that seeds can't reach (cross-NAT, fresh clusters).
func Join(cfg Config) (*Mesh, error) {
	cfg.withDefaults()

	m := &Mesh{
		Membership: &membership.Membership{},
		cfg:        cfg,
		store:      cfg.ObjectStore,
		logger:     cfg.Logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	var tlsConfig *tls.Config

	if cfg.ObjectStore != nil {
		// Bootstrap TLS from object store.
		bindIP := net.ParseIP(cfg.BindAddr)
		ips := []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
		if bindIP != nil && !bindIP.IsLoopback() && !bindIP.IsUnspecified() {
			ips = append(ips, bindIP)
		}

		var err error
		tlsConfig, err = bootstrap.SetupClusterTLS(ctx, cfg.ObjectStore, cfg.NodeID, ips)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("setup cluster TLS: %w", err)
		}
	} else {
		tlsConfig = cfg.TLS
	}

	if tlsConfig == nil {
		cancel()
		return nil, fmt.Errorf("mesh: TLS config required (provide ObjectStore or TLS)")
	}

	// Start membership layer. Always pass seeds — they're the fast path.
	memCfg := membership.MembershipConfig{
		NodeID:        cfg.NodeID,
		BindAddr:      cfg.BindAddr,
		BindPort:      cfg.BindPort,
		AdvertiseAddr: cfg.AdvertiseAddr,
		AdvertisePort: cfg.AdvertisePort,
		SeedAddrs:     cfg.SeedAddrs,
		Meta:          cfg.Meta,
		TLS:           tlsConfig,
	}

	if err := m.Membership.Start(memCfg); err != nil {
		cancel()
		return nil, fmt.Errorf("start membership: %w", err)
	}

	// Start app transport.
	appPort := cfg.BindPort + 1
	t, err := transport.New(transport.Config{
		BindAddr:       cfg.BindAddr,
		BindPort:       appPort,
		TLS:            tlsConfig,
		MemberlistPool: m.Membership.ConnPool(),
		Logger:         cfg.Logger,
	})
	if err != nil {
		m.Membership.Stop()
		cancel()
		return nil, fmt.Errorf("start transport: %w", err)
	}
	m.Transport = t

	// Register address reflection handler (used by both modes).
	natutil.RegisterReflectHandler(t)

	// Start overlay and engine before S3 discovery so that peers connected
	// via seeds are immediately usable for gossip.
	m.Overlay = overlay.New(cfg.NodeID, cfg.Overlay)
	m.Engine = engine.New(m.Overlay, t, cfg.Engine)

	// Wire membership changes to overlay reclassification.
	m.Membership.SetOnChange(func(peers []membership.PeerInfo) {
		overlayPeers := make([]overlay.PeerInfo, len(peers))
		for i, p := range peers {
			overlayPeers[i] = overlay.PeerInfo{
				NodeID: p.NodeID,
				Addr:   fmt.Sprintf("%s:%d", p.Meta.QUICAddr, p.Meta.QUICPort),
				RTT:    p.RTT,
			}
		}
		m.Overlay.Reclassify(overlayPeers)
	})

	// Start accept loops on existing memberlist connections (from seed joins).
	t.StartAcceptLoopAll()

	// S3 discovery runs as a background supplement — never blocks Join().
	if cfg.ObjectStore != nil {
		if err := m.startS3Coordination(ctx, cfg); err != nil {
			t.Shutdown()
			m.Membership.Stop()
			cancel()
			return nil, fmt.Errorf("s3 coordination: %w", err)
		}
	}

	return m, nil
}

// startS3Coordination registers this node in the object store and starts
// background loops for discovery, heartbeat, and NAT traversal. It does not
// block on peer connections — those happen in the background.
func (m *Mesh) startS3Coordination(ctx context.Context, cfg Config) error {
	appPort := cfg.BindPort + 1
	bindAddr := fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.BindPort)
	appAddr := fmt.Sprintf("%s:%d", cfg.BindAddr, appPort)

	// Register ourselves in the object store.
	reg := bootstrap.NodeRegistration{
		NodeID: cfg.NodeID,
		Addrs:  []string{bindAddr, appAddr},
	}
	if err := bootstrap.Register(ctx, cfg.ObjectStore, reg); err != nil {
		return fmt.Errorf("register node: %w", err)
	}
	m.reg = &reg

	// Initial S3 discovery — connect to peers that seeds didn't reach.
	m.discoverAndConnect(ctx, cfg, appAddr)

	// Background heartbeat.
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		bootstrap.HeartbeatLoop(ctx, cfg.ObjectStore, m.reg, cfg.HeartbeatInterval)
	}()

	// Background steady-state discovery.
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.steadyStateLoop(ctx, cfg, appAddr)
	}()

	return nil
}

// discoverAndConnect queries S3 for peers and attempts to connect to any
// that SWIM doesn't already know about. For each unknown peer it tries,
// in order: memberlist Join (fast, works if directly reachable), hole punch
// (works across NAT if both sides cooperate), relay (last resort).
func (m *Mesh) discoverAndConnect(ctx context.Context, cfg Config, appAddr string) {
	peers, err := bootstrap.DiscoverPeers(ctx, cfg.ObjectStore, cfg.StaleNodeAge)
	if err != nil {
		m.logger.Printf("[WARN] mesh: s3 discovery failed: %v", err)
		return
	}

	for _, peer := range peers {
		if peer.NodeID == cfg.NodeID {
			continue
		}

		// Skip peers that SWIM already knows about — gossip is handling them.
		if m.Membership.IsAlive(peer.NodeID) {
			continue
		}

		// Try memberlist Join first — this is the fast, normal path.
		// It works when the peer is directly reachable (same VPC, no NAT).
		if m.tryMemberlistJoin(peer) {
			continue
		}

		// Direct join failed — escalate to NAT traversal.
		if m.tryHolePunch(ctx, cfg, peer, appAddr) {
			continue
		}

		// Hole punch failed — try relay as last resort.
		m.tryRelay(ctx, cfg, peer)
	}
}

// tryMemberlistJoin attempts to connect to a peer via normal memberlist Join
// using the addresses from their S3 registration. This is the fast path —
// no S3 round-trips, just a direct QUIC dial.
func (m *Mesh) tryMemberlistJoin(peer bootstrap.NodeRegistration) bool {
	if len(peer.Addrs) == 0 {
		return false
	}
	// memberlist Join tries each address and only needs one to succeed.
	// Use the memberlist-port addresses (first addr in registration).
	_, err := m.Membership.Join(peer.Addrs[:1])
	return err == nil
}

// tryHolePunch attempts S3-signaled simultaneous QUIC open to a NATed peer.
func (m *Mesh) tryHolePunch(ctx context.Context, cfg Config, peer bootstrap.NodeRegistration, selfAddr string) bool {
	if peer.PublicAddr == "" {
		return false
	}

	m.logger.Printf("[DEBUG] mesh: attempting hole punch to %s at %s", peer.NodeID, peer.PublicAddr)
	punchParams := holepunch.PunchParams{
		Store:         cfg.ObjectStore,
		QUICTransport: m.Transport.QUICTransport(),
		TLSConfig:     m.Membership.TLSConfig(),
		QUICConfig: &quic.Config{
			MaxIdleTimeout:  30 * time.Second,
			KeepAlivePeriod: 10 * time.Second,
			EnableDatagrams: true,
		},
		SelfID:   cfg.NodeID,
		TargetID: peer.NodeID,
		SelfAddr: selfAddr,
	}

	conn, err := holepunch.AttemptHolePunch(ctx, punchParams)
	if err != nil {
		m.logger.Printf("[DEBUG] mesh: hole punch to %s failed: %v", peer.NodeID, err)
		return false
	}

	m.Transport.AddConnection(conn)
	addr := conn.RemoteAddr().String()
	if err := m.Membership.InjectConnection(conn, addr); err != nil {
		m.logger.Printf("[WARN] mesh: inject hole-punched connection to %s: %v", peer.NodeID, err)
	}
	return true
}

// tryRelay attempts to reach a peer via a relay node with a public IP.
func (m *Mesh) tryRelay(ctx context.Context, cfg Config, target bootstrap.NodeRegistration) {
	if target.IsRelay {
		// This peer IS a relay but we can't reach it directly — nothing to do.
		return
	}

	peers, err := bootstrap.DiscoverPeers(ctx, cfg.ObjectStore, cfg.StaleNodeAge)
	if err != nil {
		return
	}
	for _, p := range peers {
		if !p.IsRelay || p.NodeID == cfg.NodeID || p.NodeID == target.NodeID {
			continue
		}
		relayAddr := p.PublicAddr
		if relayAddr == "" && len(p.Addrs) > 0 {
			relayAddr = p.Addrs[0]
		}
		if relayAddr == "" {
			continue
		}

		stream, err := relay.DialViaRelay(ctx, m.Transport, relayAddr, target.NodeID)
		if err != nil {
			m.logger.Printf("[DEBUG] mesh: relay via %s to %s failed: %v", p.NodeID, target.NodeID, err)
			continue
		}
		_ = stream
		m.logger.Printf("[INFO] mesh: relayed connection to %s via %s", target.NodeID, p.NodeID)
		return
	}
}

// steadyStateLoop periodically polls S3 for new peers that gossip hasn't
// discovered yet. This catches nodes behind different NATs that join after
// us and have no shared gossip neighbor.
func (m *Mesh) steadyStateLoop(ctx context.Context, cfg Config, appAddr string) {
	ticker := time.NewTicker(cfg.SteadyStatePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.discoverAndConnect(ctx, cfg, appAddr)
		}
	}
}

// Leave gracefully leaves the mesh, deregisters from the object store,
// and shuts down all components.
func (m *Mesh) Leave() error {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()

	// Deregister from object store.
	if m.store != nil && m.reg != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = bootstrap.Deregister(ctx, m.store, m.reg.NodeID)
	}

	if m.Transport != nil {
		m.Transport.Shutdown()
	}
	if m.Membership != nil {
		return m.Membership.Stop()
	}
	return nil
}

func generateNodeID() string {
	b := make([]byte, 8)
	_, _ = net.Interfaces() // seed entropy
	for i := range b {
		b[i] = byte(time.Now().UnixNano() >> (i * 8))
	}
	return fmt.Sprintf("node-%x", b)
}
