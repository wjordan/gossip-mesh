// Package mesh provides a unified entry point for joining a gossip-mesh cluster.
// It supports two modes:
//   - S3-coordinated: uses an object store for TLS bootstrap, peer discovery,
//     hole punching, and relay — works across NAT boundaries.
//   - Direct: uses pre-configured TLS and seed addresses — requires direct IP
//     reachability (the existing behavior).
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

	// Mode 1: S3-coordinated (NAT-friendly).
	// Set ObjectStore to enable S3-based bootstrap, discovery, and NAT traversal.
	ObjectStore objstore.ObjectStore

	// Mode 2: Direct connectivity (existing behavior).
	// Set SeedAddrs and TLS for direct membership.
	SeedAddrs []string
	TLS       *tls.Config

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

	store       objstore.ObjectStore
	reg         *bootstrap.NodeRegistration
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	relayServer *relay.Server
	logger      *log.Logger
}

// Join starts a gossip-mesh node. When cfg.ObjectStore is set, it performs
// S3-coordinated bootstrap with NAT traversal. Otherwise, it uses direct
// connectivity with pre-configured TLS and seed addresses.
func Join(cfg Config) (*Mesh, error) {
	cfg.withDefaults()

	m := &Mesh{
		Membership: &membership.Membership{},
		store:      cfg.ObjectStore,
		logger:     cfg.Logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	var tlsConfig *tls.Config

	if cfg.ObjectStore != nil {
		// S3-coordinated mode: bootstrap TLS from object store.
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
		// Direct mode: use provided TLS config.
		tlsConfig = cfg.TLS
	}

	if tlsConfig == nil {
		cancel()
		return nil, fmt.Errorf("mesh: TLS config required (provide ObjectStore or TLS)")
	}

	// Start membership layer.
	memCfg := membership.MembershipConfig{
		NodeID:        cfg.NodeID,
		BindAddr:      cfg.BindAddr,
		BindPort:      cfg.BindPort,
		AdvertiseAddr: cfg.AdvertiseAddr,
		AdvertisePort: cfg.AdvertisePort,
		Meta:          cfg.Meta,
		TLS:           tlsConfig,
	}

	// In S3 mode, don't join seeds yet — we'll discover peers via S3.
	if cfg.ObjectStore == nil {
		memCfg.SeedAddrs = cfg.SeedAddrs
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

	// Register address reflection handler.
	natutil.RegisterReflectHandler(t)

	if cfg.ObjectStore != nil {
		// S3-coordinated: register, discover peers, handle NAT.
		if err := m.s3Bootstrap(ctx, cfg); err != nil {
			t.Shutdown()
			m.Membership.Stop()
			cancel()
			return nil, fmt.Errorf("s3 bootstrap: %w", err)
		}
	}

	// Start overlay and engine.
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

	// Start accept loops on existing memberlist connections.
	t.StartAcceptLoopAll()

	return m, nil
}

// s3Bootstrap handles S3-coordinated peer discovery and NAT traversal.
func (m *Mesh) s3Bootstrap(ctx context.Context, cfg Config) error {
	appPort := cfg.BindPort + 1
	bindAddr := fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.BindPort)
	appAddr := fmt.Sprintf("%s:%d", cfg.BindAddr, appPort)

	// Register ourselves.
	reg := bootstrap.NodeRegistration{
		NodeID: cfg.NodeID,
		Addrs:  []string{bindAddr, appAddr},
	}
	if err := bootstrap.Register(ctx, cfg.ObjectStore, reg); err != nil {
		return fmt.Errorf("register node: %w", err)
	}
	m.reg = &reg

	// Discover peers.
	peers, err := bootstrap.DiscoverPeers(ctx, cfg.ObjectStore, cfg.StaleNodeAge)
	if err != nil {
		return fmt.Errorf("discover peers: %w", err)
	}

	// Try to connect to each discovered peer.
	for _, peer := range peers {
		if peer.NodeID == cfg.NodeID {
			continue
		}

		connected := false

		// Try direct connection first via memberlist addrs.
		for _, addr := range peer.Addrs {
			conn, err := m.Membership.ConnPool().GetOrDial(ctx, addr)
			if err == nil {
				if err := m.Membership.InjectConnection(conn, addr); err != nil {
					m.logger.Printf("[DEBUG] mesh: inject peer %s at %s: %v", peer.NodeID, addr, err)
				}
				connected = true
				break
			}
		}
		if connected {
			continue
		}

		// Direct connection failed — try hole punch if we have a public addr.
		if peer.PublicAddr != "" {
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
				SelfAddr: appAddr,
			}
			conn, err := holepunch.AttemptHolePunch(ctx, punchParams)
			if err == nil {
				m.Transport.AddConnection(conn)
				addr := conn.RemoteAddr().String()
				if err := m.Membership.InjectConnection(conn, addr); err != nil {
					m.logger.Printf("[WARN] mesh: inject hole-punched connection to %s: %v", peer.NodeID, err)
				}
				continue
			}
			m.logger.Printf("[DEBUG] mesh: hole punch to %s failed: %v, trying relay", peer.NodeID, err)
		}

		// Hole punch failed or unavailable — find a relay.
		if peer.IsRelay {
			// This peer IS a relay, but we can't reach it. Skip.
			continue
		}
		m.tryRelay(ctx, cfg, peer)
	}

	// Start heartbeat loop.
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		bootstrap.HeartbeatLoop(ctx, cfg.ObjectStore, reg, cfg.HeartbeatInterval)
	}()

	// Start steady-state discovery loop.
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.steadyStateLoop(ctx, cfg)
	}()

	return nil
}

// tryRelay attempts to connect to a peer via any known relay node.
func (m *Mesh) tryRelay(ctx context.Context, cfg Config, target bootstrap.NodeRegistration) {
	peers, err := bootstrap.DiscoverPeers(ctx, cfg.ObjectStore, cfg.StaleNodeAge)
	if err != nil {
		return
	}
	for _, p := range peers {
		if !p.IsRelay || p.NodeID == cfg.NodeID || p.NodeID == target.NodeID {
			continue
		}
		// Try relay through this peer.
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
		_ = stream // Relayed stream established — used for SWIM probes
		m.logger.Printf("[INFO] mesh: relayed connection to %s via %s", target.NodeID, p.NodeID)
		return
	}
}

// steadyStateLoop periodically discovers new peers and connects to them.
func (m *Mesh) steadyStateLoop(ctx context.Context, cfg Config) {
	ticker := time.NewTicker(cfg.SteadyStatePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			peers, err := bootstrap.DiscoverPeers(ctx, cfg.ObjectStore, cfg.StaleNodeAge)
			if err != nil {
				m.logger.Printf("[WARN] mesh: steady-state discovery failed: %v", err)
				continue
			}

			// Try to connect to any peers we don't already know about.
			for _, peer := range peers {
				if peer.NodeID == cfg.NodeID {
					continue
				}
				if m.Membership.IsAlive(peer.NodeID) {
					continue
				}
				// Try direct connect.
				for _, addr := range peer.Addrs {
					if _, err := m.Membership.ConnPool().GetOrDial(ctx, addr); err == nil {
						break
					}
				}
			}
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
