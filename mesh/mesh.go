// Package mesh provides a unified entry point for joining a gossip-mesh cluster.
//
// The base mesh uses seeds and memberlist gossip for peer discovery. For NAT
// traversal and S3-coordinated bootstrap, use the s3nat package as a
// DiscoveryExtension.
package mesh

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/wjordan/gossip-mesh/engine"
	"github.com/wjordan/gossip-mesh/membership"
	"github.com/wjordan/gossip-mesh/overlay"
	"github.com/wjordan/gossip-mesh/transport"
)

// DiscoveryExtension is an optional hook for extending the mesh with
// additional peer discovery and connectivity mechanisms (e.g., S3-coordinated
// NAT traversal). The mesh calls Start after the base infrastructure is
// running, and Stop during Leave.
type DiscoveryExtension interface {
	Start(ctx context.Context, m *Mesh) error
	Stop() error
}

// Config configures the mesh.
type Config struct {
	NodeID    string // generated if empty
	BindAddr  string
	BindPort  int // memberlist port; app = BindPort+1

	// SeedAddrs are memberlist addresses to join on startup.
	SeedAddrs []string

	// TLS is the mutual TLS config. Required unless a DiscoveryExtension
	// provides TLS (e.g., the s3nat extension bootstraps TLS from the
	// object store).
	TLS *tls.Config

	AdvertiseAddr string
	AdvertisePort int
	Meta          membership.NodeMeta
	Logger        *log.Logger

	Overlay overlay.OverlayConfig
	Engine  engine.EngineConfig

	// Discovery is an optional extension for additional peer discovery
	// beyond seed-based membership (e.g., S3-coordinated NAT traversal).
	Discovery DiscoveryExtension
}

func (c *Config) withDefaults() {
	if c.Logger == nil {
		c.Logger = log.Default()
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

	cfg       Config
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	discovery DiscoveryExtension
	logger    *log.Logger
}

// Config returns the configuration this mesh was started with.
func (m *Mesh) Config() Config {
	return m.cfg
}

// TLSConfig returns the TLS config in use by this mesh node.
func (m *Mesh) TLSConfig() *tls.Config {
	return m.Membership.TLSConfig()
}

// Join starts a gossip-mesh node. Seeds are joined immediately. If a
// DiscoveryExtension is configured, it is started after the base
// infrastructure is running.
func Join(cfg Config) (*Mesh, error) {
	cfg.withDefaults()

	m := &Mesh{
		Membership: &membership.Membership{},
		cfg:        cfg,
		discovery:  cfg.Discovery,
		logger:     cfg.Logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	tlsConfig := cfg.TLS

	if tlsConfig == nil {
		cancel()
		return nil, fmt.Errorf("mesh: TLS config required")
	}

	// Start membership layer with seeds.
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

	// Start overlay and engine so seed-connected peers can gossip immediately.
	m.Overlay = overlay.New(cfg.NodeID, cfg.Overlay)
	m.Engine = engine.New(m.Overlay, t, cfg.Engine)

	// Wire membership changes to overlay reclassification.
	m.Membership.SetOnChange(func(peers []membership.PeerInfo) {
		overlayPeers := make([]overlay.PeerInfo, len(peers))
		for i, p := range peers {
			overlayPeers[i] = overlay.PeerInfo{
				NodeID: p.NodeID,
				Addr:   net.JoinHostPort(p.Meta.QUICAddr, strconv.Itoa(p.Meta.QUICPort)),
				RTT:    p.RTT,
			}
		}
		m.Overlay.Reclassify(overlayPeers)
	})

	// Start accept loops on existing memberlist connections (from seed joins).
	t.StartAcceptLoopAll()

	// Start discovery extension if configured.
	if m.discovery != nil {
		if err := m.discovery.Start(ctx, m); err != nil {
			t.Shutdown()
			m.Membership.Stop()
			cancel()
			return nil, fmt.Errorf("discovery extension: %w", err)
		}
	}

	return m, nil
}

// Leave gracefully leaves the mesh and shuts down all components.
func (m *Mesh) Leave() error {
	// Stop discovery extension first.
	if m.discovery != nil {
		m.discovery.Stop()
	}

	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()

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
	_, _ = net.Interfaces()
	for i := range b {
		b[i] = byte(time.Now().UnixNano() >> (i * 8))
	}
	return fmt.Sprintf("node-%x", b)
}
