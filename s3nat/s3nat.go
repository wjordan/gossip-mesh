// Package s3nat extends gossip-mesh with S3-coordinated peer discovery and
// NAT traversal. It implements mesh.DiscoveryExtension.
//
// When enabled, this extension:
//   - Bootstraps TLS from a shared CA in the object store
//   - Registers the node and discovers peers via S3
//   - Escalates through hole punching and relay for NATed peers
//   - Cleans up dead nodes' registrations when SWIM detects failure
//     (no heartbeat loop needed)
//   - Periodically polls S3 for new peers that gossip can't reach
package s3nat

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/wjordan/gossip-mesh/bootstrap"
	"github.com/wjordan/gossip-mesh/holepunch"
	"github.com/wjordan/gossip-mesh/mesh"
	"github.com/wjordan/gossip-mesh/natutil"
	"github.com/wjordan/gossip-mesh/objstore"
	"github.com/wjordan/gossip-mesh/relay"
)

// Config configures S3-coordinated discovery and NAT traversal.
type Config struct {
	ObjectStore objstore.ObjectStore

	// PollInterval is how often to poll S3 for new peers that gossip
	// hasn't discovered. Default 60s.
	PollInterval time.Duration

	Logger *log.Logger
}

func (c *Config) withDefaults() {
	if c.PollInterval == 0 {
		c.PollInterval = 60 * time.Second
	}
	if c.Logger == nil {
		c.Logger = log.Default()
	}
}

// Extension implements mesh.DiscoveryExtension for S3-coordinated NAT traversal.
type Extension struct {
	cfg    Config
	store  objstore.ObjectStore
	reg    *bootstrap.NodeRegistration
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *log.Logger
}

var _ mesh.DiscoveryExtension = (*Extension)(nil)

// New creates an S3 NAT traversal extension. Pass the returned Extension
// as mesh.Config.Discovery.
func New(cfg Config) *Extension {
	cfg.withDefaults()
	return &Extension{
		cfg:    cfg,
		store:  cfg.ObjectStore,
		logger: cfg.Logger,
	}
}

// SetupTLS bootstraps mutual TLS from the object store's shared CA.
// Call this before mesh.Join to get the TLS config needed for Config.TLS.
func (e *Extension) SetupTLS(ctx context.Context, nodeID string, ips []net.IP) (*tls.Config, error) {
	return bootstrap.SetupClusterTLS(ctx, e.store, nodeID, ips)
}

// Start registers this node in S3, runs initial discovery, hooks SWIM
// leave events for cleanup, and starts the background poll loop.
func (e *Extension) Start(ctx context.Context, m *mesh.Mesh) error {
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel

	cfg := m.Config()

	appPort := cfg.BindPort + 1
	bindAddr := net.JoinHostPort(cfg.BindAddr, strconv.Itoa(cfg.BindPort))
	appAddr := net.JoinHostPort(cfg.BindAddr, strconv.Itoa(appPort))

	// Register address reflection handler.
	natutil.RegisterReflectHandler(m.Transport)

	// Register ourselves in S3.
	reg := bootstrap.NodeRegistration{
		NodeID: cfg.NodeID,
		Addrs:  []string{bindAddr, appAddr},
	}
	if err := bootstrap.Register(ctx, e.store, reg); err != nil {
		cancel()
		return fmt.Errorf("register node: %w", err)
	}
	e.reg = &reg

	// Hook SWIM leave events to clean up dead nodes' S3 registrations.
	m.Membership.SetOnLeave(func(nodeID string) {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		if err := bootstrap.Deregister(cleanCtx, e.store, nodeID); err != nil {
			e.logger.Printf("[WARN] s3nat: failed to deregister dead node %s: %v", nodeID, err)
		} else {
			e.logger.Printf("[DEBUG] s3nat: deregistered dead node %s", nodeID)
		}
	})

	// Initial discovery — find peers that seeds didn't reach.
	e.discoverAndConnect(ctx, m, appAddr)

	// Background poll for new peers.
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.pollLoop(ctx, m, appAddr)
	}()

	return nil
}

// Stop deregisters this node from S3 and shuts down background loops.
func (e *Extension) Stop() error {
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()

	if e.reg != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = bootstrap.Deregister(ctx, e.store, e.reg.NodeID)
	}
	return nil
}

// discoverAndConnect queries S3 for peers and connects to any that SWIM
// doesn't already know about. Escalation: memberlist Join → hole punch → relay.
func (e *Extension) discoverAndConnect(ctx context.Context, m *mesh.Mesh, appAddr string) {
	peers, err := bootstrap.DiscoverPeers(ctx, e.store)
	if err != nil {
		e.logger.Printf("[WARN] s3nat: discovery failed: %v", err)
		return
	}

	nodeID := m.Config().NodeID
	for _, peer := range peers {
		if peer.NodeID == nodeID {
			continue
		}
		if m.Membership.IsAlive(peer.NodeID) {
			continue
		}

		if e.tryMemberlistJoin(m, peer) {
			continue
		}
		if e.tryHolePunch(ctx, m, peer, appAddr) {
			continue
		}
		e.tryRelay(ctx, m, peer)
	}
}

func (e *Extension) tryMemberlistJoin(m *mesh.Mesh, peer bootstrap.NodeRegistration) bool {
	if len(peer.Addrs) == 0 {
		return false
	}
	_, err := m.Membership.Join(peer.Addrs[:1])
	return err == nil
}

func (e *Extension) tryHolePunch(ctx context.Context, m *mesh.Mesh, peer bootstrap.NodeRegistration, selfAddr string) bool {
	if peer.PublicAddr == "" {
		return false
	}

	e.logger.Printf("[DEBUG] s3nat: attempting hole punch to %s at %s", peer.NodeID, peer.PublicAddr)
	punchParams := holepunch.PunchParams{
		Store:         e.store,
		QUICTransport: m.Transport.QUICTransport(),
		TLSConfig:     m.TLSConfig(),
		QUICConfig: &quic.Config{
			MaxIdleTimeout:  30 * time.Second,
			KeepAlivePeriod: 10 * time.Second,
			EnableDatagrams: true,
		},
		SelfID:   m.Config().NodeID,
		TargetID: peer.NodeID,
		SelfAddr: selfAddr,
	}

	conn, err := holepunch.AttemptHolePunch(ctx, punchParams)
	if err != nil {
		e.logger.Printf("[DEBUG] s3nat: hole punch to %s failed: %v", peer.NodeID, err)
		return false
	}

	m.Transport.AddConnection(conn)
	addr := conn.RemoteAddr().String()
	if err := m.Membership.InjectConnection(conn, addr); err != nil {
		e.logger.Printf("[WARN] s3nat: inject hole-punched connection to %s: %v", peer.NodeID, err)
	}
	return true
}

func (e *Extension) tryRelay(ctx context.Context, m *mesh.Mesh, target bootstrap.NodeRegistration) {
	if target.IsRelay {
		return
	}

	peers, err := bootstrap.DiscoverPeers(ctx, e.store)
	if err != nil {
		return
	}
	nodeID := m.Config().NodeID
	for _, p := range peers {
		if !p.IsRelay || p.NodeID == nodeID || p.NodeID == target.NodeID {
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
			e.logger.Printf("[DEBUG] s3nat: relay via %s to %s failed: %v", p.NodeID, target.NodeID, err)
			continue
		}
		_ = stream
		e.logger.Printf("[INFO] s3nat: relayed connection to %s via %s", target.NodeID, p.NodeID)
		return
	}
}

func (e *Extension) pollLoop(ctx context.Context, m *mesh.Mesh, appAddr string) {
	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.discoverAndConnect(ctx, m, appAddr)
		}
	}
}
