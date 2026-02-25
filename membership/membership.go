package membership

import (
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/quic-go/quic-go"
	memberlistquic "github.com/wjordan/memberlist-quic"
	"github.com/wjordan/vivaldi"
)

// MembershipConfig configures the membership layer.
type MembershipConfig struct {
	NodeID        string
	BindAddr      string
	BindPort      int
	AdvertiseAddr string
	AdvertisePort int
	SeedAddrs     []string
	ProbeInterval time.Duration // default 500ms
	ProbeTimeout  time.Duration // default 250ms
	SuspicionMult int           // default 3
	Meta          NodeMeta
	TLS           *tls.Config
}

// PeerInfo describes a known peer.
type PeerInfo struct {
	NodeID string
	Addr   string
	Meta   NodeMeta
	RTT    time.Duration // from Vivaldi coordinate distance
	Alive  bool
}

// Membership manages cluster membership via memberlist over QUIC with
// Vivaldi coordinate tracking.
type Membership struct {
	list          *memberlist.Memberlist
	quicTransport *memberlistquic.Transport
	meta          atomic.Pointer[NodeMeta]
	vivaldi       *vivaldi.Client
	onChange      func([]PeerInfo)
	onLeave       func(nodeID string)
	tlsConfig     *tls.Config // stored during Start for FluiteTransport reuse

	mu    sync.RWMutex
	peers map[string]*PeerInfo
}

// Start creates the QUIC transport, initializes memberlist, and joins seed nodes.
func (m *Membership) Start(cfg MembershipConfig) error {
	if cfg.ProbeInterval == 0 {
		cfg.ProbeInterval = 500 * time.Millisecond
	}
	if cfg.ProbeTimeout == 0 {
		cfg.ProbeTimeout = 250 * time.Millisecond
	}
	if cfg.SuspicionMult == 0 {
		cfg.SuspicionMult = 3
	}

	// Initialize Vivaldi client.
	vc, err := vivaldi.NewClient(vivaldi.DefaultConfig())
	if err != nil {
		return fmt.Errorf("create vivaldi client: %w", err)
	}
	m.vivaldi = vc
	m.peers = make(map[string]*PeerInfo)

	// Seed metadata with initial Vivaldi coordinate.
	cfg.Meta.NodeID = cfg.NodeID
	cfg.Meta.VivaldiCoord = vc.GetCoordinate()
	m.meta.Store(&cfg.Meta)

	// TLS config is required.
	if cfg.TLS == nil {
		return fmt.Errorf("membership: TLS config is required")
	}
	m.tlsConfig = cfg.TLS

	// Create QUIC transport.
	qt, err := memberlistquic.New(memberlistquic.Config{
		BindAddr: cfg.BindAddr,
		BindPort: cfg.BindPort,
		TLS:      cfg.TLS,
	})
	if err != nil {
		return fmt.Errorf("create QUIC transport: %w", err)
	}
	m.quicTransport = qt

	// Configure memberlist.
	mlCfg := memberlist.DefaultLANConfig()
	mlCfg.Name = cfg.NodeID
	mlCfg.Transport = qt
	mlCfg.AdvertiseAddr = cfg.AdvertiseAddr
	mlCfg.AdvertisePort = cfg.AdvertisePort
	mlCfg.ProbeInterval = cfg.ProbeInterval
	mlCfg.ProbeTimeout = cfg.ProbeTimeout
	mlCfg.SuspicionMult = cfg.SuspicionMult
	mlCfg.Delegate = &delegate{meta: &m.meta}
	mlCfg.Events = &eventDelegate{m: m}
	mlCfg.Ping = &pingDelegate{m: m}

	list, err := memberlist.Create(mlCfg)
	if err != nil {
		qt.Shutdown()
		return fmt.Errorf("create memberlist: %w", err)
	}
	m.list = list

	// Join seed nodes (non-fatal if no seeds — single-node cluster).
	if len(cfg.SeedAddrs) > 0 {
		if _, err := list.Join(cfg.SeedAddrs); err != nil {
			// Log but don't fail — node can still operate and retry via gossip.
			fmt.Printf("membership: join seeds failed: %v\n", err)
		}
	}

	return nil
}

// Stop gracefully leaves the cluster and shuts down.
func (m *Membership) Stop() error {
	if m.list != nil {
		if err := m.list.Leave(5 * time.Second); err != nil {
			return fmt.Errorf("leave: %w", err)
		}
		if err := m.list.Shutdown(); err != nil {
			return fmt.Errorf("shutdown memberlist: %w", err)
		}
	}
	if m.quicTransport != nil {
		return m.quicTransport.Shutdown()
	}
	return nil
}

// LivePeers returns a snapshot of all alive peers.
func (m *Membership) LivePeers() []PeerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make([]PeerInfo, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Alive {
			peers = append(peers, *p)
		}
	}
	return peers
}

// SetOnChange sets the callback invoked when the peer set changes.
func (m *Membership) SetOnChange(fn func([]PeerInfo)) {
	m.mu.Lock()
	m.onChange = fn
	m.mu.Unlock()
}

// SetOnLeave sets the callback invoked when a peer leaves the cluster
// (graceful leave or SWIM failure detection). Extensions can use this
// to clean up external state for dead nodes.
func (m *Membership) SetOnLeave(fn func(nodeID string)) {
	m.mu.Lock()
	m.onLeave = fn
	m.mu.Unlock()
}

// UpdateMeta atomically updates the local node metadata and triggers a
// memberlist metadata broadcast.
func (m *Membership) UpdateMeta(fn func(*NodeMeta)) {
	for {
		old := m.meta.Load()
		updated := *old
		fn(&updated)
		if m.meta.CompareAndSwap(old, &updated) {
			break
		}
	}
	if m.list != nil {
		m.list.UpdateNode(0)
	}
}

// ConnPool returns the QUIC connection pool for application-level traffic.
func (m *Membership) ConnPool() *memberlistquic.ConnPool {
	if m.quicTransport == nil {
		return nil
	}
	return m.quicTransport.ConnPool()
}

// LocalAddr returns the advertised address for this node.
func (m *Membership) LocalAddr() string {
	if m.list == nil {
		return ""
	}
	node := m.list.LocalNode()
	return net.JoinHostPort(node.Addr.String(), strconv.Itoa(int(node.Port)))
}

// IsAlive returns true if the given node is a known alive peer.
func (m *Membership) IsAlive(nodeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.peers[nodeID]
	return ok && p.Alive
}

// AddrForNode returns the QUIC app transport address for the given node,
// or empty string if the node is unknown or dead.
func (m *Membership) AddrForNode(nodeID string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.peers[nodeID]
	if !ok || !p.Alive {
		return ""
	}
	return net.JoinHostPort(p.Meta.QUICAddr, strconv.Itoa(p.Meta.QUICPort))
}

// TLSConfig returns the mutual TLS config created during Start.
// FluiteTransport reuses this to share the same CA.
func (m *Membership) TLSConfig() *tls.Config {
	return m.tlsConfig
}

// InjectConnection registers an externally established QUIC connection
// (e.g., from hole punching or relay) in the memberlist connection pool
// and makes SWIM aware of the peer.
func (m *Membership) InjectConnection(conn *quic.Conn, addr string) error {
	pool := m.ConnPool()
	if pool == nil {
		return fmt.Errorf("membership: transport not initialized")
	}
	pool.AddInbound(conn)
	if _, err := m.list.Join([]string{addr}); err != nil {
		return fmt.Errorf("membership: join injected peer: %w", err)
	}
	return nil
}

// Join attempts to reach the given memberlist addresses and merge with
// the cluster. It is safe to call multiple times with different addresses.
func (m *Membership) Join(addrs []string) (int, error) {
	if m.list == nil {
		return 0, fmt.Errorf("membership: not started")
	}
	return m.list.Join(addrs)
}

// QUICTransport returns the memberlist-quic transport, providing access to
// RawTransport() for hole punching.
func (m *Membership) QUICTransport() *memberlistquic.Transport {
	return m.quicTransport
}

// notifyChange calls the onChange callback with a snapshot of peers.
func (m *Membership) notifyChange() {
	m.mu.RLock()
	fn := m.onChange
	m.mu.RUnlock()

	if fn != nil {
		fn(m.LivePeers())
	}
}
