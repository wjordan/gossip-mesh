package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	memberlistquic "github.com/wjordan/memberlist-quic"

	"github.com/quic-go/quic-go"
)

// Handler types for extensible stream/datagram dispatch.
type BidiHandler func(from string, stream *quic.Stream)
type UniHandler func(from string, stream *quic.ReceiveStream)
type DatagramHandler func(from string, data []byte)

// Transport manages a separate QUIC listener for application-level
// traffic (gossip, repair, etc.). It uses the memberlist-quic
// ConnPool only for peer discovery (knowing which addresses exist), while
// application connections are managed independently to avoid competing
// with memberlist's stream acceptance.
type Transport struct {
	memberlistPool *memberlistquic.ConnPool

	listener  *quic.Listener
	transport *quic.Transport
	appConns  sync.Map // addr string -> *quic.Conn

	tlsConfig  *tls.Config
	quicConfig *quic.Config
	logger     *log.Logger

	shutdownCh chan struct{}
	wg         sync.WaitGroup

	// Handlers for inbound traffic, set by the gossip engine via SetHandlers.
	onEagerGossip func(from string, data []byte)
	onLazyBatch   func(from string, reader io.Reader)
	onRepair      func(from string, stream *quic.Stream)

	// Extensible handler registries for application-defined stream types.
	bidiHandlers     map[byte]BidiHandler
	uniHandlers      map[byte]UniHandler
	datagramHandlers map[byte]DatagramHandler
	handlerMu        sync.RWMutex
}

// SetHandlers atomically sets the gossip protocol handlers. Safe to call
// concurrently with transport goroutines reading these handlers.
func (t *Transport) SetHandlers(
	eager func(from string, data []byte),
	lazy func(from string, reader io.Reader),
	repair func(from string, stream *quic.Stream),
) {
	t.handlerMu.Lock()
	t.onEagerGossip = eager
	t.onLazyBatch = lazy
	t.onRepair = repair
	t.handlerMu.Unlock()
}

func (t *Transport) getEagerGossip() func(from string, data []byte) {
	t.handlerMu.RLock()
	h := t.onEagerGossip
	t.handlerMu.RUnlock()
	return h
}

func (t *Transport) getLazyBatch() func(from string, reader io.Reader) {
	t.handlerMu.RLock()
	h := t.onLazyBatch
	t.handlerMu.RUnlock()
	return h
}

func (t *Transport) getRepair() func(from string, stream *quic.Stream) {
	t.handlerMu.RLock()
	h := t.onRepair
	t.handlerMu.RUnlock()
	return h
}

// Config configures the application-level QUIC transport.
type Config struct {
	// BindAddr is the address to bind the application QUIC listener on.
	BindAddr string
	// BindPort is the UDP port for the application listener.
	// Convention: memberlist port + 1.
	BindPort int

	// ALPN protocol identifier (default: "gossip-mesh/1")
	ALPN string

	// TLS is the TLS configuration (same CA as memberlist).
	TLS *tls.Config

	// MemberlistPool is the memberlist-quic connection pool, used only
	// for peer discovery (Range to enumerate known addresses).
	MemberlistPool *memberlistquic.ConnPool

	Logger *log.Logger
}

// New creates and starts the application QUIC transport.
func New(cfg Config) (*Transport, error) {
	if cfg.TLS == nil {
		return nil, errors.New("transport: TLS config is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}

	alpn := cfg.ALPN
	if alpn == "" {
		alpn = "gossip-mesh/1"
	}

	tlsConf := cfg.TLS.Clone()
	tlsConf.NextProtos = []string{alpn}

	quicConf := &quic.Config{
		MaxIdleTimeout:       30_000_000_000, // 30s
		KeepAlivePeriod:      10_000_000_000, // 10s
		EnableDatagrams:      true,
		MaxIncomingStreams:    1000,
		MaxIncomingUniStreams: 1000,
	}

	udpAddr := &net.UDPAddr{
		IP:   net.ParseIP(cfg.BindAddr),
		Port: cfg.BindPort,
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	qTransport := &quic.Transport{Conn: udpConn}

	listener, err := qTransport.Listen(tlsConf, quicConf)
	if err != nil {
		udpConn.Close()
		return nil, err
	}

	t := &Transport{
		memberlistPool:   cfg.MemberlistPool,
		listener:         listener,
		transport:        qTransport,
		tlsConfig:        tlsConf,
		quicConfig:       quicConf,
		logger:           cfg.Logger,
		shutdownCh:       make(chan struct{}),
		bidiHandlers:     make(map[byte]BidiHandler),
		uniHandlers:      make(map[byte]UniHandler),
		datagramHandlers: make(map[byte]DatagramHandler),
	}

	// Accept inbound connections from peers.
	t.wg.Add(1)
	go t.acceptLoop()

	return t, nil
}

// RegisterBidiHandler registers a handler for a custom bidirectional stream type.
func (t *Transport) RegisterBidiHandler(typeByte byte, h BidiHandler) {
	t.handlerMu.Lock()
	t.bidiHandlers[typeByte] = h
	t.handlerMu.Unlock()
}

// RegisterUniHandler registers a handler for a custom unidirectional stream type.
func (t *Transport) RegisterUniHandler(typeByte byte, h UniHandler) {
	t.handlerMu.Lock()
	t.uniHandlers[typeByte] = h
	t.handlerMu.Unlock()
}

// RegisterDatagramHandler registers a handler for a custom datagram type.
func (t *Transport) RegisterDatagramHandler(typeByte byte, h DatagramHandler) {
	t.handlerMu.Lock()
	t.datagramHandlers[typeByte] = h
	t.handlerMu.Unlock()
}

// SendDatagram sends a fire-and-forget QUIC datagram to addr.
// The payload should already include the message type prefix byte.
func (t *Transport) SendDatagram(addr string, payload []byte) error {
	conn, err := t.getOrDial(context.Background(), addr)
	if err != nil {
		return err
	}
	return conn.SendDatagram(payload)
}

// OpenStream opens a bidirectional QUIC stream to addr.
func (t *Transport) OpenStream(ctx context.Context, addr string) (*quic.Stream, error) {
	conn, err := t.getOrDial(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		// Evict the connection so the next attempt re-dials fresh.
		// A healthy connection opens streams in <1ms; if it timed out
		// the connection is likely half-open or broken.
		t.appConns.CompareAndDelete(addr, conn)
		conn.CloseWithError(0, "stream open failed")
		return nil, fmt.Errorf("open stream to %s: %w", addr, err)
	}
	return s, nil
}

// OpenUniStream opens a unidirectional send stream to addr.
func (t *Transport) OpenUniStream(ctx context.Context, addr string) (*quic.SendStream, error) {
	conn, err := t.getOrDial(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	s, err := conn.OpenUniStreamSync(ctx)
	if err != nil {
		t.appConns.CompareAndDelete(addr, conn)
		conn.CloseWithError(0, "uni stream open failed")
		return nil, fmt.Errorf("open uni stream to %s: %w", addr, err)
	}
	return s, nil
}

// GetConnection returns an existing application connection to addr, or nil.
func (t *Transport) GetConnection(addr string) *quic.Conn {
	val, ok := t.appConns.Load(addr)
	if !ok {
		return nil
	}
	conn := val.(*quic.Conn)
	if conn.Context().Err() != nil {
		t.appConns.Delete(addr)
		return nil
	}
	return conn
}

// AddConnection registers an externally established QUIC connection
// (e.g., from hole punching or relay) in the app transport and starts
// accept loops on it.
func (t *Transport) AddConnection(conn *quic.Conn) {
	addr := conn.RemoteAddr().String()
	t.appConns.Store(addr, conn)
	t.startAcceptLoop(conn)
}

// QUICTransport returns the underlying QUIC transport. This is needed for
// hole punching — outgoing dials must originate from the same UDP socket
// as the listener so the NAT mapping is preserved.
func (t *Transport) QUICTransport() *quic.Transport {
	return t.transport
}

// Shutdown closes the transport and waits for goroutines to finish.
func (t *Transport) Shutdown() error {
	close(t.shutdownCh)
	t.listener.Close()
	t.appConns.Range(func(key, value any) bool {
		conn := value.(*quic.Conn)
		conn.CloseWithError(0, "transport shutdown")
		t.appConns.Delete(key)
		return true
	})
	t.transport.Close()
	t.wg.Wait()
	return nil
}

// Addr returns the listener address.
func (t *Transport) Addr() net.Addr {
	return t.listener.Addr()
}

// getOrDial returns an existing application connection or dials a new one.
func (t *Transport) getOrDial(ctx context.Context, addr string) (*quic.Conn, error) {
	if conn := t.GetConnection(addr); conn != nil {
		return conn, nil
	}

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}

	tlsConf := t.tlsConfig.Clone()
	tlsConf.ServerName = udpAddr.IP.String()

	conn, err := t.transport.Dial(ctx, udpAddr, tlsConf, t.quicConfig)
	if err != nil {
		return nil, err
	}

	// Store, but if another goroutine raced us, prefer the existing one.
	actual, loaded := t.appConns.LoadOrStore(addr, conn)
	if loaded {
		// Another goroutine already stored a connection. Close ours and use theirs.
		existing := actual.(*quic.Conn)
		if existing.Context().Err() == nil {
			conn.CloseWithError(0, "duplicate dial")
			return existing, nil
		}
		// Existing is dead, replace it.
		t.appConns.Store(addr, conn)
	}

	// Start accept loops for inbound traffic on this outbound connection.
	t.startAcceptLoop(conn)

	return conn, nil
}

// acceptLoop accepts inbound QUIC connections from peers.
func (t *Transport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.listener.Accept(context.Background())
		if err != nil {
			select {
			case <-t.shutdownCh:
				return
			default:
				t.logger.Printf("[ERR] gossip-mesh: accept error: %v", err)
				continue
			}
		}

		addr := conn.RemoteAddr().String()
		t.appConns.Store(addr, conn)
		t.startAcceptLoop(conn)
	}
}

// startAcceptLoop starts goroutines that accept bidirectional streams,
// unidirectional streams, and datagrams on a connection, dispatching
// each to the appropriate handler based on the first type byte.
func (t *Transport) startAcceptLoop(conn *quic.Conn) {
	t.wg.Add(3)
	go t.acceptBidiStreams(conn)
	go t.acceptUniStreams(conn)
	go t.receiveDatagrams(conn)
}

// StartAcceptLoopAll starts accept loops on all connections currently
// in the memberlist ConnPool. Useful at startup to begin accepting
// application traffic on connections that memberlist already established.
func (t *Transport) StartAcceptLoopAll() {
	if t.memberlistPool == nil {
		return
	}
	t.memberlistPool.Range(func(addr string, _ *quic.Conn) bool {
		// We use our own connections; knowing the address lets us
		// dial lazily when needed.
		return true
	})
}

func (t *Transport) acceptBidiStreams(conn *quic.Conn) {
	defer t.wg.Done()
	from := conn.RemoteAddr().String()
	for {
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			return
		}
		go t.dispatchBidiStream(from, stream)
	}
}

func (t *Transport) acceptUniStreams(conn *quic.Conn) {
	defer t.wg.Done()
	from := conn.RemoteAddr().String()
	for {
		stream, err := conn.AcceptUniStream(context.Background())
		if err != nil {
			return
		}
		go t.dispatchUniStream(from, stream)
	}
}

func (t *Transport) receiveDatagrams(conn *quic.Conn) {
	defer t.wg.Done()
	from := conn.RemoteAddr().String()
	for {
		msg, err := conn.ReceiveDatagram(context.Background())
		if err != nil {
			return
		}
		if len(msg) < 1 {
			continue
		}
		switch msg[0] {
		case MsgTypeEagerGossip:
			if h := t.getEagerGossip(); h != nil {
				h(from, msg[1:])
			}
		default:
			t.handlerMu.RLock()
			h, ok := t.datagramHandlers[msg[0]]
			t.handlerMu.RUnlock()
			if ok {
				h(from, msg[1:])
			} else {
				t.logger.Printf("[WARN] gossip-mesh: unknown datagram type 0x%02x from %s", msg[0], from)
			}
		}
	}
}

func (t *Transport) dispatchBidiStream(from string, stream *quic.Stream) {
	var typeBuf [1]byte
	if _, err := io.ReadFull(stream, typeBuf[:]); err != nil {
		stream.Close()
		return
	}
	switch typeBuf[0] {
	case StreamTypeRepair:
		if h := t.getRepair(); h != nil {
			h(from, stream)
		} else {
			stream.Close()
		}
	case MsgTypeEagerGossip:
		// Eager gossip can also arrive as a short-lived bidi stream
		// (fallback when datagrams are unavailable or payload too large).
		if h := t.getEagerGossip(); h != nil {
			data, err := io.ReadAll(stream)
			if err == nil {
				h(from, data)
			}
		}
		stream.Close()
	default:
		t.handlerMu.RLock()
		h, ok := t.bidiHandlers[typeBuf[0]]
		t.handlerMu.RUnlock()
		if ok {
			h(from, stream)
		} else {
			t.logger.Printf("[WARN] gossip-mesh: unknown bidi stream type 0x%02x from %s", typeBuf[0], from)
			stream.Close()
		}
	}
}

func (t *Transport) dispatchUniStream(from string, stream *quic.ReceiveStream) {
	var typeBuf [1]byte
	if _, err := io.ReadFull(stream, typeBuf[:]); err != nil {
		return
	}
	switch typeBuf[0] {
	case StreamTypeLazyBatch:
		if h := t.getLazyBatch(); h != nil {
			h(from, stream)
		}
	default:
		t.handlerMu.RLock()
		h, ok := t.uniHandlers[typeBuf[0]]
		t.handlerMu.RUnlock()
		if ok {
			h(from, stream)
		} else {
			t.logger.Printf("[WARN] gossip-mesh: unknown uni stream type 0x%02x from %s", typeBuf[0], from)
		}
	}
}
