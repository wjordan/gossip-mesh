package engine

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wjordan/gossip-mesh/overlay"
	"github.com/wjordan/gossip-mesh/transport"
)

func testTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	pool := x509.NewCertPool()
	parsed, _ := x509.ParseCertificate(der)
	pool.AddCert(parsed)
	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            pool,
		ClientCAs:          pool,
		InsecureSkipVerify: true,
	}
}

// TestEagerStream_OversizedPayloadDelivered verifies that eager messages
// exceeding the datagram size limit are delivered via persistent per-peer
// streams. This is a red-green test for the eager stream feature.
func TestEagerStream_OversizedPayloadDelivered(t *testing.T) {
	// Stand up two transports.
	tlsCfg := testTLSConfig(t)
	tA, err := transport.New(transport.Config{BindAddr: "127.0.0.1:0", TLS: tlsCfg})
	if err != nil {
		t.Fatal(err)
	}
	defer tA.Shutdown()
	tB, err := transport.New(transport.Config{BindAddr: "127.0.0.1:0", TLS: tlsCfg})
	if err != nil {
		t.Fatal(err)
	}
	defer tB.Shutdown()

	addrA := tA.Addr().String()
	addrB := tB.Addr().String()

	// Create overlays: A sees B as eager, B sees A as eager.
	ovA := overlay.New("nodeA", overlay.OverlayConfig{})
	ovA.Reclassify([]overlay.PeerInfo{{NodeID: "nodeB", Addr: addrB, RTT: time.Millisecond}})

	ovB := overlay.New("nodeB", overlay.OverlayConfig{})
	ovB.Reclassify([]overlay.PeerInfo{{NodeID: "nodeA", Addr: addrA, RTT: time.Millisecond}})

	// Create engines.
	engA := New(ovA, tA, EngineConfig{DeliverBufferSize: 64})
	engB := New(ovB, tB, EngineConfig{DeliverBufferSize: 64})

	// Pre-seed SeenTrackers so the OrderedApplier knows where to start.
	const topic uint16 = 1
	engA.SetInitialSeq(topic, 0)
	engB.SetInitialSeq(topic, 0)

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	go engA.Run(ctxA)
	go engB.Run(ctxB)

	// Create a payload that exceeds the datagram limit.
	bigPayload := make([]byte, 4096) // well over 1100B limit
	for i := range bigPayload {
		bigPayload[i] = byte(i % 256)
	}

	// Enqueue on A — should forward to B via persistent eager stream.
	engA.Enqueue(topic, 1, bigPayload)

	// Wait for B to deliver.
	var delivered atomic.Bool
	var receivedPayload []byte
	go func() {
		select {
		case entry := <-engB.Deliver():
			receivedPayload = entry.Payload
			delivered.Store(true)
		case <-ctxB.Done():
		}
	}()

	deadline := time.After(3 * time.Second)
	for !delivered.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for oversized eager delivery via stream")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	if len(receivedPayload) != len(bigPayload) {
		t.Fatalf("payload length: got %d, want %d", len(receivedPayload), len(bigPayload))
	}
	for i := range bigPayload {
		if receivedPayload[i] != bigPayload[i] {
			t.Fatalf("payload mismatch at byte %d: got 0x%02x, want 0x%02x", i, receivedPayload[i], bigPayload[i])
		}
	}
}

// TestEagerStream_SmallPayloadUsesDatagram verifies that small payloads
// still use the datagram path (no stream overhead).
func TestEagerStream_SmallPayloadUsesDatagram(t *testing.T) {
	tlsCfg := testTLSConfig(t)
	tA, err := transport.New(transport.Config{BindAddr: "127.0.0.1:0", TLS: tlsCfg})
	if err != nil {
		t.Fatal(err)
	}
	defer tA.Shutdown()
	tB, err := transport.New(transport.Config{BindAddr: "127.0.0.1:0", TLS: tlsCfg})
	if err != nil {
		t.Fatal(err)
	}
	defer tB.Shutdown()

	addrA := tA.Addr().String()
	addrB := tB.Addr().String()

	ovA := overlay.New("nodeA", overlay.OverlayConfig{})
	ovA.Reclassify([]overlay.PeerInfo{{NodeID: "nodeB", Addr: addrB, RTT: time.Millisecond}})
	ovB := overlay.New("nodeB", overlay.OverlayConfig{})
	ovB.Reclassify([]overlay.PeerInfo{{NodeID: "nodeA", Addr: addrA, RTT: time.Millisecond}})

	engA := New(ovA, tA, EngineConfig{DeliverBufferSize: 64})
	engB := New(ovB, tB, EngineConfig{DeliverBufferSize: 64})

	const topic uint16 = 1
	engA.SetInitialSeq(topic, 0)
	engB.SetInitialSeq(topic, 0)

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	go engA.Run(ctxA)
	go engB.Run(ctxB)

	// Small payload — should fit in a datagram.
	smallPayload := []byte("hello world")
	engA.Enqueue(topic, 1, smallPayload)

	var delivered atomic.Bool
	var receivedPayload []byte
	go func() {
		select {
		case entry := <-engB.Deliver():
			receivedPayload = entry.Payload
			delivered.Store(true)
		case <-ctxB.Done():
		}
	}()

	deadline := time.After(3 * time.Second)
	for !delivered.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for small eager delivery via datagram")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	if string(receivedPayload) != string(smallPayload) {
		t.Fatalf("payload: got %q, want %q", receivedPayload, smallPayload)
	}
}

