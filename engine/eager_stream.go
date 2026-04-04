package engine

import (
	"context"
	"encoding/binary"
	"io"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/wjordan/gossip-mesh/transport"
)

// StreamTypeEagerStream is the stream type prefix for persistent per-peer
// eager gossip streams. Unlike datagram-based eager forwarding, these streams
// carry length-prefixed messages and stay open across multiple sends, avoiding
// the overhead of stream-per-message at high cadence.
const StreamTypeEagerStream byte = 0x12

// maxDatagramPayload is the maximum eager message size that fits in a QUIC
// datagram on typical networks (Fly WireGuard overlay: ~1200B MTU minus
// gossip header overhead).
const maxDatagramPayload = 1100

// eagerStreamWriter maintains a persistent uni-stream to a single peer for
// sending oversized eager messages. The stream is lazily opened on first
// write and recreated on error.
type eagerStreamWriter struct {
	mu        sync.Mutex
	transport *transport.Transport
	addr      string
	stream    *quic.SendStream
}

func (w *eagerStreamWriter) send(msg []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stream == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		s, err := w.transport.OpenUniStream(ctx, w.addr)
		if err != nil {
			return err
		}
		// Write stream type prefix once at the start of the stream.
		if _, err := s.Write([]byte{StreamTypeEagerStream}); err != nil {
			s.Close()
			return err
		}
		w.stream = s
	}

	// Length-prefixed message: [4B len BE][payload]
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(msg)))
	if _, err := w.stream.Write(hdr[:]); err != nil {
		w.stream.Close()
		w.stream = nil
		return err
	}
	if _, err := w.stream.Write(msg); err != nil {
		w.stream.Close()
		w.stream = nil
		return err
	}
	return nil
}

func (w *eagerStreamWriter) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stream != nil {
		w.stream.Close()
		w.stream = nil
	}
}

// receiveEagerStream reads length-prefixed eager messages from a persistent
// uni-stream and dispatches each one to the eager gossip handler.
func receiveEagerStream(r io.Reader, handler func(from string, data []byte), from string) {
	var hdr [4]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return // stream closed or error — peer will reconnect
		}
		msgLen := binary.BigEndian.Uint32(hdr[:])
		if msgLen > 16*1024*1024 { // 16MB sanity limit
			return
		}
		msg := make([]byte, msgLen)
		if _, err := io.ReadFull(r, msg); err != nil {
			return
		}
		// Strip the type byte prefix (0x02) — the eager handler expects
		// [2B topic][8B seq][payload], matching the datagram receive path
		// which strips the type byte at the transport layer.
		if len(msg) > 0 && msg[0] == transport.MsgTypeEagerGossip {
			msg = msg[1:]
		}
		handler(from, msg)
	}
}
