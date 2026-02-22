package relay

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/wjordan/gossip-mesh/transport"
)

// DialViaRelay opens a relayed connection to a target node through a relay node.
// The relay node must be running a relay.Server.
func DialViaRelay(ctx context.Context, t *transport.Transport, relayAddr, targetNodeID string) (*quic.Stream, error) {
	stream, err := t.OpenStream(ctx, relayAddr)
	if err != nil {
		return nil, fmt.Errorf("open stream to relay %s: %w", relayAddr, err)
	}

	// Write relay request: [StreamTypeRelay][targetNodeIDLen][targetNodeID]
	header := make([]byte, 0, 2+len(targetNodeID))
	header = append(header, transport.StreamTypeRelay, byte(len(targetNodeID)))
	header = append(header, []byte(targetNodeID)...)
	if _, err := stream.Write(header); err != nil {
		stream.Close()
		return nil, fmt.Errorf("write relay header: %w", err)
	}

	return stream, nil
}

// StreamConn wraps a QUIC stream as a net.Conn for use with memberlist
// or other code expecting a standard connection interface.
type StreamConn struct {
	Stream     *quic.Stream
	LocalAddr_ net.Addr
	RemoteAddr_ net.Addr
}

func (c *StreamConn) Read(b []byte) (int, error)  { return c.Stream.Read(b) }
func (c *StreamConn) Write(b []byte) (int, error) { return c.Stream.Write(b) }

func (c *StreamConn) Close() error {
	c.Stream.CancelRead(0)
	return c.Stream.Close()
}

func (c *StreamConn) LocalAddr() net.Addr  { return c.LocalAddr_ }
func (c *StreamConn) RemoteAddr() net.Addr { return c.RemoteAddr_ }

func (c *StreamConn) SetDeadline(t time.Time) error {
	if err := c.Stream.SetReadDeadline(t); err != nil {
		return err
	}
	return c.Stream.SetWriteDeadline(t)
}

func (c *StreamConn) SetReadDeadline(t time.Time) error {
	return c.Stream.SetReadDeadline(t)
}

func (c *StreamConn) SetWriteDeadline(t time.Time) error {
	return c.Stream.SetWriteDeadline(t)
}

var _ net.Conn = (*StreamConn)(nil)
var _ io.ReadWriteCloser = (*StreamConn)(nil)
