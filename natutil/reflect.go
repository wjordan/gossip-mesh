package natutil

import (
	"context"
	"fmt"
	"time"

	"github.com/wjordan/gossip-mesh/transport"
)

const (
	reflectRequest  byte = 0x00
	reflectResponse byte = 0x01
)

// RegisterReflectHandler installs a datagram handler on the app transport
// that responds to address reflection requests with the observed remote
// address of the sender.
func RegisterReflectHandler(t *transport.Transport) {
	t.RegisterDatagramHandler(transport.MsgTypeAddrReflect, func(from string, data []byte) {
		if len(data) < 1 || data[0] != reflectRequest {
			return
		}
		// Respond with the observed remote address.
		resp := make([]byte, 0, 2+len(from))
		resp = append(resp, transport.MsgTypeAddrReflect, reflectResponse)
		resp = append(resp, []byte(from)...)
		_ = t.SendDatagram(from, resp)
	})
}

// RequestReflection asks a connected peer to reflect our observed public
// address back to us. This is used to detect NAT.
func RequestReflection(ctx context.Context, t *transport.Transport, peerAddr string) (string, error) {
	resultCh := make(chan string, 1)

	// Temporarily register a handler that captures the response.
	// We use a closure that writes to resultCh exactly once.
	origHandler := func(from string, data []byte) {
		if len(data) < 1 || data[0] != reflectResponse {
			return
		}
		addr := string(data[1:])
		select {
		case resultCh <- addr:
		default:
		}
	}
	t.RegisterDatagramHandler(transport.MsgTypeAddrReflect, func(from string, data []byte) {
		if len(data) < 1 {
			return
		}
		switch data[0] {
		case reflectRequest:
			// Also handle incoming requests while waiting.
			resp := make([]byte, 0, 2+len(from))
			resp = append(resp, transport.MsgTypeAddrReflect, reflectResponse)
			resp = append(resp, []byte(from)...)
			_ = t.SendDatagram(from, resp)
		case reflectResponse:
			origHandler(from, data)
		}
	})

	// Send reflection request.
	req := []byte{transport.MsgTypeAddrReflect, reflectRequest}
	if err := t.SendDatagram(peerAddr, req); err != nil {
		return "", fmt.Errorf("send reflection request: %w", err)
	}

	// Wait for response with timeout.
	timeout := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < timeout {
			timeout = remaining
		}
	}

	select {
	case addr := <-resultCh:
		return addr, nil
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(timeout):
		return "", fmt.Errorf("reflection request timed out")
	}
}
