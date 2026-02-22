package holepunch

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/wjordan/gossip-mesh/objstore"
)

// PunchParams configures a hole-punch attempt.
type PunchParams struct {
	Store         objstore.ObjectStore
	QUICTransport *quic.Transport // must be the SAME transport as the listener
	TLSConfig     *tls.Config
	QUICConfig    *quic.Config
	SelfID        string
	TargetID      string
	SelfAddr      string        // our observed public addr
	PollInterval  time.Duration // default 500ms
	Timeout       time.Duration // default 10s
}

// AttemptHolePunch coordinates a simultaneous QUIC open via S3 signaling.
// Both peers write their signal, poll for the other's signal, then dial
// each other. QUIC handles dual-connect resolution.
func AttemptHolePunch(ctx context.Context, params PunchParams) (*quic.Conn, error) {
	if params.PollInterval == 0 {
		params.PollInterval = 500 * time.Millisecond
	}
	if params.Timeout == 0 {
		params.Timeout = 10 * time.Second
	}

	// Generate a nonce for this attempt.
	nonceBytes := make([]byte, 8)
	if _, err := rand.Read(nonceBytes); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	nonce := hex.EncodeToString(nonceBytes)

	// Write our signal.
	sig := Signal{
		NodeID: params.SelfID,
		Addr:   params.SelfAddr,
		Nonce:  nonce,
	}
	if err := WriteSignalForPeer(ctx, params.Store, params.SelfID, params.TargetID, sig); err != nil {
		return nil, fmt.Errorf("write signal: %w", err)
	}

	// Clean up signals when done, regardless of outcome.
	defer func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = CleanupSignal(cleanCtx, params.Store, params.SelfID, params.TargetID)
	}()

	// Poll for peer's signal.
	deadline := time.Now().Add(params.Timeout)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	var peerSignal *Signal
	ticker := time.NewTicker(params.PollInterval)
	defer ticker.Stop()

	for peerSignal == nil {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("hole punch timed out waiting for peer signal")
		case <-ticker.C:
			_, peer, err := ReadSignals(ctx, params.Store, params.SelfID, params.TargetID)
			if err != nil {
				continue
			}
			if peer != nil {
				peerSignal = peer
			}
		}
	}

	// Both signals exist — attempt simultaneous dial.
	peerAddr, err := net.ResolveUDPAddr("udp", peerSignal.Addr)
	if err != nil {
		return nil, fmt.Errorf("resolve peer addr: %w", err)
	}

	tlsConf := params.TLSConfig.Clone()
	tlsConf.ServerName = peerAddr.IP.String()

	conn, err := params.QUICTransport.Dial(ctx, peerAddr, tlsConf, params.QUICConfig)
	if err != nil {
		return nil, fmt.Errorf("hole punch dial: %w", err)
	}

	return conn, nil
}
