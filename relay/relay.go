package relay

import (
	"io"
	"log"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/wjordan/gossip-mesh/transport"
)

// Server relays traffic between NATed peers that can't reach each other
// directly. It runs on nodes with public IPs.
type Server struct {
	transport *transport.Transport
	logger    *log.Logger
}

// NewServer creates a relay server and registers the relay stream handler
// on the given transport.
func NewServer(t *transport.Transport, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	s := &Server{
		transport: t,
		logger:    logger,
	}
	t.RegisterBidiHandler(transport.StreamTypeRelay, s.handleRelay)
	return s
}

// handleRelay handles an incoming relay request.
// Protocol: client sends [StreamTypeRelay][nodeID-len:1B][target-nodeID]
// The relay connects to the target and bridges the two streams.
func (s *Server) handleRelay(from string, stream *quic.Stream) {
	defer stream.Close()

	// Read target node ID length.
	var lenBuf [1]byte
	if _, err := io.ReadFull(stream, lenBuf[:]); err != nil {
		s.logger.Printf("[ERR] relay: read target ID length from %s: %v", from, err)
		return
	}
	idLen := int(lenBuf[0])
	if idLen == 0 || idLen > 255 {
		s.logger.Printf("[ERR] relay: invalid target ID length %d from %s", idLen, from)
		return
	}

	// Read target node ID.
	idBuf := make([]byte, idLen)
	if _, err := io.ReadFull(stream, idBuf); err != nil {
		s.logger.Printf("[ERR] relay: read target ID from %s: %v", from, err)
		return
	}
	targetNodeID := string(idBuf)

	// Find connection to target in our app transport pool.
	targetConn := s.transport.GetConnection(findAddrForNode(s.transport, targetNodeID))
	if targetConn == nil {
		s.logger.Printf("[WARN] relay: no connection to target %s requested by %s", targetNodeID, from)
		return
	}

	// Open a relay stream to the target.
	targetStream, err := targetConn.OpenStreamSync(stream.Context())
	if err != nil {
		s.logger.Printf("[ERR] relay: open stream to target %s: %v", targetNodeID, err)
		return
	}
	defer targetStream.Close()

	// Write the relay header to the target: [StreamTypeRelay][sourceNodeIDLen][sourceNodeID]
	// We need to figure out the source node ID from the 'from' address.
	// For simplicity, we forward the from address as the source identifier.
	sourceID := from
	header := make([]byte, 0, 2+len(sourceID))
	header = append(header, transport.StreamTypeRelay, byte(len(sourceID)))
	header = append(header, []byte(sourceID)...)
	if _, err := targetStream.Write(header); err != nil {
		s.logger.Printf("[ERR] relay: write header to target %s: %v", targetNodeID, err)
		return
	}

	// Bidirectionally copy between the two streams.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(stream, targetStream)
		stream.CancelRead(0)
	}()
	go func() {
		defer wg.Done()
		io.Copy(targetStream, stream)
		targetStream.CancelRead(0)
	}()
	wg.Wait()
}

// findAddrForNode iterates over known connections to find one whose remote
// address is associated with the given node ID. This is a best-effort lookup.
func findAddrForNode(t *transport.Transport, nodeID string) string {
	// The transport stores connections by address, not by node ID.
	// In the mesh integration, the caller typically provides the target's
	// address directly. For the relay server, we need to find it.
	// This is a placeholder — the mesh package will provide proper node→addr mapping.
	_ = nodeID
	return ""
}
