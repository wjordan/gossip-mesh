// Package engine implements the Plumtree-inspired gossip event loop for
// page invalidation dissemination. It replaces NATS pub/sub with two-tier
// gossip: eager peers receive immediate QUIC datagram pushes, lazy peers
// receive batched QUIC stream deliveries. Lost datagrams are recovered
// via pull-based repair when a gap is detected.
package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/wjordan/gossip-mesh/overlay"
	"github.com/wjordan/gossip-mesh/transport"
)

// EngineStats holds gossip engine counters for observability.
type EngineStats struct {
	EagerRecv    int64 // eager messages received
	EagerDup     int64 // eager messages already seen (duplicates)
	LazyRecv     int64 // lazy batch entries received
	LazyDup      int64 // lazy entries already seen
	LocalEnqueue int64 // locally enqueued entries
	DeliverDrop  int64 // entries dropped because deliver channel was full
	DeliverCount int64 // entries successfully delivered to consumer
	EagerSend    int64 // eager datagrams sent
	EagerFail    int64 // eager datagram send failures
	LazySend     int64 // lazy batches sent
	LazyFail     int64 // lazy batch send failures
	// Wire-level byte counters (transport payload, before QUIC framing).
	EagerBytesSent int64 // eager datagram + stream bytes out
	LazyBytesSent  int64 // lazy batch bytes out (compressed)
	BytesRecv      int64 // all gossip bytes received (eager + lazy)
}

// GossipEntry is a single gossip message (page invalidation, checkpoint, etc.).
type GossipEntry struct {
	Topic   uint16
	Seq     uint64
	Payload []byte // PageInvalidation or checkpoint data, unchanged format
}

// DeliveredEntry is an entry delivered to the SyncEngine for processing.
type DeliveredEntry struct {
	Topic   uint16
	Seq     uint64
	Payload []byte
}

// EngineConfig configures the gossip engine.
type EngineConfig struct {
	LazyBatchInterval time.Duration // default 50ms
	GapTimeout        time.Duration // default 500ms
	MaxGapBuffer      int           // default 64
	DeliverBufferSize int           // default 256
}

func (c *EngineConfig) withDefaults() {
	if c.LazyBatchInterval <= 0 {
		c.LazyBatchInterval = 50 * time.Millisecond
	}
	if c.GapTimeout <= 0 {
		c.GapTimeout = 500 * time.Millisecond
	}
	if c.MaxGapBuffer <= 0 {
		c.MaxGapBuffer = 64
	}
	if c.DeliverBufferSize <= 0 {
		c.DeliverBufferSize = 256
	}
}

// GossipEngine is the main gossip event loop. It accepts local
// invalidations via Enqueue(), disseminates them to peers via the
// overlay, and delivers received entries to the SyncEngine via Deliver().
type GossipEngine struct {
	overlay   *overlay.Overlay
	transport *transport.Transport
	seen      *SeenTracker
	applier   *OrderedApplier
	repair    *RepairManager
	cfg       EngineConfig

	// Enqueue channel for local invalidations.
	localCh chan GossipEntry

	// Lazy batch accumulator (per lazy peer).
	lazyMu      sync.Mutex
	lazyBuffers map[string]*LazyBuffer // nodeID → buffer

	// Persistent eager streams for oversized payloads (per peer addr).
	eagerStreamMu sync.Mutex
	eagerStreams   map[string]*eagerStreamWriter // addr → writer

	// Output to SyncEngine.
	deliverCh chan DeliveredEntry
	done      chan struct{} // closed when Run exits

	// Observability counters.
	eagerRecv    atomic.Int64
	eagerDup     atomic.Int64
	lazyRecv     atomic.Int64
	lazyDup      atomic.Int64
	localEnqueue atomic.Int64
	deliverDrop  atomic.Int64
	deliverCount atomic.Int64 // messages successfully delivered to consumer
	eagerSend    atomic.Int64
	eagerFail    atomic.Int64
	lazySend     atomic.Int64
	lazyFail     atomic.Int64
	eagerBytesSent atomic.Int64
	lazyBytesSent  atomic.Int64
	bytesRecv      atomic.Int64
}

// Stats returns a snapshot of engine counters.
func (g *GossipEngine) Stats() EngineStats {
	return EngineStats{
		EagerRecv:    g.eagerRecv.Load(),
		EagerDup:     g.eagerDup.Load(),
		LazyRecv:     g.lazyRecv.Load(),
		LazyDup:      g.lazyDup.Load(),
		LocalEnqueue: g.localEnqueue.Load(),
		DeliverDrop:  g.deliverDrop.Load(),
		DeliverCount: g.deliverCount.Load(),
		EagerSend:      g.eagerSend.Load(),
		EagerFail:      g.eagerFail.Load(),
		LazySend:       g.lazySend.Load(),
		LazyFail:       g.lazyFail.Load(),
		EagerBytesSent: g.eagerBytesSent.Load(),
		LazyBytesSent:  g.lazyBytesSent.Load(),
		BytesRecv:      g.bytesRecv.Load(),
	}
}

// StatsString returns a one-line summary of engine counters.
func (g *GossipEngine) StatsString() string {
	s := g.Stats()
	return fmt.Sprintf("eager(recv=%d dup=%d send=%d fail=%d %dKB) lazy(recv=%d dup=%d send=%d fail=%d %dKB) local=%d deliverDrop=%d recv=%dKB",
		s.EagerRecv, s.EagerDup, s.EagerSend, s.EagerFail, s.EagerBytesSent/1024,
		s.LazyRecv, s.LazyDup, s.LazySend, s.LazyFail, s.LazyBytesSent/1024,
		s.LocalEnqueue, s.DeliverDrop, s.BytesRecv/1024)
}

// New creates a GossipEngine.
func New(o *overlay.Overlay, t *transport.Transport, cfg EngineConfig) *GossipEngine {
	cfg.withDefaults()

	g := &GossipEngine{
		overlay:      o,
		transport:    t,
		seen:         &SeenTracker{},
		cfg:          cfg,
		localCh:      make(chan GossipEntry, 256),
		lazyBuffers:  make(map[string]*LazyBuffer),
		eagerStreams: make(map[string]*eagerStreamWriter),
		deliverCh:    make(chan DeliveredEntry, cfg.DeliverBufferSize),
		done:         make(chan struct{}),
	}

	// Create ordered applier that delivers to the output channel.
	// Non-blocking: if the consumer is slow (page fetch, rebase), entries
	// are dropped. The stream publisher's periodic full ticks recover
	// any dropped transient updates. Blocking here would stall the entire
	// gossip pipeline (QUIC handlers, eager forward, Enqueue).
	g.applier = NewOrderedApplier(func(topic uint16, seq uint64, payload []byte) {
		select {
		case g.deliverCh <- DeliveredEntry{Topic: topic, Seq: seq, Payload: payload}:
			g.deliverCount.Add(1)
		default:
			g.deliverDrop.Add(1)
		}
	})
	g.applier.maxGapBuffer = cfg.MaxGapBuffer
	g.applier.gapTimeout = cfg.GapTimeout

	g.repair = NewRepairManager(o, t, g.applier)

	// Attempt repair before the ordered applier skips a gap.
	g.applier.onGap = func(topic uint16, seq uint64) bool {
		return g.repair.RequestRepair(topic, seq)
	}

	// Register transport handlers (synchronized to avoid races with
	// transport goroutines that may already be running).
	t.SetHandlers(g.onEagerReceive, g.onLazyBatchReceive, g.repair.HandleRequest)

	// Register persistent eager stream handler for oversized payloads.
	// Messages arrive length-prefixed on a long-lived uni-stream and are
	// dispatched to the same onEagerReceive handler as datagrams.
	t.RegisterUniHandler(StreamTypeEagerStream, func(from string, stream *quic.ReceiveStream) {
		receiveEagerStream(stream, g.onEagerReceive, from)
	})

	return g
}

// Run starts the gossip engine event loop. Blocks until ctx is cancelled.
func (g *GossipEngine) Run(ctx context.Context) {
	defer close(g.done)
	lazyTick := time.NewTicker(g.cfg.LazyBatchInterval)
	defer lazyTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case entry := <-g.localCh:
			g.handleLocal(entry)

		case <-lazyTick.C:
			g.flushLazyBatches()
		}
	}
}

// Enqueue submits a local invalidation for gossip dissemination.
// Blocks if the internal buffer is full (backpressure from peers).
func (g *GossipEngine) Enqueue(topic uint16, seq uint64, payload []byte) {
	g.localCh <- GossipEntry{
		Topic:   topic,
		Seq:     seq,
		Payload: payload,
	}
}

// Deliver returns the channel that SyncEngine reads from.
func (g *GossipEngine) Deliver() <-chan DeliveredEntry {
	return g.deliverCh
}

// Seen returns the SeenTracker for external use (e.g., setting initial seqs).
func (g *GossipEngine) Seen() *SeenTracker {
	return g.seen
}

// SetInitialSeq primes both the SeenTracker and OrderedApplier for a topic.
// Call this after bootstrap with the current manifest seq so incoming
// messages aren't dropped as "gap too large" by the ordered applier.
func (g *GossipEngine) SetInitialSeq(topic uint16, seq uint64) {
	g.seen.SetTopicSeq(topic, seq)
	g.applier.SetNextSeq(topic, seq+1) // expect the NEXT seq after current
}

// handleLocal processes a locally-produced entry.
func (g *GossipEngine) handleLocal(entry GossipEntry) {
	g.localEnqueue.Add(1)

	// Mark as seen and cache for repair serving.
	g.seen.ShouldProcess(entry.Topic, entry.Seq)
	g.repair.CacheEntry(entry)

	// Eager forward to all eager peers.
	g.eagerForward(entry, "")

	// Enqueue for lazy batch to all lazy peers.
	g.enqueueForLazy(entry, "")
}

// onEagerReceive is called by Transport when an eager gossip
// message arrives.
func (g *GossipEngine) onEagerReceive(from string, data []byte) {
	g.bytesRecv.Add(int64(len(data)))
	entry, err := decodeEagerMessage(data)
	if err != nil {
		return
	}

	if !g.seen.ShouldProcess(entry.Topic, entry.Seq) {
		g.eagerDup.Add(1)
		return // already seen via another path
	}
	g.eagerRecv.Add(1)
	g.repair.CacheEntry(entry)

	// Deliver to SyncEngine (via ordered applier).
	g.applier.Receive(entry.Topic, entry.Seq, entry.Payload)

	// Forward to OUR eager peers (excluding sender).
	g.eagerForward(entry, from)

	// Enqueue for OUR lazy peers (excluding sender).
	g.enqueueForLazy(entry, from)
}

// onLazyBatchReceive is called by Transport when a lazy batch arrives.
func (g *GossipEngine) onLazyBatchReceive(from string, reader io.Reader) {
	batch, err := ReadLazyBatch(reader)
	if err != nil {
		log.Printf("gossip: decode lazy batch from %s: %v", from, err)
		return
	}

	// Update our knowledge of what the sender has seen.
	g.lazyMu.Lock()
	buf, ok := g.lazyBuffers[from]
	if ok {
		buf.UpdatePeerSeqs(batch.SenderHighWaterMarks)
	}
	g.lazyMu.Unlock()

	// Process each entry.
	for _, entry := range batch.Entries {
		if !g.seen.ShouldProcess(entry.Topic, entry.Seq) {
			g.lazyDup.Add(1)
			continue
		}
		g.lazyRecv.Add(1)
		g.repair.CacheEntry(entry)
		g.applier.Receive(entry.Topic, entry.Seq, entry.Payload)
		// Forward to OUR eager peers (excluding sender).
		g.eagerForward(entry, from)
		// Enqueue for OUR lazy peers (excluding sender).
		g.enqueueForLazy(entry, from)
	}
}

// eagerForward sends an entry to all eager peers (except excludeNodeID).
// Small payloads use fire-and-forget datagrams. Large payloads (exceeding
// the QUIC datagram MTU) use a persistent per-peer uni-stream that avoids
// the overhead of opening a new stream per message.
func (g *GossipEngine) eagerForward(entry GossipEntry, excludeNodeID string) {
	peers := g.overlay.EagerPeers()

	msg := encodeEagerMessage(entry)
	useStream := len(msg) > maxDatagramPayload
	for _, peer := range peers {
		if peer.NodeID == excludeNodeID {
			continue
		}
		if useStream {
			if err := g.getEagerStream(peer.Addr).send(msg); err != nil {
				if n := g.eagerFail.Add(1); n == 1 || n%100 == 0 {
					log.Printf("gossip: eager stream send to %s (%s) failed (#%d): %v", peer.NodeID, peer.Addr, n, err)
				}
			} else {
				g.eagerSend.Add(1)
				g.eagerBytesSent.Add(int64(len(msg) + 5)) // +1 type +4 length header
			}
		} else {
			if err := g.transport.SendDatagram(peer.Addr, msg); err != nil {
				if n := g.eagerFail.Add(1); n == 1 || n%100 == 0 {
					log.Printf("gossip: eager send to %s (%s) failed (#%d): %v", peer.NodeID, peer.Addr, n, err)
				}
			} else {
				g.eagerSend.Add(1)
				g.eagerBytesSent.Add(int64(len(msg)))
			}
		}
	}
}

// getEagerStream returns the persistent eager stream writer for the given
// peer address, creating one if it doesn't exist.
func (g *GossipEngine) getEagerStream(addr string) *eagerStreamWriter {
	g.eagerStreamMu.Lock()
	defer g.eagerStreamMu.Unlock()
	w, ok := g.eagerStreams[addr]
	if !ok {
		w = &eagerStreamWriter{transport: g.transport, addr: addr}
		g.eagerStreams[addr] = w
	}
	return w
}

// enqueueForLazy adds an entry to lazy buffers for all lazy peers
// (except excludeNodeID).
func (g *GossipEngine) enqueueForLazy(entry GossipEntry, excludeNodeID string) {
	peers := g.overlay.LazyPeers()

	g.lazyMu.Lock()
	defer g.lazyMu.Unlock()

	for _, peer := range peers {
		if peer.NodeID == excludeNodeID {
			continue
		}
		buf, ok := g.lazyBuffers[peer.NodeID]
		if !ok {
			buf = NewLazyBuffer()
			g.lazyBuffers[peer.NodeID] = buf
		}
		buf.Add(entry)
	}
}

// flushLazyBatches sends accumulated lazy batches to all lazy peers.
func (g *GossipEngine) flushLazyBatches() {
	peers := g.overlay.LazyPeers()
	myMarks := g.seen.HighWaterMarks()

	g.lazyMu.Lock()
	// Collect non-empty batches.
	type peerBatch struct {
		addr  string
		batch LazyBatch
	}
	var batches []peerBatch
	for _, peer := range peers {
		buf, ok := g.lazyBuffers[peer.NodeID]
		if !ok || buf.Len() == 0 {
			continue
		}
		entries := buf.Flush()
		batches = append(batches, peerBatch{
			addr: peer.Addr,
			batch: LazyBatch{
				Entries:              entries,
				SenderHighWaterMarks: myMarks,
			},
		})
	}
	g.lazyMu.Unlock()

	// Send each batch (outside the lock).
	for _, pb := range batches {
		go g.sendLazyBatch(pb.addr, pb.batch)
	}
}

// sendLazyBatch sends a lazy batch to a peer via QUIC uni stream.
func (g *GossipEngine) sendLazyBatch(addr string, batch LazyBatch) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := g.transport.OpenUniStream(ctx, addr)
	if err != nil {
		if n := g.lazyFail.Add(1); n == 1 || n%100 == 0 {
			log.Printf("gossip: lazy send to %s failed (#%d): %v", addr, n, err)
		}
		return
	}
	defer stream.Close()

	// Write stream type prefix.
	if _, err := stream.Write([]byte{transport.StreamTypeLazyBatch}); err != nil {
		g.lazyFail.Add(1)
		return
	}

	compressed, err := EncodeLazyBatch(batch)
	if err != nil {
		g.lazyFail.Add(1)
		return
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(compressed)))
	if _, err := stream.Write(lenBuf[:]); err != nil {
		g.lazyFail.Add(1)
		return
	}
	if _, err := stream.Write(compressed); err != nil {
		g.lazyFail.Add(1)
		return
	}
	g.lazySend.Add(1)
	g.lazyBytesSent.Add(int64(1 + 4 + len(compressed))) // type + length + data
}

// Eager message encoding: [1B type][2B topic BE][8B seq BE][payload...]
func encodeEagerMessage(entry GossipEntry) []byte {
	msg := make([]byte, 1+2+8+len(entry.Payload))
	msg[0] = transport.MsgTypeEagerGossip
	binary.BigEndian.PutUint16(msg[1:], entry.Topic)
	binary.BigEndian.PutUint64(msg[3:], entry.Seq)
	copy(msg[11:], entry.Payload)
	return msg
}

func decodeEagerMessage(data []byte) (GossipEntry, error) {
	if len(data) < 10 {
		return GossipEntry{}, io.ErrUnexpectedEOF
	}
	return GossipEntry{
		Topic:   binary.BigEndian.Uint16(data[0:]),
		Seq:     binary.BigEndian.Uint64(data[2:]),
		Payload: data[10:],
	}, nil
}
