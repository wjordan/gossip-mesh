# gossip-mesh

A Go library for efficient, reliable, low-latency message broadcast across clusters of 1 to 1000+ nodes, built on [QUIC](https://github.com/quic-go/quic-go).

## The problem

Broadcasting a message to every node in a cluster is straightforward when the cluster is small: open a connection to each peer and send. But full-mesh broadcast scales poorly. At N nodes, each message costs O(N) sends at the source, and the source becomes a bottleneck. If the sender crashes mid-broadcast, some nodes get the message and others don't.

**Gossip protocols** solve this by having each node forward messages to a small, random subset of peers. No single node needs to know the full membership or act as a central relay. Messages spread epidemically -- like a rumor -- reaching the entire cluster in O(log N) hops. The protocol tolerates node failures naturally because there is no single point of failure: if one forwarding path breaks, another peer will deliver the message. This makes gossip a good foundation for clusters that are large, dynamic (nodes joining and leaving), or where you can't rely on stable infrastructure.

The tradeoff is redundancy. In classic epidemic gossip, each node receives the same message from multiple peers -- roughly O(ln N) redundant copies -- because every forwarder picks peers independently without knowing who has already received the message. Per-node bandwidth grows with cluster size.

**[Plumtree](https://repositorium.sdum.uminho.pt/handle/1822/38894)** (Leitão, Pereira, Rodrigues -- *"Epidemic Broadcast Trees"*, IEEE SRDS 2007) eliminates this redundancy by splitting peers into two sets:

- **Eager peers** receive every message immediately (push). These form a spanning tree through the cluster, so each message is delivered exactly once along the tree edges -- zero redundancy on the fast path.
- **Lazy peers** receive only lightweight summaries (message IDs). If a node detects it missed a message (a gap in the sequence, or an ID it hasn't seen), it pulls the full payload from a lazy peer. This repairs tree partitions without manual intervention.

The result is a protocol with the latency of tree-based multicast and the reliability of epidemic gossip.

## What gossip-mesh adds

gossip-mesh implements the Plumtree eager/lazy split over QUIC, with several additions for **multi-region clusters** -- the common case where groups of instances are deployed across globally distributed datacenters. In this setting, intra-region latency is sub-millisecond while cross-region latency is 50-200ms, and bandwidth between regions is expensive. A gossip protocol needs to be aware of this topology: it should prefer local peers for eager delivery (low latency), ensure every region has at least one eager link (so no region is stuck waiting for a lazy repair round-trip), and keep cross-region fan-out bounded (so bandwidth costs don't scale with cluster size).

- **RTT-aware peer classification.** Peers are grouped into latency clusters using [Vivaldi](https://github.com/wjordan/vivaldi) network coordinates (decentralized RTT estimation -- no active probing). The overlay guarantees at least one eager peer per RTT cluster, so messages reach every region in a single eager hop even when the total eager budget is small. Without this, random peer selection can strand entire regions on the lazy path, adding a full repair round-trip of latency.

- **Bounded fan-out.** The overlay maintains ~8 eager + ~6 lazy peers regardless of cluster size. Per-node bandwidth is O(1). Eager messages use QUIC datagrams (unordered, no head-of-line blocking); lazy batches use compressed QUIC streams.

- **Ordered delivery with gap repair.** The engine tracks per-topic sequences, deduplicates, buffers out-of-order arrivals, and pulls missing messages from lazy peers on a configurable timeout. Applications receive messages in order per topic.

- **Cluster membership.** Built on [memberlist](https://github.com/hashicorp/memberlist) over QUIC, with Vivaldi coordinate tracking and extensible node metadata (`json.RawMessage` `AppData` field).

- **Application transport.** A separate QUIC listener for application-level traffic (streams and datagrams), with a handler registry for custom message types alongside the built-in gossip types.

### Packages

| Package | Role |
|---------|------|
| `membership` | Cluster join/leave, peer discovery, Vivaldi RTT estimation, extensible node metadata |
| `transport` | QUIC streams + datagrams, handler registry for application-defined message types |
| `overlay` | Eager/lazy peer classification with RTT clustering for cross-region connectivity |
| `engine` | Gossip event loop: enqueue, dedup, ordered delivery, lazy batching, gap repair |

## Install

```
go get github.com/wjordan/gossip-mesh
```

Requires Go 1.24+ and mutual TLS (the library does not manage certificates -- you provide a `*tls.Config`).

## Quick start

```go
package main

import (
    "crypto/tls"
    "fmt"
    "log"

    "github.com/wjordan/gossip-mesh/engine"
    "github.com/wjordan/gossip-mesh/membership"
    "github.com/wjordan/gossip-mesh/overlay"
    "github.com/wjordan/gossip-mesh/transport"
)

func main() {
    tlsCfg := loadTLSConfig() // your TLS setup

    // 1. Join the cluster.
    mem := &membership.Membership{}
    if err := mem.Start(membership.MembershipConfig{
        NodeID:    "node-1",
        BindAddr:  "0.0.0.0",
        BindPort:  7946,
        SeedAddrs: []string{"10.0.0.1:7946"},
        TLS:       tlsCfg,
        Meta: membership.NodeMeta{
            QUICAddr: "0.0.0.0",
            QUICPort: 7947,
        },
    }); err != nil {
        log.Fatal(err)
    }
    defer mem.Stop()

    // 2. Start the application transport.
    t, err := transport.New(transport.Config{
        BindAddr:       "0.0.0.0",
        BindPort:       7947,
        TLS:            mem.TLSConfig(),
        MemberlistPool: mem.ConnPool(),
    })
    if err != nil {
        log.Fatal(err)
    }
    defer t.Shutdown()

    // 3. Register application-specific stream handlers.
    t.RegisterBidiHandler(0x20, func(from string, s *quic.Stream) {
        // handle custom bidirectional stream
    })

    // 4. Build the overlay and gossip engine.
    ov := overlay.New("node-1", overlay.OverlayConfig{})
    ge := engine.New(ov, t, engine.EngineConfig{})

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    go ge.Run(ctx)

    // 5. Publish and consume gossip messages.
    ge.Enqueue(0, 1, []byte("hello cluster"))

    for entry := range ge.Deliver() {
        fmt.Printf("topic=%d seq=%d payload=%s\n",
            entry.Topic, entry.Seq, entry.Payload)
    }
}
```

## Packages

### membership

Cluster membership via memberlist over QUIC with Vivaldi coordinate tracking.

```go
mem := &membership.Membership{}
mem.Start(membership.MembershipConfig{
    NodeID:    "node-1",
    BindAddr:  "0.0.0.0",
    BindPort:  7946,
    SeedAddrs: []string{"10.0.0.1:7946"},
    TLS:       tlsCfg,
    Meta:      membership.NodeMeta{QUICAddr: "0.0.0.0", QUICPort: 7947},
})

mem.SetOnChange(func(peers []membership.PeerInfo) {
    for _, p := range peers {
        fmt.Printf("%s rtt=%v\n", p.NodeID, p.RTT)
    }
})
```

`NodeMeta` includes an `AppData` field (`json.RawMessage`) for application-specific metadata that rides on memberlist protocol messages without any extra network calls.

### transport

QUIC application transport separate from the memberlist gossip channel. Supports bidirectional streams, unidirectional streams, and datagrams.

Built-in stream types handle gossip-internal traffic (eager messages, lazy batches, repair). Applications register handlers for their own stream types:

```go
t.RegisterBidiHandler(0x20, myBidiHandler)
t.RegisterUniHandler(0x21, myUniHandler)
t.RegisterDatagramHandler(0x22, myDatagramHandler)
```

ALPN is configurable (default `gossip-mesh/1`).

### overlay

Eager/lazy peer classification. Peers are grouped by RTT into latency clusters, then eager slots are distributed to guarantee at least one eager peer per cluster. The total peer set is bounded (~8 eager + ~6 lazy = ~14) regardless of cluster size.

```go
ov := overlay.New("self", overlay.OverlayConfig{
    MaxEagerPeers: 8,
    MaxLazyPeers:  6,
})
ov.Reclassify(peers) // call when membership changes
```

### engine

Gossip event loop. Handles:
- **Enqueue**: publish a message to the cluster
- **Dedup**: `SeenTracker` provides O(1) per-topic sequence deduplication
- **Ordered delivery**: `OrderedApplier` buffers out-of-order messages and delivers in sequence, with configurable gap timeout
- **Lazy batching**: accumulates summaries for lazy peers, flushed on a timer
- **Gap repair**: detects missing messages from lazy summaries and requests repair via bidirectional QUIC stream

Messages are tagged with a `Topic` (uint16) for independent per-channel sequencing.

## Bandwidth scaling

Per-node gossip bandwidth is O(1) -- bounded by the overlay peer count (~14), not the cluster size. At 1000 nodes with 200-byte payloads, per-node overhead is ~2.9 KB/msg (~7.7x fan-out). Eager peers carry ~30% of traffic via low-latency datagrams; lazy peers carry the rest via compressed batch streams.

## Dependencies

- [memberlist](https://github.com/hashicorp/memberlist) -- cluster membership protocol
- [quic-go](https://github.com/quic-go/quic-go) -- QUIC transport
- [memberlist-quic](https://github.com/wjordan/memberlist-quic) -- QUIC transport adapter for memberlist
- [vivaldi](https://github.com/wjordan/vivaldi) -- Vivaldi network coordinate system for RTT estimation
- [compress](https://github.com/klauspost/compress) -- zstd compression for lazy batches

## Further reading

- [Epidemic Broadcast Trees](https://repositorium.sdum.uminho.pt/handle/1822/38894) (Leitão, Pereira, Rodrigues, 2007) -- the Plumtree paper
- [Vivaldi: A Decentralized Network Coordinate System](https://dl.acm.org/doi/10.1145/1030194.1015471) (Dabek et al., 2004) -- decentralized RTT estimation
- [QUIC: A UDP-Based Multiplexed and Secure Transport](https://www.rfc-editor.org/rfc/rfc9000) (RFC 9000)

## License

Apache 2.0. See [LICENSE](LICENSE).
