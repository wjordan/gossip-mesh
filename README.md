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

- **S3-coordinated NAT traversal.** Nodes behind NATs (residential ISPs, CGNAT, corporate firewalls) can join the mesh using an S3-compatible object store for TLS bootstrap, peer discovery, hole punching, and relay. No additional infrastructure beyond the object store.

## Connectivity modes

gossip-mesh supports two connectivity modes that can be used independently or together:

**Direct mode** -- provide seed addresses and a TLS config. Nodes dial seeds on startup and discover the rest of the cluster through memberlist gossip. This is the fast path and works when nodes have direct IP reachability (same VPC, overlay network, public IPs).

**S3-coordinated mode** -- provide an `ObjectStore` implementation. Nodes bootstrap TLS from a shared CA in the object store, register themselves, and discover peers via S3. For peers behind NAT, the library escalates through hole punching (simultaneous QUIC open coordinated via S3 signaling) and relay (traffic forwarded through a public-IP node).

When both are provided, they layer naturally:

1. Seeds are joined immediately (fast path -- no S3 latency).
2. S3 discovery runs in parallel, finding peers that seeds can't reach.
3. For each S3-discovered peer not already known via gossip, try memberlist Join first (direct dial), then hole punch, then relay.
4. Once any connection exists, memberlist gossip propagates the rest of the membership -- S3 is no longer needed for that peer.
5. A background loop polls S3 periodically for new joiners that gossip hasn't discovered yet (e.g., nodes behind different NATs with no shared gossip neighbor).

S3 handles bootstrap only. SWIM (memberlist) handles steady-state membership. Gossip handles message dissemination. The object store is a side-channel for situations where the mesh can't self-heal.

### Object store key layout

```
{prefix}/
  ca/cert.pem                           # shared CA certificate
  ca/key.pem                            # CA private key (nodes self-sign)
  nodes/{node-id}                       # node registration JSON + heartbeat
  signal/{lower-id}/{higher-id}/{id}    # hole-punch signaling (ephemeral)
```

### NAT traversal escalation

For each peer discovered via S3 that isn't already known to SWIM:

1. **memberlist Join** -- try the peer's registered addresses directly. Works if the peer is reachable (same network, public IP, port-forwarded).
2. **Hole punch** -- both peers write signals to S3, poll for each other, then simultaneously dial from their listener socket. QUIC handles dual-connect resolution. Works for cone NAT (most residential/mobile NAT).
3. **Relay** -- route through a node with a public IP. The relay bridges two QUIC bidi streams. Works for symmetric NAT where hole punching fails.

### S3 heartbeat

Each node periodically re-PUTs its registration JSON (default every 30s) to keep the `Heartbeat` timestamp fresh. `DiscoverPeers` ignores registrations older than `StaleNodeAge` (default 2 min) and garbage-collects them. This gives registrations an effective TTL so new joiners don't waste time connecting to dead nodes. The heartbeat also picks up any updates to the registration (e.g., `PublicAddr` learned after NAT detection).

## Packages

| Package | Role |
|---------|------|
| `mesh` | Unified `Join()`/`Leave()` entry point -- wires everything together |
| `membership` | Cluster join/leave, peer discovery, Vivaldi RTT estimation, extensible node metadata |
| `transport` | QUIC streams + datagrams, handler registry for application-defined message types |
| `overlay` | Eager/lazy peer classification with RTT clustering for cross-region connectivity |
| `engine` | Gossip event loop: enqueue, dedup, ordered delivery, lazy batching, gap repair |
| `objstore` | `ObjectStore` interface + in-memory implementation for tests |
| `bootstrap` | TLS CA bootstrap, node registration, peer discovery via object store |
| `natutil` | Address reflection, NAT detection |
| `holepunch` | S3-signaled simultaneous QUIC open |
| `relay` | Relay server + client for symmetric NAT fallback |

## Install

```
go get github.com/wjordan/gossip-mesh
```

Requires Go 1.24+ and mutual TLS (bootstrapped from object store, or provided directly).

## Quick start

### Using the mesh package (recommended)

```go
package main

import (
    "fmt"
    "log"

    "github.com/wjordan/gossip-mesh/mesh"
    "github.com/wjordan/gossip-mesh/membership"
)

func main() {
    // S3-coordinated mode with seed fallback.
    m, err := mesh.Join(mesh.Config{
        NodeID:      "node-1",
        BindAddr:    "0.0.0.0",
        BindPort:    7946,
        SeedAddrs:   []string{"10.0.0.1:7946"}, // optional fast path
        ObjectStore: myS3Store,                   // enables NAT traversal
        Meta: membership.NodeMeta{
            QUICAddr: "0.0.0.0",
            QUICPort: 7947,
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer m.Leave()

    // Publish and consume gossip messages.
    m.Engine.Enqueue(0, 1, []byte("hello cluster"))

    for entry := range m.Engine.Deliver() {
        fmt.Printf("topic=%d seq=%d payload=%s\n",
            entry.Topic, entry.Seq, entry.Payload)
    }
}
```

### Direct mode (no object store)

```go
m, err := mesh.Join(mesh.Config{
    NodeID:    "node-1",
    BindAddr:  "0.0.0.0",
    BindPort:  7946,
    SeedAddrs: []string{"10.0.0.1:7946"},
    TLS:       tlsCfg, // your TLS setup
    Meta: membership.NodeMeta{
        QUICAddr: "0.0.0.0",
        QUICPort: 7947,
    },
})
```

### Using individual packages

The `mesh` package is optional. You can wire the individual packages yourself for full control:

```go
// 1. Join the cluster.
mem := &membership.Membership{}
mem.Start(membership.MembershipConfig{
    NodeID:    "node-1",
    BindAddr:  "0.0.0.0",
    BindPort:  7946,
    SeedAddrs: []string{"10.0.0.1:7946"},
    TLS:       tlsCfg,
    Meta:      membership.NodeMeta{QUICAddr: "0.0.0.0", QUICPort: 7947},
})
defer mem.Stop()

// 2. Start the application transport.
t, _ := transport.New(transport.Config{
    BindAddr:       "0.0.0.0",
    BindPort:       7947,
    TLS:            mem.TLSConfig(),
    MemberlistPool: mem.ConnPool(),
})
defer t.Shutdown()

// 3. Register custom stream handlers.
t.RegisterBidiHandler(0x20, myBidiHandler)

// 4. Build the overlay and gossip engine.
ov := overlay.New("node-1", overlay.OverlayConfig{})
ge := engine.New(ov, t, engine.EngineConfig{})
go ge.Run(ctx)
```

## Bandwidth scaling

Per-node gossip bandwidth is O(1) -- bounded by the overlay peer count (~14), not the cluster size. At 1000 nodes with 200-byte payloads, per-node overhead is ~2.9 KB/msg (~7.7x fan-out). Eager peers carry ~30% of traffic via low-latency datagrams; lazy peers carry the rest via compressed batch streams.

## Dependencies

- [memberlist](https://github.com/hashicorp/memberlist) -- cluster membership protocol
- [quic-go](https://github.com/quic-go/quic-go) -- QUIC transport
- [memberlist-quic](https://github.com/wjordan/memberlist-quic) -- QUIC transport adapter for memberlist
- [vivaldi](https://github.com/wjordan/vivaldi) -- Vivaldi network coordinate system for RTT estimation
- [compress](https://github.com/klauspost/compress) -- zstd compression for lazy batches

The `objstore` package defines the `ObjectStore` interface but includes no S3 SDK dependency. Callers bring their own implementation (or use `MemoryObjectStore` for tests).

## Further reading

- [Epidemic Broadcast Trees](https://repositorium.sdum.uminho.pt/handle/1822/38894) (Leitão, Pereira, Rodrigues, 2007) -- the Plumtree paper
- [Vivaldi: A Decentralized Network Coordinate System](https://dl.acm.org/doi/10.1145/1030194.1015471) (Dabek et al., 2004) -- decentralized RTT estimation
- [QUIC: A UDP-Based Multiplexed and Secure Transport](https://www.rfc-editor.org/rfc/rfc9000) (RFC 9000)

## License

Apache 2.0. See [LICENSE](LICENSE).
