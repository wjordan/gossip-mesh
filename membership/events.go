package membership

import (
	"log"
	"time"

	"github.com/hashicorp/memberlist"
)

// eventDelegate implements memberlist.EventDelegate.
type eventDelegate struct {
	m *Membership
}

func (e *eventDelegate) NotifyJoin(node *memberlist.Node) {
	meta, err := DecodeMeta(node.Meta)
	if err != nil {
		log.Printf("membership: failed to decode meta for %s: %v", node.Name, err)
		return
	}

	var rtt time.Duration
	if meta.VivaldiCoord != nil && meta.VivaldiCoord.IsValid() {
		rtt = e.m.vivaldi.DistanceTo(meta.VivaldiCoord)
	}

	info := &PeerInfo{
		NodeID: node.Name,
		Addr:   node.Addr.String(),
		Meta:   *meta,
		RTT:    rtt,
		Alive:  true,
	}

	e.m.mu.Lock()
	e.m.peers[node.Name] = info
	e.m.mu.Unlock()

	e.m.notifyChange()
}

func (e *eventDelegate) NotifyLeave(node *memberlist.Node) {
	e.m.vivaldi.ForgetNode(node.Name)

	e.m.mu.Lock()
	delete(e.m.peers, node.Name)
	e.m.mu.Unlock()

	e.m.notifyChange()
}

func (e *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	meta, err := DecodeMeta(node.Meta)
	if err != nil {
		log.Printf("membership: failed to decode meta for %s: %v", node.Name, err)
		return
	}

	var rtt time.Duration
	if meta.VivaldiCoord != nil && meta.VivaldiCoord.IsValid() {
		rtt = e.m.vivaldi.DistanceTo(meta.VivaldiCoord)
	}

	info := &PeerInfo{
		NodeID: node.Name,
		Addr:   node.Addr.String(),
		Meta:   *meta,
		RTT:    rtt,
		Alive:  true,
	}

	e.m.mu.Lock()
	e.m.peers[node.Name] = info
	e.m.mu.Unlock()

	e.m.notifyChange()
}
