package membership

import (
	"encoding/json"
	"log"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/wjordan/vivaldi"
)

// pingDelegate implements memberlist.PingDelegate.
type pingDelegate struct {
	m *Membership
}

func (p *pingDelegate) AckPayload() []byte {
	coord := p.m.vivaldi.GetCoordinate()
	data, err := json.Marshal(coord)
	if err != nil {
		return nil
	}
	return data
}

func (p *pingDelegate) NotifyPingComplete(other *memberlist.Node, rtt time.Duration, payload []byte) {
	var coord vivaldi.Coordinate
	if err := json.Unmarshal(payload, &coord); err != nil {
		log.Printf("membership: failed to decode ping coordinate from %s: %v", other.Name, err)
		return
	}

	if !coord.IsValid() {
		return
	}

	_, err := p.m.vivaldi.Update(other.Name, &coord, rtt)
	if err != nil {
		log.Printf("membership: vivaldi update from %s failed: %v", other.Name, err)
		return
	}

	// Update local metadata with new coordinate.
	p.m.UpdateMeta(func(meta *NodeMeta) {
		meta.VivaldiCoord = p.m.vivaldi.GetCoordinate()
	})
}
