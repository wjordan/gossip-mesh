package membership

import (
	"encoding/json"
	"fmt"

	"github.com/wjordan/vivaldi"
)

// NodeMeta is the metadata advertised by each node via memberlist.
// It is JSON-encoded and must fit within memberlist's 512-byte metadata limit.
type NodeMeta struct {
	NodeID       string              `json:"id"`
	VivaldiCoord *vivaldi.Coordinate `json:"coord,omitempty"`
	QUICAddr     string              `json:"quic_addr"`
	QUICPort     int                 `json:"quic_port"`
	AppData      json.RawMessage     `json:"app,omitempty"`
}

// Encode serializes NodeMeta to JSON.
func (m *NodeMeta) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeMeta deserializes NodeMeta from JSON.
func DecodeMeta(data []byte) (*NodeMeta, error) {
	var m NodeMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("decode node meta: %w", err)
	}
	return &m, nil
}
