package membership

import (
	"encoding/json"
	"testing"

	"github.com/wjordan/vivaldi"
)

func TestNodeMeta_RoundTrip(t *testing.T) {
	coord := vivaldi.NewCoordinate(vivaldi.DefaultConfig())
	original := &NodeMeta{
		NodeID:       "node-abc-123",
		VivaldiCoord: coord,
		QUICAddr:     "10.0.0.1",
		QUICPort:     7947,
		AppData:      json.RawMessage(`{"slots":[0,5,12],"s3_lat":15.5}`),
	}

	data, err := original.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodeMeta(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.NodeID != original.NodeID {
		t.Fatalf("NodeID: got %q, want %q", decoded.NodeID, original.NodeID)
	}
	if decoded.QUICAddr != original.QUICAddr {
		t.Fatalf("QUICAddr: got %q, want %q", decoded.QUICAddr, original.QUICAddr)
	}
	if decoded.QUICPort != original.QUICPort {
		t.Fatalf("QUICPort: got %d, want %d", decoded.QUICPort, original.QUICPort)
	}
	if decoded.VivaldiCoord == nil {
		t.Fatal("VivaldiCoord should not be nil")
	}
	if string(decoded.AppData) != string(original.AppData) {
		t.Fatalf("AppData: got %s, want %s", decoded.AppData, original.AppData)
	}
}

func TestNodeMeta_EmptyFields(t *testing.T) {
	original := &NodeMeta{
		NodeID:   "minimal-node",
		QUICAddr: "127.0.0.1",
		QUICPort: 8000,
	}

	data, err := original.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodeMeta(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.NodeID != "minimal-node" {
		t.Fatalf("NodeID: got %q, want %q", decoded.NodeID, "minimal-node")
	}
	if decoded.VivaldiCoord != nil {
		t.Fatal("VivaldiCoord should be nil for empty")
	}
	if decoded.AppData != nil {
		t.Fatalf("AppData should be nil, got %s", decoded.AppData)
	}
}

func TestNodeMeta_FitsInMemberlistLimit(t *testing.T) {
	meta := &NodeMeta{
		NodeID:       "node-abcdef-1234567890",
		VivaldiCoord: vivaldi.NewCoordinate(vivaldi.DefaultConfig()),
		QUICAddr:     "192.168.100.200",
		QUICPort:     7947,
		AppData:      json.RawMessage(`{"slots":[0,1,2,3,4,5,6,7],"leader":[0,1],"s3_lat":25.3}`),
	}

	data, err := meta.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if len(data) > 512 {
		t.Fatalf("encoded metadata is %d bytes, exceeds 512-byte memberlist limit", len(data))
	}
}

func TestNodeMeta_AppDataRoundTrip(t *testing.T) {
	type customAppData struct {
		Region string `json:"region"`
		Weight int    `json:"weight"`
	}
	app := customAppData{Region: "us-east-1", Weight: 42}
	appJSON, _ := json.Marshal(app)

	meta := &NodeMeta{
		NodeID:   "test-node",
		QUICAddr: "10.0.0.1",
		QUICPort: 9000,
		AppData:  appJSON,
	}

	data, err := meta.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodeMeta(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	var got customAppData
	if err := json.Unmarshal(decoded.AppData, &got); err != nil {
		t.Fatalf("unmarshal AppData: %v", err)
	}
	if got.Region != app.Region || got.Weight != app.Weight {
		t.Fatalf("AppData mismatch: got %+v, want %+v", got, app)
	}
}

func TestDecodeMeta_InvalidJSON(t *testing.T) {
	_, err := DecodeMeta([]byte("not-json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
