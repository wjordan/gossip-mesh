package engine

import (
	"bytes"
	"testing"
)

func TestLazyBatch_RoundTrip(t *testing.T) {
	original := LazyBatch{
		Entries: []GossipEntry{
			{Topic: 0, Seq: 1, Payload: []byte("page-invalidation-1")},
			{Topic: 3, Seq: 42, Payload: []byte("checkpoint-data")},
			{Topic: 100, Seq: 999, Payload: []byte{0x00, 0xff, 0x80}},
		},
		SenderHighWaterMarks: []TopicSeq{
			{Topic: 0, Seq: 10},
			{Topic: 3, Seq: 50},
		},
	}

	compressed, err := EncodeLazyBatch(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodeLazyBatch(compressed)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(decoded.Entries) != len(original.Entries) {
		t.Fatalf("entries: expected %d, got %d", len(original.Entries), len(decoded.Entries))
	}
	for i, e := range decoded.Entries {
		orig := original.Entries[i]
		if e.Topic != orig.Topic || e.Seq != orig.Seq || !bytes.Equal(e.Payload, orig.Payload) {
			t.Fatalf("entry %d mismatch: got {%d, %d, %x}, want {%d, %d, %x}",
				i, e.Topic, e.Seq, e.Payload, orig.Topic, orig.Seq, orig.Payload)
		}
	}

	if len(decoded.SenderHighWaterMarks) != len(original.SenderHighWaterMarks) {
		t.Fatalf("HWMs: expected %d, got %d", len(original.SenderHighWaterMarks), len(decoded.SenderHighWaterMarks))
	}
	for i, m := range decoded.SenderHighWaterMarks {
		orig := original.SenderHighWaterMarks[i]
		if m.Topic != orig.Topic || m.Seq != orig.Seq {
			t.Fatalf("HWM %d mismatch: got {%d, %d}, want {%d, %d}",
				i, m.Topic, m.Seq, orig.Topic, orig.Seq)
		}
	}
}

func TestLazyBatch_EmptyRoundTrip(t *testing.T) {
	original := LazyBatch{}

	compressed, err := EncodeLazyBatch(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodeLazyBatch(compressed)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(decoded.Entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(decoded.Entries))
	}
	if len(decoded.SenderHighWaterMarks) != 0 {
		t.Fatalf("expected 0 HWMs, got %d", len(decoded.SenderHighWaterMarks))
	}
}

func TestLazyBatch_FramedRoundTrip(t *testing.T) {
	original := LazyBatch{
		Entries: []GossipEntry{
			{Topic: 7, Seq: 123, Payload: []byte("test-data")},
		},
		SenderHighWaterMarks: []TopicSeq{
			{Topic: 7, Seq: 200},
		},
	}

	var buf bytes.Buffer
	if err := WriteLazyBatch(&buf, original); err != nil {
		t.Fatalf("write: %v", err)
	}

	decoded, err := ReadLazyBatch(&buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(decoded.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(decoded.Entries))
	}
	if decoded.Entries[0].Topic != 7 || decoded.Entries[0].Seq != 123 {
		t.Fatalf("entry mismatch: %+v", decoded.Entries[0])
	}
	if !bytes.Equal(decoded.Entries[0].Payload, []byte("test-data")) {
		t.Fatalf("payload mismatch: %x", decoded.Entries[0].Payload)
	}
}

func TestLazyBatch_LargePayload(t *testing.T) {
	// Test with a payload larger than typical pages.
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	original := LazyBatch{
		Entries: []GossipEntry{
			{Topic: 0, Seq: 1, Payload: payload},
		},
	}

	compressed, err := EncodeLazyBatch(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodeLazyBatch(compressed)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !bytes.Equal(decoded.Entries[0].Payload, payload) {
		t.Fatal("large payload round-trip mismatch")
	}

	// Compressed should be smaller due to zstd (cyclic pattern compresses well).
	if len(compressed) >= len(payload) {
		t.Logf("note: compressed %d bytes vs original %d bytes payload", len(compressed), len(payload))
	}
}

func TestLazyBuffer_Add_Dedup(t *testing.T) {
	buf := NewLazyBuffer()

	buf.Add(GossipEntry{Topic: 0, Seq: 1, Payload: []byte("a")})
	buf.Add(GossipEntry{Topic: 0, Seq: 2, Payload: []byte("b")})
	buf.Add(GossipEntry{Topic: 0, Seq: 3, Payload: []byte("c")})

	if buf.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", buf.Len())
	}

	// After updating peer seqs, entries at or below the mark should be skipped.
	buf.UpdatePeerSeqs([]TopicSeq{{Topic: 0, Seq: 10}})
	buf.Add(GossipEntry{Topic: 0, Seq: 5, Payload: []byte("skip")})
	buf.Add(GossipEntry{Topic: 0, Seq: 10, Payload: []byte("skip")})
	buf.Add(GossipEntry{Topic: 0, Seq: 11, Payload: []byte("keep")})

	if buf.Len() != 4 { // 3 original + 1 new (seq 11)
		t.Fatalf("expected 4 entries, got %d", buf.Len())
	}
}

func TestLazyBuffer_Flush(t *testing.T) {
	buf := NewLazyBuffer()
	buf.Add(GossipEntry{Topic: 0, Seq: 1, Payload: []byte("a")})
	buf.Add(GossipEntry{Topic: 0, Seq: 2, Payload: []byte("b")})

	entries := buf.Flush()
	if len(entries) != 2 {
		t.Fatalf("expected 2 flushed entries, got %d", len(entries))
	}
	if buf.Len() != 0 {
		t.Fatalf("expected 0 entries after flush, got %d", buf.Len())
	}
}

func TestEagerMessage_RoundTrip(t *testing.T) {
	entry := GossipEntry{Topic: 42, Seq: 12345, Payload: []byte("test-payload")}

	encoded := encodeEagerMessage(entry)
	decoded, err := decodeEagerMessage(encoded[1:])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Topic != entry.Topic {
		t.Fatalf("topic: got %d, want %d", decoded.Topic, entry.Topic)
	}
	if decoded.Seq != entry.Seq {
		t.Fatalf("seq: got %d, want %d", decoded.Seq, entry.Seq)
	}
	if !bytes.Equal(decoded.Payload, entry.Payload) {
		t.Fatalf("payload: got %x, want %x", decoded.Payload, entry.Payload)
	}
}

func TestEagerMessage_TooShort(t *testing.T) {
	_, err := decodeEagerMessage([]byte{0x00, 0x01})
	if err == nil {
		t.Fatal("expected error for short message")
	}
}
