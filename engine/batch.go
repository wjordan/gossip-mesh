package engine

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// LazyBatch is a compressed batch of gossip entries with piggybacked
// high-water marks, sent over reliable QUIC streams.
type LazyBatch struct {
	Entries              []GossipEntry
	SenderHighWaterMarks []TopicSeq
}

// LazyBuffer accumulates entries for a single lazy peer.
type LazyBuffer struct {
	entries  []GossipEntry
	peerSeqs map[uint16]uint64 // last known peer high-water marks
}

// NewLazyBuffer creates a new LazyBuffer.
func NewLazyBuffer() *LazyBuffer {
	return &LazyBuffer{
		peerSeqs: make(map[uint16]uint64),
	}
}

// Add adds an entry to the buffer if the peer hasn't already seen it.
func (b *LazyBuffer) Add(entry GossipEntry) {
	if entry.Seq > b.peerSeqs[entry.Topic] {
		b.entries = append(b.entries, entry)
	}
}

// Flush returns the accumulated entries and clears the buffer.
func (b *LazyBuffer) Flush() []GossipEntry {
	entries := b.entries
	b.entries = nil // new backing array on next Add; old slice owned by caller
	return entries
}

// Len returns the number of buffered entries.
func (b *LazyBuffer) Len() int {
	return len(b.entries)
}

// UpdatePeerSeqs updates the known high-water marks for this peer
// (received from piggybacked marks in lazy batches).
func (b *LazyBuffer) UpdatePeerSeqs(marks []TopicSeq) {
	for _, m := range marks {
		if m.Seq > b.peerSeqs[m.Topic] {
			b.peerSeqs[m.Topic] = m.Seq
		}
	}
}

// Shared zstd encoder/decoder (thread-safe).
var (
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	zstdDecoder, _ = zstd.NewReader(nil)
)

// EncodeLazyBatch serializes and compresses a lazy batch.
// Wire format (after type byte, which is written by caller):
//
//	[4B compressed_length BE]
//	[compressed_payload]:
//	  zstd_decode →
//	    [2B entry_count BE]
//	    [entry_count × GossipEntry]:
//	      [2B topic BE][8B seq BE][4B payload_length BE][payload...]
//	    [2B hwm_count BE]
//	    [hwm_count × TopicSeq]:
//	      [2B topic BE][8B seq BE]
func EncodeLazyBatch(batch LazyBatch) ([]byte, error) {
	// Estimate size: entries + HWM marks.
	size := 2 // entry_count
	for _, e := range batch.Entries {
		size += 2 + 8 + 4 + len(e.Payload) // topic + seq + payload_len + payload
	}
	size += 2 // hwm_count
	size += len(batch.SenderHighWaterMarks) * 10 // topic + seq

	buf := make([]byte, 0, size)

	// Entry count.
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(batch.Entries)))
	for _, e := range batch.Entries {
		buf = binary.BigEndian.AppendUint16(buf, e.Topic)
		buf = binary.BigEndian.AppendUint64(buf, e.Seq)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.Payload)))
		buf = append(buf, e.Payload...)
	}

	// HWM marks.
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(batch.SenderHighWaterMarks)))
	for _, m := range batch.SenderHighWaterMarks {
		buf = binary.BigEndian.AppendUint16(buf, m.Topic)
		buf = binary.BigEndian.AppendUint64(buf, m.Seq)
	}

	// Compress.
	compressed := zstdEncoder.EncodeAll(buf, nil)
	return compressed, nil
}

// DecodeLazyBatch decompresses and deserializes a lazy batch.
func DecodeLazyBatch(compressed []byte) (LazyBatch, error) {
	data, err := zstdDecoder.DecodeAll(compressed, nil)
	if err != nil {
		return LazyBatch{}, fmt.Errorf("zstd decompress: %w", err)
	}

	if len(data) < 2 {
		return LazyBatch{}, fmt.Errorf("batch too short")
	}

	var batch LazyBatch
	off := 0

	entryCount := int(binary.BigEndian.Uint16(data[off:]))
	off += 2

	batch.Entries = make([]GossipEntry, entryCount)
	for i := 0; i < entryCount; i++ {
		if off+14 > len(data) {
			return LazyBatch{}, fmt.Errorf("truncated entry %d", i)
		}
		topic := binary.BigEndian.Uint16(data[off:])
		off += 2
		seq := binary.BigEndian.Uint64(data[off:])
		off += 8
		plen := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		if off+plen > len(data) {
			return LazyBatch{}, fmt.Errorf("truncated payload %d", i)
		}
		payload := make([]byte, plen)
		copy(payload, data[off:off+plen])
		off += plen
		batch.Entries[i] = GossipEntry{Topic: topic, Seq: seq, Payload: payload}
	}

	if off+2 > len(data) {
		return LazyBatch{}, fmt.Errorf("truncated HWM header")
	}
	hwmCount := int(binary.BigEndian.Uint16(data[off:]))
	off += 2

	batch.SenderHighWaterMarks = make([]TopicSeq, hwmCount)
	for i := 0; i < hwmCount; i++ {
		if off+10 > len(data) {
			return LazyBatch{}, fmt.Errorf("truncated HWM %d", i)
		}
		batch.SenderHighWaterMarks[i] = TopicSeq{
			Topic: binary.BigEndian.Uint16(data[off:]),
			Seq:    binary.BigEndian.Uint64(data[off+2:]),
		}
		off += 10
	}

	return batch, nil
}

// WriteLazyBatch writes an encoded lazy batch to a writer with a
// 4-byte big-endian length prefix (for framing on QUIC streams).
func WriteLazyBatch(w io.Writer, batch LazyBatch) error {
	compressed, err := EncodeLazyBatch(batch)
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(compressed)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = w.Write(compressed)
	return err
}

// ReadLazyBatch reads a framed lazy batch from a reader.
func ReadLazyBatch(r io.Reader) (LazyBatch, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return LazyBatch{}, err
	}
	size := binary.BigEndian.Uint32(lenBuf[:])
	if size > 16*1024*1024 { // 16MB sanity limit
		return LazyBatch{}, fmt.Errorf("lazy batch too large: %d bytes", size)
	}
	compressed := make([]byte, size)
	if _, err := io.ReadFull(r, compressed); err != nil {
		return LazyBatch{}, err
	}
	return DecodeLazyBatch(compressed)
}
