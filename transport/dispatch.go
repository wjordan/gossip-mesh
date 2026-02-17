package transport

// Datagram message types (unreliable hot path).
const (
	MsgTypeEagerGossip byte = 0x02
)

// Stream type prefixes (reliable cold path).
// The first byte written on any application stream identifies its purpose.
const (
	StreamTypeLazyBatch byte = 0x11
	StreamTypeRepair    byte = 0x16
)
