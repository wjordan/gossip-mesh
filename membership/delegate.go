package membership

import "sync/atomic"

// delegate implements memberlist.Delegate.
type delegate struct {
	meta *atomic.Pointer[NodeMeta]
}

func (d *delegate) NodeMeta(limit int) []byte {
	m := d.meta.Load()
	if m == nil {
		return nil
	}
	data, err := m.Encode()
	if err != nil {
		return nil
	}
	if len(data) > limit {
		return nil
	}
	return data
}

func (d *delegate) NotifyMsg([]byte)                    {}
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (d *delegate) LocalState(join bool) []byte                { return nil }
func (d *delegate) MergeRemoteState(buf []byte, join bool)     {}
