package transport

import (
	"encoding/binary"
	"errors"

	"github.com/gliedabrennung/raft"
)

var errDecode = errors.New("transport: malformed message")

func appendUvarint(b []byte, v uint64) []byte { return binary.AppendUvarint(b, v) }

func appendBytes(b, p []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(p)))
	return append(b, p...)
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendIDs(b []byte, ids []raft.NodeID) []byte {
	b = appendUvarint(b, uint64(len(ids)))
	for _, id := range ids {
		b = appendString(b, string(id))
	}
	return b
}

func appendSnapshotMeta(b []byte, s raft.SnapshotMeta) []byte {
	b = appendUvarint(b, s.Index)
	b = appendUvarint(b, s.Term)
	b = appendIDs(b, s.Conf.Voters)
	return appendIDs(b, s.Conf.Learners)
}

func AppendMessage(b []byte, m *raft.Message) []byte {
	b = append(b, byte(m.Type))
	b = appendString(b, string(m.From))
	b = appendString(b, string(m.To))
	b = appendUvarint(b, m.Term)
	b = appendUvarint(b, m.Index)
	b = appendUvarint(b, m.LogTerm)
	b = appendUvarint(b, m.Commit)
	var flags byte
	if m.Reject {
		flags |= 1
	}
	if m.Snapshot != nil {
		flags |= 2
	}
	b = append(b, flags)
	b = appendUvarint(b, m.RejectHint)
	b = appendUvarint(b, uint64(len(m.Entries)))
	for i := range m.Entries {
		e := &m.Entries[i]
		b = appendUvarint(b, e.Index)
		b = appendUvarint(b, e.Term)
		b = append(b, byte(e.Type))
		b = appendBytes(b, e.Data)
	}
	if m.Snapshot != nil {
		b = appendSnapshotMeta(b, *m.Snapshot)
	}
	return b
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) fail() {
	if d.err == nil {
		d.err = errDecode
	}
	d.b = nil
}

func (d *decoder) byte() byte {
	if len(d.b) < 1 {
		d.fail()
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *decoder) uvarint() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) bytes() []byte {
	n := d.uvarint()
	if n > uint64(len(d.b)) {
		d.fail()
		return nil
	}
	p := d.b[:n:n]
	d.b = d.b[n:]
	return p
}

func (d *decoder) string() string { return string(d.bytes()) }

func (d *decoder) count(minSize int) int {
	n := d.uvarint()
	if n > uint64(len(d.b)/minSize) {
		d.fail()
		return 0
	}
	return int(n)
}

func (d *decoder) ids() []raft.NodeID {
	n := d.count(1)
	var out []raft.NodeID
	for range n {
		out = append(out, raft.NodeID(d.string()))
	}
	return out
}

func (d *decoder) snapshotMeta() raft.SnapshotMeta {
	s := raft.SnapshotMeta{Index: d.uvarint(), Term: d.uvarint()}
	s.Conf.Voters = d.ids()
	s.Conf.Learners = d.ids()
	return s
}

func (d *decoder) message() raft.Message {
	m := raft.Message{Type: raft.MessageType(d.byte())}
	m.From = raft.NodeID(d.string())
	m.To = raft.NodeID(d.string())
	m.Term = d.uvarint()
	m.Index = d.uvarint()
	m.LogTerm = d.uvarint()
	m.Commit = d.uvarint()
	flags := d.byte()
	m.Reject = flags&1 != 0
	m.RejectHint = d.uvarint()
	if n := d.count(4); n > 0 {
		m.Entries = make([]raft.Entry, n)
		for i := range m.Entries {
			e := &m.Entries[i]
			e.Index = d.uvarint()
			e.Term = d.uvarint()
			e.Type = raft.EntryType(d.byte())
			if p := d.bytes(); len(p) > 0 {
				e.Data = p
			}
		}
	}
	if flags&2 != 0 {
		s := d.snapshotMeta()
		m.Snapshot = &s
	}
	if m.Type == 0 || m.Type > raft.MsgSnap || flags > 3 {
		d.fail()
	}
	return m
}

func DecodeMessage(b []byte) (raft.Message, error) {
	d := decoder{b: b}
	m := d.message()
	if d.err == nil && len(d.b) != 0 {
		d.fail()
	}
	return m, d.err
}

func appendBatch(b []byte, msgs []raft.Message) []byte {
	b = appendUvarint(b, uint64(len(msgs)))
	for i := range msgs {
		b = AppendMessage(b, &msgs[i])
	}
	return b
}

func decodeBatch(b []byte) ([]raft.Message, error) {
	d := decoder{b: b}
	n := d.count(8)
	out := make([]raft.Message, 0, n)
	for range n {
		m := d.message()
		if d.err != nil {
			return nil, d.err
		}
		out = append(out, m)
	}
	if len(d.b) != 0 {
		return nil, errDecode
	}
	return out, d.err
}
