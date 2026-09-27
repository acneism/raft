package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/acneism/raft"
	"github.com/acneism/raft/internal/fsx"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const slotHeader = 16

type slotFile struct {
	f      *os.File
	size   int
	seq    uint64
	synced int
	cur    int
}

func openSlotFile(path string, size int) (*slotFile, []byte, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if st.Size() < int64(2*size) {
		if err := f.Truncate(int64(2 * size)); err != nil {
			f.Close()
			return nil, nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, nil, err
		}
		if err := fsx.SyncDir(filepath.Dir(path)); err != nil {
			f.Close()
			return nil, nil, err
		}
	}
	s := &slotFile{f: f, size: size, synced: 1, cur: 1}
	var best []byte
	buf := make([]byte, size)
	for slot := range 2 {
		if _, err := f.ReadAt(buf, int64(slot*size)); err != nil {
			f.Close()
			return nil, nil, err
		}
		seq, payload, ok := decodeSlot(buf)
		if ok && (best == nil || seq > s.seq) {
			s.seq, s.synced, s.cur = seq, slot, slot
			best = append([]byte(nil), payload...)
		}
	}
	return s, best, nil
}

func decodeSlot(buf []byte) (uint64, []byte, bool) {
	crc := binary.LittleEndian.Uint32(buf[0:])
	seq := binary.LittleEndian.Uint64(buf[4:])
	n := int(binary.LittleEndian.Uint32(buf[12:]))
	if seq == 0 || n > len(buf)-slotHeader {
		return 0, nil, false
	}
	if crc32.Checksum(buf[4:slotHeader+n], castagnoli) != crc {
		return 0, nil, false
	}
	return seq, buf[slotHeader : slotHeader+n], true
}

func (s *slotFile) write(payload []byte, sync bool) error {
	if len(payload) > s.size-slotHeader {
		return fmt.Errorf("wal: %d bytes do not fit into a %d byte slot", len(payload), s.size)
	}
	buf := make([]byte, slotHeader+len(payload))
	binary.LittleEndian.PutUint64(buf[4:], s.seq+1)
	binary.LittleEndian.PutUint32(buf[12:], uint32(len(payload)))
	copy(buf[slotHeader:], payload)
	binary.LittleEndian.PutUint32(buf[0:], crc32.Checksum(buf[4:], castagnoli))
	target := 1 - s.synced
	if _, err := s.f.WriteAt(buf, int64(target)*int64(s.size)); err != nil {
		return err
	}
	s.seq++
	s.cur = target
	if sync {
		return s.sync()
	}
	return nil
}

func (s *slotFile) sync() error {
	if s.cur == s.synced {
		return nil
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	s.synced = s.cur
	return nil
}

type meta struct {
	epoch        uint64
	compactIndex uint64
	compactTerm  uint64
	snapshot     raft.SnapshotMeta
	restoring    *raft.SnapshotMeta
}

var errShort = errors.New("wal: truncated record")

type reader struct {
	b   []byte
	err error
}

func (r *reader) u64() uint64 {
	if r.err != nil || len(r.b) < 8 {
		r.err = errShort
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}

func (r *reader) u8() uint8 {
	if r.err != nil || len(r.b) < 1 {
		r.err = errShort
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *reader) str() string {
	if r.err != nil || len(r.b) < 2 {
		r.err = errShort
		return ""
	}
	n := int(binary.LittleEndian.Uint16(r.b))
	if len(r.b) < 2+n {
		r.err = errShort
		return ""
	}
	s := string(r.b[2 : 2+n])
	r.b = r.b[2+n:]
	return s
}

func (r *reader) ids() []raft.NodeID {
	n := int(r.u64())
	if r.err != nil || n > len(r.b) {
		r.err = errShort
		return nil
	}
	var out []raft.NodeID
	for range n {
		out = append(out, raft.NodeID(r.str()))
	}
	return out
}

func (r *reader) snap() raft.SnapshotMeta {
	s := raft.SnapshotMeta{Index: r.u64(), Term: r.u64()}
	s.Conf.Voters = r.ids()
	s.Conf.Learners = r.ids()
	return s
}

func appendStr(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func appendIDs(b []byte, ids []raft.NodeID) []byte {
	b = binary.LittleEndian.AppendUint64(b, uint64(len(ids)))
	for _, id := range ids {
		b = appendStr(b, string(id))
	}
	return b
}

func appendSnap(b []byte, s raft.SnapshotMeta) []byte {
	b = binary.LittleEndian.AppendUint64(b, s.Index)
	b = binary.LittleEndian.AppendUint64(b, s.Term)
	b = appendIDs(b, s.Conf.Voters)
	return appendIDs(b, s.Conf.Learners)
}

func (m *meta) encode() []byte {
	b := binary.LittleEndian.AppendUint64(nil, m.epoch)
	b = binary.LittleEndian.AppendUint64(b, m.compactIndex)
	b = binary.LittleEndian.AppendUint64(b, m.compactTerm)
	b = appendSnap(b, m.snapshot)
	if m.restoring == nil {
		return append(b, 0)
	}
	return appendSnap(append(b, 1), *m.restoring)
}

func decodeMeta(b []byte) (meta, error) {
	r := reader{b: b}
	m := meta{epoch: r.u64(), compactIndex: r.u64(), compactTerm: r.u64(), snapshot: r.snap()}
	if r.u8() == 1 {
		s := r.snap()
		m.restoring = &s
	}
	return m, r.err
}

func encodeHardState(hs raft.HardState) []byte {
	b := binary.LittleEndian.AppendUint64(nil, hs.Term)
	b = binary.LittleEndian.AppendUint64(b, hs.Commit)
	return appendStr(b, string(hs.Vote))
}

func decodeHardState(b []byte) (raft.HardState, error) {
	r := reader{b: b}
	hs := raft.HardState{Term: r.u64(), Commit: r.u64()}
	hs.Vote = raft.NodeID(r.str())
	return hs, r.err
}
