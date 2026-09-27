package kvfsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/acneism/raft"
	"github.com/acneism/raft/internal/fsx"
	"github.com/acneism/raft/node"
)

const (
	recEntry byte = 1
	recState byte = 2
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type FSM struct {
	mu        sync.Mutex
	dir       string
	f         *os.File
	w         *bufio.Writer
	data      map[string]string
	results   map[uint64]result
	order     []uint64
	applied   uint64
	durable   uint64
	syncEvery int
	batches   int
	applies   int
	restores  int
}

func Open(dir string, syncEvery int) (*FSM, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FSM{dir: dir, data: map[string]string{}, results: map[uint64]result{}, syncEvery: max(1, syncEvery)}
	path := filepath.Join(dir, "fsm.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	end, err := s.load(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Truncate(end); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(end, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	s.f, s.w = f, bufio.NewWriter(f)
	s.durable = s.applied
	return s, nil
}

func (s *FSM) load(f *os.File) (int64, error) {
	r := bufio.NewReader(f)
	var off int64
	for {
		var h [9]byte
		if _, err := io.ReadFull(r, h[:]); err != nil {
			return off, nil
		}
		n := binary.LittleEndian.Uint32(h[1:])
		if n > 1<<30 {
			return off, nil
		}
		p := make([]byte, n)
		if _, err := io.ReadFull(r, p); err != nil {
			return off, nil
		}
		if crc32.Update(crc32.Checksum(h[:5], castagnoli), castagnoli, p) != binary.LittleEndian.Uint32(h[5:]) {
			return off, nil
		}
		if err := s.replay(h[0], p); err != nil {
			return 0, err
		}
		off += 9 + int64(n)
	}
}

func (s *FSM) replay(typ byte, p []byte) error {
	switch typ {
	case recEntry:
		if len(p) < 8 {
			return errors.New("kvfsm: short record")
		}
		s.applyCommand(binary.LittleEndian.Uint64(p), p[8:])
		s.applied = binary.LittleEndian.Uint64(p)
	case recState:
		idx, data, err := decodeState(p)
		if err != nil {
			return err
		}
		s.data, s.applied = data, idx
	default:
		return fmt.Errorf("kvfsm: unknown record type %d", typ)
	}
	return nil
}

func record(typ byte, p []byte) []byte {
	b := make([]byte, 9, 9+len(p))
	b[0] = typ
	binary.LittleEndian.PutUint32(b[1:], uint32(len(p)))
	binary.LittleEndian.PutUint32(b[5:], crc32.Update(crc32.Checksum(b[:5], castagnoli), castagnoli, p))
	return append(b, p...)
}

const (
	opPut byte = iota + 1
	opAppend
	opGet
)

const maxResults = 8192

type result struct {
	value string
	ok    bool
}

func encode(op byte, key, value string) []byte {
	b := binary.AppendUvarint([]byte{op}, uint64(len(key)))
	b = append(b, key...)
	return append(b, value...)
}

func Put(key, value string) []byte { return encode(opPut, key, value) }

func Append(key, value string) []byte { return encode(opAppend, key, value) }

func Get(key string) []byte { return encode(opGet, key, "") }

func (s *FSM) applyCommand(index uint64, cmd []byte) {
	if len(cmd) < 2 {
		return
	}
	n, k := binary.Uvarint(cmd[1:])
	if k <= 0 || uint64(len(cmd)-1-k) < n {
		return
	}
	key := string(cmd[1+k : 1+k+int(n)])
	value := string(cmd[1+k+int(n):])
	switch cmd[0] {
	case opPut:
		s.data[key] = value
	case opAppend:
		s.data[key] += value
	case opGet:
		v, ok := s.data[key]
		s.results[index] = result{value: v, ok: ok}
		s.order = append(s.order, index)
		if len(s.order) > maxResults {
			delete(s.results, s.order[0])
			s.order = s.order[1:]
		}
	}
}

func (s *FSM) Result(index uint64) (value string, exists, found bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, found := s.results[index]
	delete(s.results, index)
	return r.value, r.ok, found
}

func (s *FSM) Apply(ents []raft.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range ents {
		if e.Index != s.applied+1 {
			return fmt.Errorf("kvfsm: applying %d after %d", e.Index, s.applied)
		}
		var cmd []byte
		if e.Type == raft.EntryNormal {
			cmd = e.Data
		}
		p := binary.LittleEndian.AppendUint64(nil, e.Index)
		if _, err := s.w.Write(record(recEntry, append(p, cmd...))); err != nil {
			return err
		}
		s.applyCommand(e.Index, cmd)
		s.applied = e.Index
		s.applies++
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	s.batches++
	if s.batches%s.syncEvery == 0 {
		if err := fsx.Datasync(s.f); err != nil {
			return err
		}
		s.durable = s.applied
	}
	return nil
}

func (s *FSM) DurableIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.durable
}

func encodeState(index uint64, data map[string]string) []byte {
	b := binary.LittleEndian.AppendUint64(nil, index)
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		b = binary.AppendUvarint(b, uint64(len(k)))
		b = append(b, k...)
		b = binary.AppendUvarint(b, uint64(len(data[k])))
		b = append(b, data[k]...)
	}
	return b
}

func decodeState(p []byte) (uint64, map[string]string, error) {
	if len(p) < 8 {
		return 0, nil, errors.New("kvfsm: short state")
	}
	idx := binary.LittleEndian.Uint64(p)
	p = p[8:]
	data := map[string]string{}
	next := func() (string, bool) {
		n, k := binary.Uvarint(p)
		if k <= 0 || uint64(len(p)-k) < n {
			return "", false
		}
		s := string(p[k : k+int(n)])
		p = p[k+int(n):]
		return s, true
	}
	for len(p) > 0 {
		k, ok1 := next()
		v, ok2 := next()
		if !ok1 || !ok2 {
			return 0, nil, errors.New("kvfsm: corrupt state")
		}
		data[k] = v
	}
	return idx, data, nil
}

func writeFileSync(path string, b []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (s *FSM) Snapshot(dir string) (raft.SnapshotMeta, error) {
	s.mu.Lock()
	idx, b := s.applied, record(recState, encodeState(s.applied, s.data))
	s.mu.Unlock()
	if err := writeFileSync(filepath.Join(dir, "state"), b); err != nil {
		return raft.SnapshotMeta{}, err
	}
	return raft.SnapshotMeta{Index: idx}, fsx.SyncDir(dir)
}

func (s *FSM) Restore(src node.SnapshotSource) error {
	b, err := os.ReadFile(filepath.Join(src.Dir, "state"))
	if err != nil {
		return err
	}
	if len(b) < 9 || b[0] != recState {
		return errors.New("kvfsm: bad snapshot file")
	}
	idx, data, err := decodeState(b[9:])
	if err != nil {
		return err
	}
	if idx != src.Meta.Index {
		return fmt.Errorf("kvfsm: snapshot holds index %d, expected %d", idx, src.Meta.Index)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tmp := filepath.Join(s.dir, "fsm.log.tmp")
	if err := writeFileSync(tmp, b); err != nil {
		return err
	}
	s.f.Close()
	path := filepath.Join(s.dir, "fsm.log")
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if err := fsx.SyncDir(s.dir); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	s.f, s.w = f, bufio.NewWriter(f)
	s.data, s.applied, s.durable = data, idx, idx
	s.restores++
	return nil
}

func (s *FSM) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *FSM) Applied() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied
}

func (s *FSM) Stats() (applies, restores int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applies, s.restores
}

func (s *FSM) Digest() (uint64, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := fnv.New64a()
	h.Write(encodeState(0, s.data))
	return s.applied, h.Sum64()
}

func (s *FSM) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

func (s *FSM) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.w.Flush(); err != nil {
		s.f.Close()
		return err
	}
	return s.f.Close()
}
