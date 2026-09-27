package wal

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/acneism/raft"
)

var _ raft.Storage = (*Log)(nil)

var testConf = raft.ConfState{Voters: []raft.NodeID{"a", "b", "c"}}

func ents(from, term uint64, n int) []raft.Entry {
	var out []raft.Entry
	for i := range n {
		idx := from + uint64(i)
		out = append(out, raft.Entry{Index: idx, Term: term, Data: []byte(fmt.Sprintf("e%d-t%d", idx, term))})
	}
	return out
}

func open(t *testing.T, dir string, opts Options) *Log {
	t.Helper()
	l, err := Open(dir, testConf, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func appendChunks(t *testing.T, l *Log, es []raft.Entry) {
	t.Helper()
	for len(es) > 0 {
		n := min(5, len(es))
		must(t, l.Append(es[:n]))
		es = es[n:]
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func all(t *testing.T, l *Log) []raft.Entry {
	t.Helper()
	first, _ := l.FirstIndex()
	last, _ := l.LastIndex()
	if last < first {
		return nil
	}
	es, err := l.Entries(first, last+1, ^uint64(0))
	must(t, err)
	return es
}

func sameEntries(a, b []raft.Entry) bool {
	return slices.EqualFunc(a, b, func(x, y raft.Entry) bool {
		return x.Index == y.Index && x.Term == y.Term && x.Type == y.Type && bytes.Equal(x.Data, y.Data)
	})
}

func TestAppendAndReopen(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentSize: 512})
	want := ents(1, 1, 50)
	appendChunks(t, l, want)
	must(t, l.SetHardState(raft.HardState{Term: 3, Vote: "b", Commit: 20}))
	must(t, l.Sync())
	if len(l.segs) < 3 {
		t.Fatalf("expected rollover into several segments, got %d", len(l.segs))
	}
	must(t, l.Close())

	l = open(t, dir, Options{SegmentSize: 512})
	defer l.Close()
	if got := all(t, l); !sameEntries(got, want) {
		t.Fatalf("entries after reopen differ: %d vs %d", len(got), len(want))
	}
	hs, cs, _ := l.InitialState()
	if hs != (raft.HardState{Term: 3, Vote: "b", Commit: 20}) || !slices.Equal(cs.Voters, testConf.Voters) {
		t.Fatalf("state %+v %+v", hs, cs)
	}
	if tm, _ := l.Term(0); tm != 0 {
		t.Fatalf("term(0) = %d", tm)
	}
}

func TestTruncateSuffix(t *testing.T) {
	for _, n := range []int{1, 30} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			dir := t.TempDir()
			l := open(t, dir, Options{SegmentSize: 512})
			appendChunks(t, l, ents(1, 1, 40))
			must(t, l.Sync())
			must(t, l.Append(ents(uint64(41-n), 2, 3)))
			must(t, l.Sync())
			want := append(ents(1, 1, 40-n), ents(uint64(41-n), 2, 3)...)
			if got := all(t, l); !sameEntries(got, want) {
				t.Fatal("entries differ before reopen")
			}
			must(t, l.Close())
			l = open(t, dir, Options{SegmentSize: 512})
			defer l.Close()
			if got := all(t, l); !sameEntries(got, want) {
				t.Fatalf("entries differ after reopen: last %d, want %d", got[len(got)-1].Index, want[len(want)-1].Index)
			}
		})
	}
}

func TestOverwriteLeavesNoStaleTail(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	must(t, l.Append(ents(1, 1, 10)))
	must(t, l.Sync())
	e := raft.Entry{Index: 5, Term: 2, Data: []byte("e5-t1")}
	must(t, l.Append([]raft.Entry{e}))
	must(t, l.Sync())
	must(t, l.Close())
	l = open(t, dir, Options{})
	defer l.Close()
	if last, _ := l.LastIndex(); last != 5 {
		t.Fatalf("old entries after an overwrite of equal size survived: last index %d", last)
	}
}

func TestCompaction(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentSize: 512})
	appendChunks(t, l, ents(1, 1, 60))
	must(t, l.Sync())
	if _, err := l.CreateSnapshot(45, testConf); err != nil {
		t.Fatal(err)
	}
	if err := l.Compact(61); err == nil {
		t.Fatal("compacted past the last index")
	}
	before := len(l.segs)
	must(t, l.Compact(40))
	if len(l.segs) >= before {
		t.Fatalf("no segment deleted: %d -> %d", before, len(l.segs))
	}
	if _, err := l.Entries(40, 41, 100); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("entries below first index: %v", err)
	}
	must(t, l.Close())
	l = open(t, dir, Options{SegmentSize: 512})
	defer l.Close()
	if first, _ := l.FirstIndex(); first != 41 {
		t.Fatalf("first index %d", first)
	}
	if tm, _ := l.Term(40); tm != 1 {
		t.Fatalf("term at compaction point %d", tm)
	}
	if got := all(t, l); !sameEntries(got, ents(41, 1, 20)) {
		t.Fatal("entries after compaction differ")
	}
	snap, _ := l.Snapshot()
	if snap.Index != 45 || snap.Term != 1 {
		t.Fatalf("snapshot %+v", snap)
	}

	must(t, l.Compact(52))
	if _, err := l.CreateSnapshot(50, testConf); err == nil {
		t.Fatal("snapshot below the compacted index")
	}
	if err := l.ApplySnapshot(raft.SnapshotMeta{Index: 50, Term: 1, Conf: testConf}); err == nil {
		t.Fatal("applied a snapshot below the compacted index")
	}
	must(t, l.Close())
	l = open(t, dir, Options{SegmentSize: 512})
	defer l.Close()
	if first, _ := l.FirstIndex(); first != 53 {
		t.Fatalf("first index past the snapshot %d", first)
	}
	if snap, _ := l.Snapshot(); snap.Index != 45 {
		t.Fatalf("snapshot after compacting past it %+v", snap)
	}
	if got := all(t, l); !sameEntries(got, ents(53, 1, 8)) {
		t.Fatal("entries after compacting past the snapshot differ")
	}
	if _, err := l.CreateSnapshot(55, testConf); err != nil {
		t.Fatal(err)
	}
}

func TestApplySnapshot(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentSize: 512})
	must(t, l.Append(ents(1, 1, 30)))
	must(t, l.Sync())
	s := raft.SnapshotMeta{Index: 100, Term: 7, Conf: testConf}
	must(t, l.SetRestoring(&s))
	must(t, l.Close())

	l = open(t, dir, Options{SegmentSize: 512})
	if r := l.Restoring(); r == nil || r.Index != 100 {
		t.Fatalf("restoring marker lost: %+v", r)
	}
	must(t, l.ApplySnapshot(s))
	must(t, l.Append(ents(101, 7, 5)))
	must(t, l.Sync())
	must(t, l.Close())

	l = open(t, dir, Options{SegmentSize: 512})
	defer l.Close()
	if l.Restoring() != nil {
		t.Fatal("restoring marker survived ApplySnapshot")
	}
	if got := all(t, l); !sameEntries(got, ents(101, 7, 5)) {
		t.Fatalf("entries after snapshot: %+v", got)
	}
	if tm, _ := l.Term(100); tm != 7 {
		t.Fatalf("term at snapshot %d", tm)
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	for _, m := range matches {
		if epoch, _, _ := parseSegName(filepath.Base(m)); epoch != l.meta.epoch {
			t.Fatalf("segment of an old epoch left: %s", m)
		}
	}
}

func TestHardStateTornSlot(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	a := raft.HardState{Term: 1, Vote: "a", Commit: 1}
	b := raft.HardState{Term: 2, Vote: "b", Commit: 2}
	must(t, l.SetHardState(a))
	must(t, l.Sync())
	must(t, l.SetHardState(b))
	must(t, l.Sync())
	slot := int64(l.hsF.cur)
	must(t, l.Close())
	f, err := os.OpenFile(filepath.Join(dir, "hardstate"), os.O_RDWR, 0)
	must(t, err)
	_, err = f.WriteAt([]byte{0xff, 0xff}, slot*hardStateSlot+20)
	must(t, err)
	f.Close()
	l = open(t, dir, Options{})
	defer l.Close()
	if hs, _, _ := l.InitialState(); hs != a {
		t.Fatalf("torn slot: got %+v, want the previous %+v", hs, a)
	}
}

type crashModel struct {
	t       *testing.T
	rng     *rand.Rand
	dir     string
	opts    Options
	l       *Log
	cur     []raft.Entry
	states  [][]raft.Entry
	low     uint64
	hs      []raft.HardState
	hsDirty bool
	disk    map[string][]byte
	synced  map[string]int64
}

func (m *crashModel) compact() uint64 {
	c := m.l.meta.compactIndex
	return c
}

func (m *crashModel) snapshotDisk() {
	m.disk = map[string][]byte{}
	m.synced = map[string]int64{}
	for _, s := range m.l.segs {
		m.synced[filepath.Base(s.path)] = s.end
	}
	des, _ := os.ReadDir(m.dir)
	for _, de := range des {
		b, err := os.ReadFile(filepath.Join(m.dir, de.Name()))
		must(m.t, err)
		m.disk[de.Name()] = b
	}
	m.states = [][]raft.Entry{slices.Clone(m.cur)}
	m.low = ^uint64(0)
}

func (m *crashModel) sync() {
	must(m.t, m.l.Sync())
	if m.hsDirty {
		m.hs = m.hs[len(m.hs)-1:]
		m.hsDirty = false
	}
	m.snapshotDisk()
}

func (m *crashModel) appendEnts() {
	lo := int64(max(m.compact(), m.l.meta.snapshot.Index)) + 1
	if len(m.cur) > 0 {
		last := int64(m.cur[len(m.cur)-1].Index)
		lo = min(max(lo, last-int64(m.rng.IntN(6)))+int64(m.rng.IntN(2)), last+1)
	}
	first := uint64(lo)
	prevTerm := m.l.meta.compactTerm
	if k := int(first) - int(m.compact()) - 2; k >= 0 && k < len(m.cur) {
		prevTerm = m.cur[k].Term
	}
	term := prevTerm + uint64(m.rng.IntN(3))
	es := ents(first, term, 1+m.rng.IntN(8))
	for i := range es {
		es[i].Data = append(es[i].Data, bytes.Repeat([]byte{byte(m.rng.IntN(256))}, m.rng.IntN(60))...)
	}
	must(m.t, m.l.Append(es))
	keep := int(first - m.compact() - 1)
	m.cur = append(slices.Clone(m.cur[:keep]), es...)
	m.states = append(m.states, slices.Clone(m.cur))
	m.low = min(m.low, first)
}

func (m *crashModel) crash() {
	var dirty []string
	for _, s := range m.l.segs {
		dirty = append(dirty, filepath.Base(s.path))
	}
	m.l.mu.Lock()
	for _, s := range m.l.segs {
		s.f.Close()
	}
	m.l.hsF.f.Close()
	m.l.metaF.f.Close()
	m.l.closed = true
	m.l.mu.Unlock()
	for _, name := range append(dirty, "hardstate") {
		p := filepath.Join(m.dir, name)
		cur, err := os.ReadFile(p)
		must(m.t, err)
		old := m.disk[name]
		for off := 0; off < len(cur); off += 512 {
			end := min(off+512, len(cur))
			if off < len(old) && bytes.Equal(cur[off:end], old[off:min(end, len(old))]) {
				continue
			}
			choices := 3
			if int64(off) < m.synced[name] {
				choices = 2
			}
			switch m.rng.IntN(choices) {
			case 0:
				if off < len(old) {
					copy(cur[off:end], old[off:min(end, len(old))])
				}
			case 2:
				for i := off; i < end; i++ {
					cur[i] = byte(m.rng.IntN(256))
				}
			}
		}
		must(m.t, os.WriteFile(p, cur, 0o644))
	}
	l, err := Open(m.dir, testConf, m.opts)
	must(m.t, err)
	m.l = l
	m.t.Cleanup(func() { l.Close() })
	m.verify()
}

func (m *crashModel) verify() {
	got := all(m.t, m.l)
	c := m.compact()
	ok := false
	for _, st := range m.states {
		n := len(got)
		if n > len(st) {
			continue
		}
		if !sameEntries(got, st[:n]) {
			continue
		}
		if m.low != ^uint64(0) && c+uint64(n) < min(m.low-1, c+uint64(len(m.states[0]))) {
			continue
		}
		if m.low == ^uint64(0) && n != len(st) {
			continue
		}
		ok = true
		break
	}
	if !ok {
		m.t.Fatalf("recovered %d entries after %d that match no state since the last sync (low %d, states %d)", len(got), c, m.low, len(m.states))
	}
	hs, _, _ := m.l.InitialState()
	if !slices.Contains(m.hs, hs) {
		m.t.Fatalf("recovered hard state %+v, candidates %+v", hs, m.hs)
	}
	m.cur = got
	m.snapshotDisk()
	m.hs = []raft.HardState{hs}
	m.hsDirty = false
}

func TestRandomizedCrashRecovery(t *testing.T) {
	for seed := range uint64(40) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(seed, 99))
			m := &crashModel{t: t, rng: rng, dir: t.TempDir(), opts: Options{SegmentSize: int64(256 + rng.IntN(2048)), CacheBytes: int64(64 + rng.IntN(1024)), NoSync: true}}
			m.l = open(t, m.dir, m.opts)
			m.hs = []raft.HardState{{}}
			m.snapshotDisk()
			var term uint64
			for range 400 {
				switch r := rng.IntN(20); {
				case r < 9:
					m.appendEnts()
				case r < 12:
					m.sync()
				case r < 14:
					prev := m.hs[len(m.hs)-1]
					hs := prev
					if rng.IntN(2) == 0 {
						term++
						hs = raft.HardState{Term: term, Vote: raft.NodeID(fmt.Sprint(rng.IntN(3)))}
					}
					hs.Commit = uint64(rng.IntN(1000))
					must(t, m.l.SetHardState(hs))
					m.hsDirty = m.hsDirty || hs.Term != prev.Term || hs.Vote != prev.Vote
					m.hs = append(m.hs, hs)
				case r < 15:
					m.sync()
					if n := len(m.cur); n > 2 {
						i := m.cur[rng.IntN(n-1)].Index
						if i > m.l.meta.snapshot.Index {
							_, err := m.l.CreateSnapshot(i, testConf)
							must(t, err)
						}
						j := m.l.meta.snapshot.Index
						if rng.IntN(2) == 0 {
							j = m.cur[rng.IntN(n)].Index
						}
						if j > m.compact() {
							must(t, m.l.Compact(j-uint64(rng.IntN(int(j-m.compact())))))
							m.cur = m.cur[len(m.cur)-len(all(t, m.l)):]
						}
					}
					m.snapshotDisk()
				case r < 16:
					m.sync()
					s := raft.SnapshotMeta{Index: max(m.compact(), m.l.meta.snapshot.Index) + uint64(1+rng.IntN(20)), Term: term + 1, Conf: testConf}
					must(t, m.l.ApplySnapshot(s))
					m.cur = nil
					m.snapshotDisk()
				case r < 19:
					m.crash()
				default:
					m.sync()
					must(t, m.l.Close())
					m.l = open(t, m.dir, m.opts)
					if got := all(t, m.l); !sameEntries(got, m.cur) {
						t.Fatal("clean reopen changed the log")
					}
					m.snapshotDisk()
				}
				if n := len(m.cur); n > 0 {
					lo := m.cur[rng.IntN(n)].Index
					got, err := m.l.Entries(lo, m.cur[n-1].Index+1, uint64(rng.IntN(400)))
					must(t, err)
					if len(got) == 0 || !sameEntries(got, m.cur[lo-m.cur[0].Index:][:len(got)]) {
						t.Fatalf("Entries(%d) differ from the model", lo)
					}
				}
			}
			m.l.Close()
		})
	}
}
