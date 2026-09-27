package wal

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/acneism/raft"
	"github.com/acneism/raft/internal/fsx"
)

var (
	ErrClosed  = errors.New("wal: closed")
	ErrCorrupt = errors.New("wal: corrupt log")
)

const (
	hardStateSlot = 512
	metaSlot      = 64 << 10
)

type Options struct {
	SegmentSize int64
	NoSync      bool
	CacheBytes  int64
}

type location struct {
	seg  *segment
	off  int64
	size uint32
	term uint64
	prev uint32
	crc  uint32
}

type Log struct {
	mu         sync.RWMutex
	dir        string
	opts       Options
	meta       meta
	metaF      *slotFile
	hs         raft.HardState
	hsF        *slotFile
	hsDirty    bool
	segs       []*segment
	locs       []location
	cache      []raft.Entry
	cacheBytes int64
	buf        []byte
	closed     bool
	fileMu     sync.Mutex
}

func Open(dir string, bootstrap raft.ConfState, opts Options) (*Log, error) {
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = 16 << 20
	}
	if opts.CacheBytes <= 0 {
		opts.CacheBytes = 8 << 20
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	l := &Log{dir: dir, opts: opts}
	var payload []byte
	var err error
	if l.metaF, payload, err = openSlotFile(filepath.Join(dir, "meta"), metaSlot); err != nil {
		return nil, err
	}
	if payload == nil {
		l.meta = meta{epoch: 1, snapshot: raft.SnapshotMeta{Conf: bootstrap}}
		if err := l.metaF.write(l.meta.encode(), true); err != nil {
			l.metaF.f.Close()
			return nil, err
		}
	} else if l.meta, err = decodeMeta(payload); err != nil {
		l.metaF.f.Close()
		return nil, fmt.Errorf("wal: meta: %w", err)
	}
	if l.hsF, payload, err = openSlotFile(filepath.Join(dir, "hardstate"), hardStateSlot); err != nil {
		l.metaF.f.Close()
		return nil, err
	}
	if payload != nil {
		if l.hs, err = decodeHardState(payload); err != nil {
			l.Close()
			return nil, fmt.Errorf("wal: hardstate: %w", err)
		}
	}
	err = l.recover()
	if err == nil {
		err = l.syncAll()
	}
	if err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}

func (l *Log) syncAll() error {
	for _, s := range l.segs {
		if err := fsx.Datasync(s.f); err != nil {
			return err
		}
		s.dirty = false
	}
	l.hsDirty = false
	return l.hsF.f.Sync()
}

func (l *Log) recover() error {
	dirents, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}
	var paths []string
	removed := false
	for _, de := range dirents {
		epoch, _, ok := parseSegName(de.Name())
		if !ok {
			continue
		}
		p := filepath.Join(l.dir, de.Name())
		if epoch != l.meta.epoch {
			if err := os.Remove(p); err != nil {
				return err
			}
			removed = true
			continue
		}
		paths = append(paths, p)
	}
	slices.Sort(paths)
	var lastTerm, last uint64
	for i, p := range paths {
		seg, err := openSegment(p)
		bad := err != nil
		switch {
		case bad:
		case len(l.segs) == 0:
			if seg.first > l.meta.compactIndex+1 {
				seg.close()
				return fmt.Errorf("%w: first segment starts at %d after compacted index %d", ErrCorrupt, seg.first, l.meta.compactIndex)
			}
		case seg.first != last+1 || seg.seed != l.segs[len(l.segs)-1].crc:
			seg.close()
			bad = true
		}
		if bad {
			for _, q := range paths[i:] {
				if err := os.Remove(q); err != nil {
					return err
				}
			}
			removed = true
			break
		}
		next := seg.first
		err = seg.scan(func(r scanned) bool {
			if r.e.Index != next || r.e.Term < lastTerm {
				return false
			}
			next++
			lastTerm = r.e.Term
			if r.e.Index > l.meta.compactIndex {
				l.locs = append(l.locs, location{seg: seg, off: r.off, size: r.size, term: r.e.Term, prev: r.prev, crc: r.crc})
			}
			return true
		})
		if err != nil {
			seg.close()
			return err
		}
		last = next - 1
		l.segs = append(l.segs, seg)
	}
	if removed {
		if err := fsx.SyncDir(l.dir); err != nil {
			return err
		}
	}
	if len(l.segs) > 0 && last < l.meta.compactIndex {
		for _, s := range l.segs {
			if err := l.removeSegment(s); err != nil {
				return err
			}
		}
		l.segs, l.locs = nil, nil
	}
	if len(l.segs) == 0 {
		seg, err := createSegment(l.dir, l.meta.epoch, l.meta.compactIndex+1, rand.Uint32(), l.opts.SegmentSize)
		if err != nil {
			return err
		}
		l.segs = []*segment{seg}
	}
	return l.dropCompactedSegments()
}

func (l *Log) dropCompactedSegments() error {
	n := 0
	for n+1 < len(l.segs) && l.segs[n+1].first <= l.meta.compactIndex+1 {
		n++
	}
	if n == 0 {
		return nil
	}
	for _, s := range l.segs[:n] {
		if err := l.removeSegment(s); err != nil {
			return err
		}
	}
	l.segs = slices.Delete(l.segs, 0, n)
	return fsx.SyncDir(l.dir)
}

func (l *Log) firstIndex() uint64 { return l.meta.compactIndex + 1 }

func (l *Log) lastIndex() uint64 { return l.meta.compactIndex + uint64(len(l.locs)) }

func (l *Log) InitialState() (raft.HardState, raft.ConfState, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.hs, l.meta.snapshot.Conf, nil
}

func (l *Log) FirstIndex() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.firstIndex(), nil
}

func (l *Log) LastIndex() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndex(), nil
}

func (l *Log) Snapshot() (raft.SnapshotMeta, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.meta.snapshot, nil
}

func (l *Log) Restoring() *raft.SnapshotMeta {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.meta.restoring
}

func (l *Log) Term(i uint64) (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	switch {
	case i == l.meta.compactIndex:
		return l.meta.compactTerm, nil
	case i < l.meta.compactIndex:
		return 0, raft.ErrCompacted
	case i > l.lastIndex():
		return 0, raft.ErrUnavailable
	}
	return l.locs[i-l.firstIndex()].term, nil
}

func (l *Log) Entries(lo, hi, maxSize uint64) ([]raft.Entry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, ErrClosed
	}
	if lo <= l.meta.compactIndex {
		return nil, raft.ErrCompacted
	}
	if hi > l.lastIndex()+1 {
		return nil, fmt.Errorf("wal: entries [%d, %d) past last index %d: %w", lo, hi, l.lastIndex(), raft.ErrUnavailable)
	}
	var out []raft.Entry
	var size uint64
	cacheFirst := l.lastIndex() + 1 - uint64(len(l.cache))
	for i := lo; i < hi; i++ {
		var e raft.Entry
		if i >= cacheFirst {
			e = l.cache[i-cacheFirst]
		} else {
			loc := l.locs[i-l.firstIndex()]
			var err error
			if e, err = loc.seg.read(loc.off, loc.size); err != nil {
				return nil, err
			}
		}
		size += 24 + uint64(len(e.Data))
		if len(out) > 0 && size > maxSize {
			break
		}
		out = append(out, e)
	}
	return out, nil
}

func (l *Log) Append(ents []raft.Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if len(ents) == 0 || ents[len(ents)-1].Index <= l.meta.compactIndex {
		return nil
	}
	if ents[0].Index <= l.meta.compactIndex {
		ents = ents[l.meta.compactIndex+1-ents[0].Index:]
	}
	first, last := ents[0].Index, l.lastIndex()
	if first > last+1 {
		return fmt.Errorf("wal: gap: appending %d after last index %d", first, last)
	}
	if first <= last {
		if err := l.truncate(first); err != nil {
			return err
		}
	}
	seg := l.segs[len(l.segs)-1]
	var size int64
	for i := range ents {
		size += recHeader + entryHead + int64(len(ents[i].Data))
	}
	if seg.end > headerSize && seg.end+size > l.opts.SegmentSize {
		if _, err := seg.f.WriteAt(make([]byte, recHeader), seg.end); err != nil {
			return err
		}
		seg.dirty = true
		seg.writes++
		next, err := createSegment(l.dir, l.meta.epoch, first, seg.crc, max(l.opts.SegmentSize, headerSize+size))
		if err != nil {
			return err
		}
		l.segs = append(l.segs, next)
		seg = next
	}
	buf, crc, off := l.buf[:0], seg.crc, seg.end
	for i := range ents {
		start := len(buf)
		prev := crc
		buf, crc = appendRecord(buf, crc, &ents[i])
		l.locs = append(l.locs, location{
			seg:  seg,
			off:  off + int64(start),
			size: uint32(len(buf) - start - recHeader),
			term: ents[i].Term,
			prev: prev,
			crc:  crc,
		})
	}
	if _, err := seg.f.WriteAt(buf, off); err != nil {
		l.locs = l.locs[:len(l.locs)-len(ents)]
		return err
	}
	l.buf = buf
	seg.end += int64(len(buf))
	seg.crc = crc
	seg.dirty = true
	seg.writes++
	l.cacheAppend(ents)
	return nil
}

func (l *Log) truncate(first uint64) error {
	pos := first - l.firstIndex()
	loc := l.locs[pos]
	k := slices.Index(l.segs, loc.seg)
	if k+1 < len(l.segs) {
		for _, s := range l.segs[k+1:] {
			if err := l.removeSegment(s); err != nil {
				return err
			}
		}
		l.segs = l.segs[:k+1]
		if err := fsx.SyncDir(l.dir); err != nil {
			return err
		}
	}
	loc.seg.end, loc.seg.crc = loc.off, loc.prev
	l.locs = l.locs[:pos]
	l.cacheTruncate(first)
	return nil
}

func (l *Log) cacheAppend(ents []raft.Entry) {
	l.cache = append(l.cache, ents...)
	for i := range ents {
		l.cacheBytes += 24 + int64(len(ents[i].Data))
	}
	if l.cacheBytes <= l.opts.CacheBytes {
		return
	}
	drop := 0
	for l.cacheBytes > l.opts.CacheBytes*3/4 && drop < len(l.cache) {
		l.cacheBytes -= 24 + int64(len(l.cache[drop].Data))
		drop++
	}
	n := copy(l.cache, l.cache[drop:])
	clear(l.cache[n:])
	l.cache = l.cache[:n]
}

func (l *Log) cacheTruncate(first uint64) {
	cacheFirst := l.lastIndex() + 1
	if n := len(l.cache); n > 0 {
		cacheFirst = l.cache[0].Index
	}
	if first <= cacheFirst {
		l.cache, l.cacheBytes = nil, 0
		return
	}
	keep := l.cache[:first-cacheFirst]
	l.cache = append([]raft.Entry(nil), keep...)
	l.cacheBytes = 0
	for i := range l.cache {
		l.cacheBytes += 24 + int64(len(l.cache[i].Data))
	}
}

func (l *Log) SetHardState(hs raft.HardState) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if hs.IsEmpty() || hs == l.hs {
		return nil
	}
	if err := l.hsF.write(encodeHardState(hs), false); err != nil {
		return err
	}
	l.hsDirty = l.hsDirty || hs.Term != l.hs.Term || hs.Vote != l.hs.Vote
	l.hs = hs
	return nil
}

func (l *Log) Sync() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ErrClosed
	}
	var segs []*segment
	var writes []uint64
	for _, s := range l.segs {
		if s.dirty {
			segs = append(segs, s)
			writes = append(writes, s.writes)
		}
	}
	l.mu.Unlock()
	if !l.opts.NoSync {
		l.fileMu.Lock()
		for _, s := range segs {
			if s.removed {
				continue
			}
			if err := fsx.Datasync(s.f); err != nil {
				l.fileMu.Unlock()
				return err
			}
		}
		l.fileMu.Unlock()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, s := range segs {
		if s.writes == writes[i] {
			s.dirty = false
		}
	}
	if l.hsDirty {
		if err := l.hsF.sync(); err != nil {
			return err
		}
		l.hsDirty = false
	}
	return nil
}

func (l *Log) removeSegment(s *segment) error {
	l.fileMu.Lock()
	defer l.fileMu.Unlock()
	s.removed = true
	return s.remove()
}

func (l *Log) writeMeta(m meta) error {
	if err := l.metaF.write(m.encode(), true); err != nil {
		return err
	}
	l.meta = m
	return nil
}

func (l *Log) SetRestoring(s *raft.SnapshotMeta) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	m := l.meta
	m.restoring = s
	return l.writeMeta(m)
}

func (l *Log) ApplySnapshot(s raft.SnapshotMeta) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if s.Index <= max(l.meta.snapshot.Index, l.meta.compactIndex) {
		return fmt.Errorf("wal: snapshot %d is not newer than %d", s.Index, max(l.meta.snapshot.Index, l.meta.compactIndex))
	}
	m := meta{epoch: l.meta.epoch + 1, compactIndex: s.Index, compactTerm: s.Term, snapshot: s}
	if err := l.writeMeta(m); err != nil {
		return err
	}
	for _, seg := range l.segs {
		if err := l.removeSegment(seg); err != nil {
			return err
		}
	}
	l.segs, l.locs, l.cache, l.cacheBytes = nil, nil, nil, 0
	seg, err := createSegment(l.dir, m.epoch, s.Index+1, rand.Uint32(), l.opts.SegmentSize)
	if err != nil {
		return err
	}
	l.segs = []*segment{seg}
	return nil
}

func (l *Log) CreateSnapshot(i uint64, conf raft.ConfState) (raft.SnapshotMeta, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return raft.SnapshotMeta{}, ErrClosed
	}
	if i <= l.meta.snapshot.Index {
		return raft.SnapshotMeta{}, fmt.Errorf("wal: snapshot %d is not newer than %d", i, l.meta.snapshot.Index)
	}
	if i < l.meta.compactIndex || i > l.lastIndex() {
		return raft.SnapshotMeta{}, fmt.Errorf("wal: snapshot %d outside the log [%d, %d]", i, l.meta.compactIndex, l.lastIndex())
	}
	term := l.meta.compactTerm
	if i > l.meta.compactIndex {
		term = l.locs[i-l.firstIndex()].term
	}
	m := l.meta
	m.snapshot = raft.SnapshotMeta{Index: i, Term: term, Conf: conf}
	if err := l.writeMeta(m); err != nil {
		return raft.SnapshotMeta{}, err
	}
	return m.snapshot, nil
}

func (l *Log) Compact(i uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if i <= l.meta.compactIndex {
		return raft.ErrCompacted
	}
	if i > l.lastIndex() {
		return fmt.Errorf("wal: compact %d past last index %d", i, l.lastIndex())
	}
	m := l.meta
	m.compactIndex, m.compactTerm = i, l.locs[i-l.firstIndex()].term
	drop := i - l.meta.compactIndex
	if err := l.writeMeta(m); err != nil {
		return err
	}
	n := copy(l.locs, l.locs[drop:])
	clear(l.locs[n:])
	l.locs = l.locs[:n]
	if n := len(l.cache); n > 0 && l.cache[0].Index <= i {
		k := min(uint64(n), i+1-l.cache[0].Index)
		for _, e := range l.cache[:k] {
			l.cacheBytes -= 24 + int64(len(e.Data))
		}
		l.cache = append([]raft.Entry(nil), l.cache[k:]...)
	}
	return l.dropCompactedSegments()
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	l.fileMu.Lock()
	defer l.fileMu.Unlock()
	var errs []error
	if l.hsF != nil {
		errs = append(errs, l.syncAll())
	}
	for _, s := range l.segs {
		errs = append(errs, s.close())
	}
	if l.hsF != nil {
		errs = append(errs, l.hsF.f.Close())
	}
	errs = append(errs, l.metaF.f.Close())
	return errors.Join(errs...)
}
