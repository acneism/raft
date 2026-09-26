package raft

import (
	"errors"
	"fmt"
)

type unstable struct {
	snapshot           *SnapshotMeta
	entries            []Entry
	offset             uint64
	offsetInProgress   uint64
	snapshotInProgress bool
}

func (u *unstable) maybeLastIndex() (uint64, bool) {
	if n := len(u.entries); n != 0 {
		return u.offset + uint64(n) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Index, true
	}
	return 0, false
}

func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot != nil && u.snapshot.Index == i {
			return u.snapshot.Term, true
		}
		return 0, false
	}
	last, ok := u.maybeLastIndex()
	if !ok || i > last {
		return 0, false
	}
	return u.entries[i-u.offset].Term, true
}

func (u *unstable) nextEntries() []Entry {
	n := u.offsetInProgress - u.offset
	if n == uint64(len(u.entries)) {
		return nil
	}
	return u.entries[n:len(u.entries):len(u.entries)]
}

func (u *unstable) nextSnapshot() *SnapshotMeta {
	if u.snapshot == nil || u.snapshotInProgress {
		return nil
	}
	return u.snapshot
}

func (u *unstable) acceptInProgress() {
	u.offsetInProgress = u.offset + uint64(len(u.entries))
	if u.snapshot != nil {
		u.snapshotInProgress = true
	}
}

func (u *unstable) stableTo(i, term uint64) {
	t, ok := u.maybeTerm(i)
	if !ok || i < u.offset || t != term {
		return
	}
	n := i + 1 - u.offset
	u.entries = u.entries[n:]
	u.offset = i + 1
	u.offsetInProgress = max(u.offsetInProgress, u.offset)
	if len(u.entries) == 0 {
		u.entries = nil
	} else if cap(u.entries) > 4*len(u.entries) {
		u.entries = append([]Entry(nil), u.entries...)
	}
}

func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Index == i {
		u.snapshot = nil
		u.snapshotInProgress = false
	}
}

func (u *unstable) restore(s SnapshotMeta) {
	u.offset = s.Index + 1
	u.offsetInProgress = u.offset
	u.entries = nil
	u.snapshot = &s
	u.snapshotInProgress = false
}

func (u *unstable) truncateAndAppend(ents []Entry) {
	from := ents[0].Index
	switch {
	case from == u.offset+uint64(len(u.entries)):
		u.entries = append(u.entries, ents...)
	case from <= u.offset:
		u.entries = append([]Entry(nil), ents...)
		u.offset = from
		u.offsetInProgress = from
	default:
		keep := u.entries[:from-u.offset]
		u.entries = append(append(make([]Entry, 0, len(keep)+len(ents)), keep...), ents...)
		u.offsetInProgress = min(u.offsetInProgress, from)
	}
}

func (u *unstable) slice(lo, hi uint64) []Entry {
	return u.entries[lo-u.offset : hi-u.offset : hi-u.offset]
}

type raftLog struct {
	storage         Storage
	unstable        unstable
	committed       uint64
	applying        uint64
	applied         uint64
	maxApplyingSize uint64
}

func newLog(storage Storage, maxApplyingSize uint64) (*raftLog, error) {
	first, err := storage.FirstIndex()
	if err != nil {
		return nil, err
	}
	last, err := storage.LastIndex()
	if err != nil {
		return nil, err
	}
	return &raftLog{
		storage:         storage,
		unstable:        unstable{offset: last + 1, offsetInProgress: last + 1},
		committed:       first - 1,
		applying:        first - 1,
		applied:         first - 1,
		maxApplyingSize: maxApplyingSize,
	}, nil
}

func (l *raftLog) firstIndex() uint64 {
	if l.unstable.snapshot != nil {
		return l.unstable.snapshot.Index + 1
	}
	i, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *raftLog) lastIndex() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *raftLog) lastStable() uint64 {
	if l.unstable.snapshot != nil {
		return 0
	}
	return l.unstable.offset - 1
}

func (l *raftLog) term(i uint64) (uint64, error) {
	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}
	if i+1 < l.firstIndex() {
		return 0, ErrCompacted
	}
	if i > l.lastIndex() {
		return 0, ErrUnavailable
	}
	t, err := l.storage.Term(i)
	if err != nil && !errors.Is(err, ErrCompacted) {
		panic(err)
	}
	return t, err
}

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		panic(err)
	}
	return t
}

func (l *raftLog) matchTerm(i, term uint64) bool {
	t, err := l.term(i)
	return err == nil && t == term
}

func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	myTerm := l.lastTerm()
	return lastTerm > myTerm || (lastTerm == myTerm && lastIndex >= l.lastIndex())
}

func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents []Entry) (uint64, bool) {
	if !l.matchTerm(index, logTerm) {
		return 0, false
	}
	lastNew := index + uint64(len(ents))
	if ci := l.findConflict(ents); ci != 0 {
		if ci <= l.committed {
			panic(fmt.Sprintf("raft: entry %d conflicts with committed entry, committed %d", ci, l.committed))
		}
		l.append(ents[ci-index-1:]...)
	}
	l.commitTo(min(committed, lastNew))
	return lastNew, true
}

func (l *raftLog) findConflict(ents []Entry) uint64 {
	for i := range ents {
		if !l.matchTerm(ents[i].Index, ents[i].Term) {
			return ents[i].Index
		}
	}
	return 0
}

func (l *raftLog) findConflictByTerm(index, term uint64) (uint64, uint64) {
	for ; index > 0; index-- {
		t, err := l.term(index)
		if err != nil {
			return index, 0
		}
		if t <= term {
			return index, t
		}
	}
	return 0, 0
}

func (l *raftLog) append(ents ...Entry) {
	if len(ents) == 0 {
		return
	}
	if after := ents[0].Index - 1; after < l.committed {
		panic(fmt.Sprintf("raft: append after %d is below committed %d", after, l.committed))
	}
	l.unstable.truncateAndAppend(ents)
}

func (l *raftLog) commitTo(i uint64) {
	if i <= l.committed {
		return
	}
	if i > l.lastIndex() {
		panic(fmt.Sprintf("raft: commit %d is past last index %d", i, l.lastIndex()))
	}
	l.committed = i
}

func (l *raftLog) maybeCommit(i, term uint64) bool {
	if i <= l.committed {
		return false
	}
	if t, err := l.term(i); err != nil || t != term {
		return false
	}
	l.committed = i
	return true
}

func (l *raftLog) appliedTo(i uint64) {
	l.applied = max(l.applied, i)
	l.applying = max(l.applying, i)
}

func (l *raftLog) hasNextCommittedEnts() bool { return l.applying < l.committed }

func (l *raftLog) nextCommittedEnts() []Entry {
	if !l.hasNextCommittedEnts() {
		return nil
	}
	ents, err := l.slice(l.applying+1, l.committed+1, l.maxApplyingSize)
	if err != nil {
		panic(fmt.Sprintf("raft: committed entries [%d, %d]: %v", l.applying+1, l.committed, err))
	}
	return ents
}

func (l *raftLog) entries(i, maxSize uint64) ([]Entry, error) {
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxSize)
}

func (l *raftLog) slice(lo, hi, maxSize uint64) ([]Entry, error) {
	if lo < l.firstIndex() {
		return nil, ErrCompacted
	}
	if hi > l.lastIndex()+1 {
		panic(fmt.Sprintf("raft: slice [%d, %d) past last index %d", lo, hi, l.lastIndex()))
	}
	if lo == hi {
		return nil, nil
	}
	var ents []Entry
	if lo < l.unstable.offset {
		stableHi := min(hi, l.unstable.offset)
		stored, err := l.storage.Entries(lo, stableHi, maxSize)
		if err != nil {
			if errors.Is(err, ErrCompacted) {
				return nil, err
			}
			panic(err)
		}
		if uint64(len(stored)) < stableHi-lo || hi == stableHi {
			return stored, nil
		}
		ents = stored
	}
	if hi > l.unstable.offset {
		tail := l.unstable.slice(max(lo, l.unstable.offset), hi)
		if len(ents) == 0 {
			ents = tail
		} else {
			ents = append(append(make([]Entry, 0, len(ents)+len(tail)), ents...), tail...)
		}
	}
	return limitSize(ents, maxSize), nil
}

func (l *raftLog) snapshot() (SnapshotMeta, error) {
	if l.unstable.snapshot != nil {
		return *l.unstable.snapshot, nil
	}
	return l.storage.Snapshot()
}

func (l *raftLog) restore(s SnapshotMeta) {
	l.committed = s.Index
	l.applying = max(l.applying, s.Index)
	l.unstable.restore(s)
}
