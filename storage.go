package raft

import (
	"fmt"
	"sync"
)

type Storage interface {
	InitialState() (HardState, ConfState, error)
	Entries(lo, hi, maxSize uint64) ([]Entry, error)
	Term(i uint64) (uint64, error)
	LastIndex() (uint64, error)
	FirstIndex() (uint64, error)
	Snapshot() (SnapshotMeta, error)
}

type MemoryStorage struct {
	mu   sync.Mutex
	hs   HardState
	snap SnapshotMeta
	ents []Entry
}

func NewMemoryStorage(cs ConfState) *MemoryStorage {
	return &MemoryStorage{snap: SnapshotMeta{Conf: cs}, ents: make([]Entry, 1)}
}

func (s *MemoryStorage) InitialState() (HardState, ConfState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hs, s.snap.Conf, nil
}

func (s *MemoryStorage) SetHardState(hs HardState) {
	s.mu.Lock()
	s.hs = hs
	s.mu.Unlock()
}

func (s *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	offset := s.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > s.lastIndex()+1 {
		return nil, fmt.Errorf("raft: entries [%d, %d) out of bound, last index %d: %w", lo, hi, s.lastIndex(), ErrUnavailable)
	}
	return limitSize(s.ents[lo-offset:hi-offset], maxSize), nil
}

func (s *MemoryStorage) Term(i uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	offset := s.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if i-offset >= uint64(len(s.ents)) {
		return 0, ErrUnavailable
	}
	return s.ents[i-offset].Term, nil
}

func (s *MemoryStorage) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastIndex(), nil
}

func (s *MemoryStorage) lastIndex() uint64 { return s.ents[0].Index + uint64(len(s.ents)) - 1 }

func (s *MemoryStorage) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ents[0].Index + 1, nil
}

func (s *MemoryStorage) Snapshot() (SnapshotMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snap, nil
}

func (s *MemoryStorage) ApplySnapshot(snap SnapshotMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap.Index <= s.snap.Index {
		return fmt.Errorf("raft: snapshot %d is not newer than %d", snap.Index, s.snap.Index)
	}
	s.snap = snap
	s.ents = []Entry{{Index: snap.Index, Term: snap.Term}}
	return nil
}

func (s *MemoryStorage) CreateSnapshot(i uint64, cs ConfState) (SnapshotMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i <= s.snap.Index {
		return SnapshotMeta{}, fmt.Errorf("raft: snapshot %d is not newer than %d", i, s.snap.Index)
	}
	if i > s.lastIndex() {
		return SnapshotMeta{}, fmt.Errorf("raft: snapshot %d is past last index %d", i, s.lastIndex())
	}
	s.snap = SnapshotMeta{Index: i, Term: s.ents[i-s.ents[0].Index].Term, Conf: cs}
	return s.snap, nil
}

func (s *MemoryStorage) Compact(i uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	offset := s.ents[0].Index
	if i <= offset {
		return ErrCompacted
	}
	if i > s.snap.Index {
		return fmt.Errorf("raft: compact %d is past snapshot %d", i, s.snap.Index)
	}
	ents := make([]Entry, 1, uint64(len(s.ents))-(i-offset))
	ents[0] = Entry{Index: i, Term: s.ents[i-offset].Term}
	s.ents = append(ents, s.ents[i-offset+1:]...)
	return nil
}

func (s *MemoryStorage) Append(ents []Entry) error {
	if len(ents) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	first := s.ents[0].Index + 1
	if last := ents[len(ents)-1].Index; last < first {
		return nil
	}
	if first > ents[0].Index {
		ents = ents[first-ents[0].Index:]
	}
	offset := ents[0].Index - s.ents[0].Index
	switch {
	case offset == uint64(len(s.ents)):
		s.ents = append(s.ents, ents...)
	case offset < uint64(len(s.ents)):
		s.ents = append(append([]Entry{}, s.ents[:offset]...), ents...)
	default:
		return fmt.Errorf("raft: gap: appending %d after last index %d", ents[0].Index, s.lastIndex())
	}
	return nil
}
