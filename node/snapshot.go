package node

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/acneism/raft"
	"github.com/acneism/raft/internal/fsx"
	"github.com/acneism/raft/transport"
	"github.com/acneism/raft/wal"
)

func (n *Node) snapDir(s raft.SnapshotMeta) string {
	return filepath.Join(n.snapRoot, fmt.Sprintf("%016x-%016x", s.Term, s.Index))
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func (n *Node) promote(from string, s raft.SnapshotMeta) error {
	to := n.snapDir(s)
	if from == to || exists(to) {
		return nil
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}
	return fsx.SyncDir(n.snapRoot)
}

func (n *Node) finishRestore() error {
	r := n.log.Restoring()
	if r == nil {
		return n.ensureSnapshotFiles()
	}
	dir := transport.IncomingDir(n.snapRoot, *r)
	if !exists(dir) {
		dir = n.snapDir(*r)
	}
	n.logger.Warn("repeating an interrupted snapshot restore", "index", r.Index)
	if err := n.fsm.Restore(SnapshotSource{Meta: *r, Dir: dir}); err != nil {
		return fmt.Errorf("node: restore snapshot %d: %w", r.Index, err)
	}
	if err := n.log.ApplySnapshot(*r); err != nil {
		return err
	}
	if err := n.promote(dir, *r); err != nil {
		return err
	}
	return n.ensureSnapshotFiles()
}

func (n *Node) ensureSnapshotFiles() error {
	snap, _ := n.log.Snapshot()
	if snap.Index == 0 || exists(n.snapDir(snap)) {
		return nil
	}
	if in := transport.IncomingDir(n.snapRoot, snap); exists(in) {
		return n.promote(in, snap)
	}
	return nil
}

func (n *Node) installSnapshot(s raft.SnapshotMeta) error {
	resume, ok := n.pauseApplier()
	if !ok {
		return errors.New("node: stopped while installing a snapshot")
	}
	defer resume()
	dir := transport.IncomingDir(n.snapRoot, s)
	if !exists(dir) {
		return fmt.Errorf("node: snapshot %d was never received", s.Index)
	}
	if err := n.log.SetRestoring(&s); err != nil {
		return err
	}
	if err := n.fsm.Restore(SnapshotSource{Meta: s, Dir: dir}); err != nil {
		return fmt.Errorf("node: restore snapshot %d: %w", s.Index, err)
	}
	if err := n.log.ApplySnapshot(s); err != nil {
		return err
	}
	n.setConf(s.Conf)
	if err := n.promote(dir, s); err != nil {
		return err
	}
	n.prune(s.Index)
	return nil
}

type storage struct {
	*wal.Log
	wanted *atomic.Bool
}

func (s storage) Snapshot() (raft.SnapshotMeta, error) {
	snap, _ := s.Log.Snapshot()
	if first, _ := s.FirstIndex(); snap.Index+1 < first {
		s.wanted.Store(true)
		return raft.SnapshotMeta{}, raft.ErrSnapshotTemporarilyUnavailable
	}
	return snap, nil
}

func (n *Node) maybeCompact() error {
	n.wmu.Lock()
	applied := n.applied
	n.wmu.Unlock()
	durable := min(applied, n.fsm.DurableIndex())
	if durable <= n.cfg.TrailingEntries {
		return nil
	}
	to := durable - n.cfg.TrailingEntries
	if first, _ := n.log.FirstIndex(); to < first-1+n.cfg.CompactEntries {
		return nil
	}
	return n.log.Compact(to)
}

func (n *Node) maybeSnapshot() error {
	if !n.snapWanted.Swap(false) {
		return nil
	}
	snap, _ := n.log.Snapshot()
	first, _ := n.log.FirstIndex()
	if snap.Index+1 >= first {
		return nil
	}
	n.wmu.Lock()
	applied := n.applied
	n.wmu.Unlock()
	tmp := filepath.Join(n.snapRoot, "tmp")
	os.RemoveAll(tmp)
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return err
	}
	meta, err := n.fsm.Snapshot(tmp)
	if err != nil {
		return fmt.Errorf("node: snapshot: %w", err)
	}
	if meta.Index+1 < first || meta.Index > applied {
		os.RemoveAll(tmp)
		return fmt.Errorf("node: state machine snapshot at %d, want between the compacted index %d and the applied index %d", meta.Index, first-1, applied)
	}
	if meta.Term, err = n.log.Term(meta.Index); err != nil {
		os.RemoveAll(tmp)
		return err
	}
	if err := n.promote(tmp, meta); err != nil {
		return err
	}
	if _, err := n.log.CreateSnapshot(meta.Index, n.conf); err != nil {
		return err
	}
	n.logger.Info("created a snapshot for a lagging follower", "index", meta.Index)
	n.prune(meta.Index)
	return nil
}

func (n *Node) prune(keep uint64) {
	des, err := os.ReadDir(n.snapRoot)
	if err != nil {
		return
	}
	var names []string
	for _, de := range des {
		name := de.Name()
		var term, index uint64
		incoming := strings.HasPrefix(name, "incoming-")
		if _, err := fmt.Sscanf(strings.TrimPrefix(name, "incoming-"), "%016x-%016x", &term, &index); err != nil {
			continue
		}
		if incoming && index <= keep {
			os.RemoveAll(filepath.Join(n.snapRoot, name))
			continue
		}
		if !incoming {
			names = append(names, name)
		}
	}
	slices.SortFunc(names, func(a, b string) int { return strings.Compare(a[17:], b[17:]) })
	for len(names) > 2 {
		os.RemoveAll(filepath.Join(n.snapRoot, names[0]))
		names = names[1:]
	}
}
