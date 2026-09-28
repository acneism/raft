package node

import (
	"context"
	"slices"
	"time"

	"github.com/acneism/raft"
)

type readResult struct {
	index uint64
	err   error
}

func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	ch := make(chan readResult, 1)
	n.mu.Lock()
	if n.cfg.LeaseReads {
		n.catchUp(time.Now())
	}
	id, err := n.core.ReadIndex()
	if err == nil {
		n.reads[id] = append(n.reads[id], ch)
	}
	n.mu.Unlock()
	if err != nil {
		return 0, err
	}
	n.wake()
	select {
	case r := <-ch:
		return r.index, r.err
	case <-n.stopc:
		return 0, ErrClosed
	case <-ctx.Done():
		n.mu.Lock()
		n.reads[id] = slices.DeleteFunc(n.reads[id], func(c chan readResult) bool { return c == ch })
		if len(n.reads[id]) == 0 {
			delete(n.reads, id)
		}
		n.mu.Unlock()
		select {
		case r := <-ch:
			return r.index, r.err
		default:
			return 0, ctx.Err()
		}
	}
}

func (n *Node) resolveReads(states []raft.ReadState) {
	for _, rs := range states {
		r := readResult{index: rs.Index}
		if rs.Failed {
			r = readResult{err: ErrNotLeader}
		}
		for _, ch := range n.reads[rs.ID] {
			ch <- r
		}
		delete(n.reads, rs.ID)
	}
}
