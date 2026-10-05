package node

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/acneism/raft"
)

func (n *Node) applyEntries(ents []raft.Entry) error {
	for len(ents) > 0 {
		k := 0
		for k < len(ents) && ents[k].Type != raft.EntryConfChange {
			k++
		}
		if k < len(ents) {
			k++
		}
		batch := ents[:k]
		ents = ents[k:]
		if last := &batch[len(batch)-1]; last.Type == raft.EntryConfChange {
			if len(batch) > 1 {
				if err := n.fsm.Apply(batch[:len(batch)-1]); err != nil {
					return fmt.Errorf("node: apply: %w", err)
				}
				n.appliedTo(batch[len(batch)-2].Index, batch[:len(batch)-1])
				batch = batch[len(batch)-1:]
			}
			cs, _, err := raft.DecodeConfState(last.Data)
			if err != nil {
				return fmt.Errorf("node: configuration at %d: %w", last.Index, err)
			}
			if err := n.log.SetConf(cs); err != nil {
				return err
			}
			n.setConf(cs)
		}
		if err := n.fsm.Apply(batch); err != nil {
			return fmt.Errorf("node: apply: %w", err)
		}
		n.appliedTo(batch[len(batch)-1].Index, batch)
	}
	return nil
}

func (n *Node) setConf(cs raft.ConfState) {
	n.wmu.Lock()
	defer n.wmu.Unlock()
	n.conf = cs.Clone()
	if n.tr == nil || len(cs.Addrs) == 0 {
		return
	}
	for id, addr := range cs.Addrs {
		if id != n.id && n.peerAddrs[id] != addr {
			n.tr.AddPeer(id, addr)
		}
	}
	for id := range n.peerAddrs {
		if _, ok := cs.Addrs[id]; !ok {
			n.tr.RemovePeer(id)
		}
	}
	n.peerAddrs = maps.Clone(cs.Addrs)
	delete(n.peerAddrs, n.id)
}

func (n *Node) admit(id raft.NodeID, addr string) {
	n.wmu.Lock()
	defer n.wmu.Unlock()
	if n.tr == nil || len(n.conf.Voters)+len(n.conf.Learners) > 0 {
		return
	}
	n.peerAddrs[id] = addr
	n.tr.AddPeer(id, addr)
}

func (n *Node) ConfState() raft.ConfState {
	n.wmu.Lock()
	defer n.wmu.Unlock()
	return n.conf.Clone()
}

func (n *Node) changeConf(ctx context.Context, cc raft.ConfChange) error {
	if err := n.syncCore(ctx); err != nil {
		return err
	}
	n.mu.Lock()
	index, term, err := n.core.ProposeConfChange(cc)
	n.mu.Unlock()
	if err != nil {
		return err
	}
	n.wake()
	return n.Wait(ctx, Proposal{Index: index, Term: term})
}

func (n *Node) AddLearner(ctx context.Context, id raft.NodeID, addr string) error {
	return n.changeConf(ctx, raft.ConfChange{Type: raft.ConfAddLearner, Node: id, Addr: addr})
}

func (n *Node) Promote(ctx context.Context, id raft.NodeID) error {
	if err := n.syncCore(ctx); err != nil {
		return err
	}
	target := n.Status().Commit
	for {
		n.mu.Lock()
		match, ok := n.core.Match(id)
		n.mu.Unlock()
		if !ok {
			if n.Status().State != raft.StateLeader {
				return ErrNotLeader
			}
			return fmt.Errorf("%w: %s is not a member", raft.ErrConfChangeInvalid, id)
		}
		if match >= target {
			break
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopc:
			return ErrClosed
		}
	}
	return n.changeConf(ctx, raft.ConfChange{Type: raft.ConfPromote, Node: id})
}

func (n *Node) Remove(ctx context.Context, id raft.NodeID) error {
	return n.changeConf(ctx, raft.ConfChange{Type: raft.ConfRemove, Node: id})
}

func (n *Node) syncCore(ctx context.Context) error {
	for {
		n.wmu.Lock()
		applied := n.applied
		n.wmu.Unlock()
		if n.Status().Applied >= applied {
			return nil
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopc:
			return ErrClosed
		}
	}
}
