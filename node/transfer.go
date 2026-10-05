package node

import (
	"context"

	"github.com/acneism/raft"
)

func (n *Node) TransferLeadership(ctx context.Context, to raft.NodeID) error {
	n.mu.Lock()
	start := n.core.Status().Term
	err := n.core.TransferLeadership(to)
	n.mu.Unlock()
	if err != nil {
		return err
	}
	n.wake()
	for {
		n.mu.Lock()
		st, changed := n.core.Status(), n.changed
		n.mu.Unlock()
		switch {
		case st.Lead == to:
			return nil
		case st.Term > start && st.Lead != raft.None:
			return ErrTransferPreempted
		case st.State == raft.StateLeader && st.LeadTransferee != to && st.TransferSent:
			return ErrTransferTimeout
		case st.State == raft.StateLeader && st.LeadTransferee != to:
			return ErrTransferBehind
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopc:
			return ErrClosed
		}
	}
}
