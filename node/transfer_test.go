package node_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/acneism/raft"
	"github.com/acneism/raft/node"
)

func TestTransferLeadership(t *testing.T) {
	c := newCluster(t, 3, nil)
	for range 3 {
		l := c.leader(5 * time.Second)
		var to raft.NodeID
		for id := range c.members {
			if id != l.id {
				to = id
				break
			}
		}
		c.write("before", "x")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := l.n.TransferLeadership(ctx, to)
		cancel()
		if err != nil {
			t.Fatalf("transfer %s -> %s: %v", l.id, to, err)
		}
		if nl := c.leader(5 * time.Second); nl.id != to {
			t.Fatalf("leader is %s, want %s", nl.id, to)
		}
		c.write("after", "y")
		c.converged(5 * time.Second)
	}
}

func TestTransferLeadershipToStoppedNodeFails(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(5 * time.Second)
	var to raft.NodeID
	for id := range c.members {
		if id != l.id {
			to = id
			break
		}
	}
	c.stop(to)
	c.write("k", "v")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.n.TransferLeadership(ctx, to); !errors.Is(err, node.ErrTransferFailed) {
		t.Fatalf("transfer to a stopped node: %v", err)
	}
	if st := l.n.Status(); st.State != raft.StateLeader {
		t.Fatalf("old leader is %v after the failed transfer", st.State)
	}
	c.write("k", "w")
}
