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
	c.write("k", "v")
	deadline := time.Now().Add(slowDisk)
	for st := l.n.Status(); st.Progress[to].Match != st.LastIndex; st = l.n.Status() {
		if time.Now().After(deadline) {
			t.Fatalf("%s never caught up: %+v", to, st)
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.stop(to)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.n.TransferLeadership(ctx, to); !errors.Is(err, node.ErrTransferTimeout) {
		t.Fatalf("transfer to a stopped node that was up to date: %v", err)
	}
	c.write("k", "w")
	err := c.leader(5*time.Second).n.TransferLeadership(ctx, to)
	if !errors.Is(err, node.ErrTransferBehind) || !errors.Is(err, node.ErrTransferFailed) {
		t.Fatalf("transfer to a stopped node that lags: %v", err)
	}
	c.write("k", "x")
}
