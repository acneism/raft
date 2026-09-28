package node_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/acneism/raft"
	"github.com/acneism/raft/internal/kvfsm"
	"github.com/acneism/raft/node"
)

func leaseReads(cfg *node.Config) { cfg.LeaseReads = true }

func TestLeaseReadsOnEveryNode(t *testing.T) {
	c := newCluster(t, 3, leaseReads)
	for i, want := range []string{"1", "2", "3"} {
		c.write("k", want)
		for _, m := range c.members {
			if v, _ := readOn(t, m, "k"); v != want {
				t.Fatalf("round %d: %s read %q, want %q", i, m.id, v, want)
			}
		}
	}
}

func TestLeaseExpiresWithoutQuorum(t *testing.T) {
	c := newCluster(t, 3, leaseReads)
	l := c.leader(5 * time.Second)
	c.write("k", "v")
	for id := range c.members {
		if id != l.id {
			c.stop(id)
		}
	}
	time.Sleep(500 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	idx, err := l.n.ReadIndex(ctx)
	if err == nil {
		t.Fatalf("read confirmed at %d from an expired lease", idx)
	}
	if !errors.Is(err, node.ErrNotLeader) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestLeaseReadsNeedCheckQuorum(t *testing.T) {
	fsm, err := kvfsm.Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer fsm.Close()
	_, err = node.Open(node.Config{ID: "a", Dir: t.TempDir(), Peers: map[raft.NodeID]string{"a": "127.0.0.1:0"}, StateMachine: fsm, LeaseReads: true})
	if err == nil {
		t.Fatal("opened with LeaseReads and without CheckQuorum")
	}
}
