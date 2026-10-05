package node_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/acneism/raft"
	"github.com/acneism/raft/node"
)

func (c *cluster) addMember(id raft.NodeID) *member {
	c.t.Helper()
	c.peers[id] = freeAddrs(c.t, 1)[0]
	m := &member{id: id, dir: c.t.TempDir(), join: true}
	c.members[id] = m
	c.start(id)
	return m
}

func (c *cluster) change(ctx context.Context, f func(l *member) error) *member {
	c.t.Helper()
	for {
		l := c.leader(5 * time.Second)
		err := f(l)
		if err == nil {
			return l
		}
		if !errors.Is(err, node.ErrNotLeader) || ctx.Err() != nil {
			c.t.Fatal(err)
		}
	}
}

func TestMembershipChanges(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.write("a", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	joiner := c.addMember("n4")
	c.change(ctx, func(l *member) error { return l.n.AddLearner(ctx, "n4", c.peers["n4"]) })
	c.write("b", "2")
	l := c.change(ctx, func(l *member) error { return l.n.Promote(ctx, "n4") })
	if cs := l.n.ConfState(); len(cs.Voters) != 4 || !cs.IsVoter("n4") || len(cs.Learners) != 0 {
		t.Fatalf("configuration after promoting %+v", cs)
	}
	if v, _ := readOn(t, joiner, "b"); v != "2" {
		t.Fatalf("new voter read %q", v)
	}

	c.change(ctx, func(l *member) error { return l.n.Remove(ctx, "n1") })
	c.stop("n1")
	delete(c.members, "n1")
	c.write("c", "3")

	c.stop("n2")
	c.write("d", "4")

	c.stop("n4")
	c.start("n4")
	cs := joiner.n.ConfState()
	if len(cs.Voters) != 3 || !cs.IsVoter("n4") || slices.Contains(cs.Voters, "n1") || cs.Addrs["n3"] == "" {
		t.Fatalf("configuration after restarting the new member %+v", cs)
	}
	c.write("e", "5")
	c.converged(5 * time.Second)
}

func TestJoinThroughSnapshot(t *testing.T) {
	c := newCluster(t, 3, nil)
	for i := range 600 {
		c.write(fmt.Sprintf("k%d", i), "v")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	joiner := c.addMember("n4")
	c.change(ctx, func(l *member) error { return l.n.AddLearner(ctx, "n4", c.peers["n4"]) })
	c.change(ctx, func(l *member) error { return l.n.Promote(ctx, "n4") })
	if _, restores, _ := joiner.fsm.Stats(); restores == 0 {
		t.Fatal("the new member caught up without a snapshot")
	}
	if cs := joiner.n.ConfState(); len(cs.Addrs) != 4 {
		t.Fatalf("new member's configuration %+v", cs)
	}
	c.change(ctx, func(l *member) error { return l.n.Remove(ctx, "n1") })
	c.stop("n1")
	delete(c.members, "n1")
	c.write("after", "1")
	c.stop("n2")
	c.write("without-n2", "1")
}

func TestLearnersJoinOneAfterAnotherBySnapshot(t *testing.T) {
	c := newCluster(t, 3, nil)
	for i := range 600 {
		c.write(fmt.Sprintf("k%d", i), "v")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, id := range []raft.NodeID{"n4", "n5"} {
		m := c.addMember(id)
		c.change(ctx, func(l *member) error { return l.n.AddLearner(ctx, id, c.peers[id]) })
		c.change(ctx, func(l *member) error { return l.n.Promote(ctx, id) })
		if _, restores, _ := m.fsm.Stats(); restores == 0 {
			t.Fatalf("%s caught up without a snapshot", id)
		}
	}
}

func TestJoinKnowingOnlyItself(t *testing.T) {
	c := newCluster(t, 3, func(cfg *node.Config) {
		if cfg.Join {
			cfg.Peers = map[raft.NodeID]string{cfg.ID: cfg.Peers[cfg.ID]}
		}
	})
	c.write("a", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c.addMember("n4")
	c.change(ctx, func(l *member) error { return l.n.AddLearner(ctx, "n4", c.peers["n4"]) })
	c.change(ctx, func(l *member) error { return l.n.Promote(ctx, "n4") })
	c.write("b", "2")
	c.converged(5 * time.Second)
}

func waitErr(t *testing.T, m *member, want error) {
	t.Helper()
	deadline := time.Now().Add(slowDisk)
	for !errors.Is(m.n.Err(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("%s: Err() = %v, want %v", m.id, m.n.Err(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRemovedNodeStops(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.write("a", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	l := c.leader(5 * time.Second)
	var live, down raft.NodeID
	for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
		switch {
		case id == l.id:
		case live == "":
			live = id
		default:
			down = id
		}
	}
	c.stop(down)
	c.change(ctx, func(l *member) error { return l.n.Remove(ctx, down) })
	c.change(ctx, func(l *member) error { return l.n.Remove(ctx, live) })
	waitErr(t, c.members[live], node.ErrRemoved)
	c.start(down)
	waitErr(t, c.members[down], node.ErrRemoved)
	c.write("b", "2")
}
