package node_test

import (
	"context"
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

func TestMembershipChanges(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.write("a", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	joiner := c.addMember("n4")
	l := c.leader(5 * time.Second)
	if err := l.n.AddLearner(ctx, "n4", c.peers["n4"]); err != nil {
		t.Fatal(err)
	}
	c.write("b", "2")
	if err := l.n.Promote(ctx, "n4"); err != nil {
		t.Fatal(err)
	}
	if cs := l.n.ConfState(); len(cs.Voters) != 4 || !cs.IsVoter("n4") || len(cs.Learners) != 0 {
		t.Fatalf("configuration after promoting %+v", cs)
	}
	if v, _ := readOn(t, joiner, "b"); v != "2" {
		t.Fatalf("new voter read %q", v)
	}

	var removed []raft.NodeID
	for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
		if id != l.id && len(removed) < 1 {
			removed = append(removed, id)
		}
	}
	if err := l.n.Remove(ctx, removed[0]); err != nil {
		t.Fatal(err)
	}
	c.stop(removed[0])
	delete(c.members, removed[0])
	c.write("c", "3")

	for id := range c.members {
		if id != l.id && id != "n4" {
			c.stop(id)
		}
	}
	c.write("d", "4")

	c.stop("n4")
	c.start("n4")
	cs := joiner.n.ConfState()
	if len(cs.Voters) != 3 || !cs.IsVoter("n4") || slices.Contains(cs.Voters, removed[0]) || cs.Addrs[l.id] == "" {
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
	l := c.leader(5 * time.Second)
	if err := l.n.AddLearner(ctx, "n4", c.peers["n4"]); err != nil {
		t.Fatal(err)
	}
	if err := l.n.Promote(ctx, "n4"); err != nil {
		t.Fatal(err)
	}
	if _, restores, _ := joiner.fsm.Stats(); restores == 0 {
		t.Fatal("the new member caught up without a snapshot")
	}
	if cs := joiner.n.ConfState(); len(cs.Addrs) != 4 {
		t.Fatalf("new member's configuration %+v", cs)
	}
	for id := range c.members {
		if id != l.id && id != "n4" {
			if err := l.n.Remove(ctx, id); err != nil {
				t.Fatal(err)
			}
			c.stop(id)
			delete(c.members, id)
			break
		}
	}
	c.write("after", "1")
	c.stop(l.id)
	c.leader(5 * time.Second)
	c.write("after-leader", "1")
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
		l := c.leader(5 * time.Second)
		if err := l.n.AddLearner(ctx, id, c.peers[id]); err != nil {
			t.Fatal(err)
		}
		if err := l.n.Promote(ctx, id); err != nil {
			t.Fatalf("promote %s: %v", id, err)
		}
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
	l := c.leader(5 * time.Second)
	if err := l.n.AddLearner(ctx, "n4", c.peers["n4"]); err != nil {
		t.Fatal(err)
	}
	if err := l.n.Promote(ctx, "n4"); err != nil {
		t.Fatalf("promote: %v", err)
	}
	c.write("b", "2")
	c.converged(5 * time.Second)
}
