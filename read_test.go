package raft

import (
	"errors"
	"testing"
)

func (n *testNode) takeReads() []ReadState {
	out := n.reads
	n.reads = nil
	return out
}

func TestReadIndexOnLeader(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	nw.propose(t, "1", "x")
	l := nw.nodes["1"]
	l.takeReads()
	id, err := l.core.ReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	nw.deliver(l.ready())
	reads := l.takeReads()
	if len(reads) != 1 || reads[0].ID != id || reads[0].Failed || reads[0].Index != l.core.log.committed {
		t.Fatalf("reads %+v, want id %d at %d", reads, id, l.core.log.committed)
	}
}

func TestReadIndexCoalescesUntilReady(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	l := nw.nodes["1"]
	a, _ := l.core.ReadIndex()
	b, _ := l.core.ReadIndex()
	if a != b {
		t.Fatalf("reads before Ready got rounds %d and %d", a, b)
	}
	heartbeats := 0
	for _, m := range l.ready() {
		if m.Type == MsgHeartbeat {
			heartbeats++
		}
	}
	if heartbeats != 2 {
		t.Fatalf("%d heartbeats for one round, want one per follower", heartbeats)
	}
	c, _ := l.core.ReadIndex()
	if c == a {
		t.Fatal("a read after Ready joined a round whose heartbeats were already sent")
	}
}

func TestReadIndexNeedsQuorum(t *testing.T) {
	nw := newNetwork(t, 5)
	nw.campaign("1")
	l := nw.nodes["1"]
	nw.cut["3"], nw.cut["4"], nw.cut["5"] = true, true, true
	id, err := l.core.ReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	nw.deliver(l.ready())
	if reads := l.takeReads(); len(reads) != 0 {
		t.Fatalf("read confirmed by 2 of 5: %+v", reads)
	}
	delete(nw.cut, "3")
	nw.tick("1", testHeartbeat)
	reads := l.takeReads()
	if len(reads) != 1 || reads[0].ID != id || reads[0].Failed {
		t.Fatalf("reads %+v after a quorum answered", reads)
	}
}

func TestReadIndexFailsWhenLeadershipIsLost(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	l := nw.nodes["1"]
	nw.cut["2"], nw.cut["3"] = true, true
	id, _ := l.core.ReadIndex()
	nw.deliver(l.ready())
	l.core.Step(Message{Type: MsgApp, From: "2", To: "1", Term: l.core.term + 1})
	l.ready()
	reads := l.takeReads()
	if len(reads) != 1 || reads[0].ID != id || !reads[0].Failed {
		t.Fatalf("reads %+v after stepping down, want %d failed", reads, id)
	}
}

func TestReadIndexFromFollower(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	nw.propose(t, "1", "x")
	f := nw.nodes["2"]
	f.takeReads()
	id, err := f.core.ReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	nw.deliver(f.ready())
	reads := f.takeReads()
	if len(reads) != 1 || reads[0].ID != id || reads[0].Failed || reads[0].Index != nw.nodes["1"].core.log.committed {
		t.Fatalf("follower reads %+v, want id %d at leader commit %d", reads, id, nw.nodes["1"].core.log.committed)
	}
}

func TestReadIndexNeedsCommittedEntryOfTerm(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.manual = map[NodeID]bool{"1": true}
	l := nw.nodes["1"]
	l.core.campaign(false)
	for _, id := range []NodeID{"2", "3"} {
		l.core.Step(Message{Type: MsgVoteResp, From: id, To: "1", Term: l.core.term})
	}
	if l.core.state != StateLeader {
		t.Fatal("not leader")
	}
	if _, err := l.core.ReadIndex(); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("read before the term's first entry is committed: %v", err)
	}
	if _, err := nw.nodes["2"].core.ReadIndex(); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("read on a follower without a leader: %v", err)
	}
}

func TestReadIndexSingleVoter(t *testing.T) {
	s := NewMemoryStorage(ConfState{Voters: []NodeID{"1"}})
	n := &testNode{core: newTestCore(t, "1", s), storage: s}
	n.core.campaign(false)
	n.ready()
	id, err := n.core.ReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	n.ready()
	if reads := n.takeReads(); len(reads) != 1 || reads[0].ID != id || reads[0].Failed {
		t.Fatalf("reads %+v", reads)
	}
}
