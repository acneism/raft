package raft

import (
	"errors"
	"testing"
)

func (nw *network) join(t *testing.T, id NodeID) *testNode {
	s := NewMemoryStorage(ConfState{})
	n := &testNode{core: newTestCore(t, id, s), storage: s}
	nw.nodes[id] = n
	nw.ids = append(nw.ids, id)
	return n
}

func (nw *network) changeConf(t *testing.T, id NodeID, cc ConfChange) {
	t.Helper()
	if _, _, err := nw.nodes[id].core.ProposeConfChange(cc); err != nil {
		t.Fatalf("%+v on %s: %v", cc, id, err)
	}
	nw.flush()
	nw.tick(id, testHeartbeat)
}

func TestAddLearnerThenPromote(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	nw.propose(t, "1", "a")
	joiner := nw.join(t, "4")
	nw.changeConf(t, "1", ConfChange{Type: ConfAddLearner, Node: "4", Addr: "addr4"})
	nw.propose(t, "1", "b")
	if got := data(joiner.applied); len(got) != 2 || got[1] != "b" {
		t.Fatalf("learner applied %v", got)
	}
	if cs := joiner.core.ConfState(); !cs.IsLearner("4") || cs.Addrs["4"] != "addr4" || len(cs.Voters) != 3 {
		t.Fatalf("learner's configuration %+v", cs)
	}
	nw.tick("4", 3*testElection)
	if st := nw.status("4"); st.State != StateFollower {
		t.Fatalf("learner became %v", st.State)
	}
	nw.changeConf(t, "1", ConfChange{Type: ConfPromote, Node: "4"})
	nw.cut["2"], nw.cut["3"] = true, true
	idx, _, err := nw.nodes["1"].core.Propose([]byte("c"))
	if err != nil {
		t.Fatal(err)
	}
	nw.flush()
	if c := nw.status("1").Commit; c >= idx {
		t.Fatalf("committed %d with 2 of 4 voters", c)
	}
	delete(nw.cut, "2")
	nw.tick("1", testHeartbeat)
	if c := nw.status("1").Commit; c < idx {
		t.Fatalf("not committed with 3 of 4 voters: %d < %d", c, idx)
	}
}

func TestConfChangesOneAtATime(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	nw.join(t, "4")
	l := nw.nodes["1"].core
	if _, _, err := l.ProposeConfChange(ConfChange{Type: ConfAddLearner, Node: "4", Addr: "a"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.ProposeConfChange(ConfChange{Type: ConfRemove, Node: "3"}); !errors.Is(err, ErrConfChangePending) {
		t.Fatalf("second change before the first applied: %v", err)
	}
	nw.flush()
	if _, _, err := l.ProposeConfChange(ConfChange{Type: ConfRemove, Node: "3"}); err != nil {
		t.Fatalf("change after the first applied: %v", err)
	}
}

func TestRemovedLeaderStepsDown(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	nw.propose(t, "1", "a")
	nw.changeConf(t, "1", ConfChange{Type: ConfRemove, Node: "1"})
	if st := nw.status("1"); st.State == StateLeader {
		t.Fatal("removed leader still leads")
	}
	for range 5 * testElection {
		for _, id := range []NodeID{"1", "2", "3"} {
			nw.tick(id, 1)
		}
	}
	lead := nw.status("2").Lead
	if lead != "2" && lead != "3" {
		t.Fatalf("leader after the removal is %q", lead)
	}
	nw.propose(t, lead, "b")
	if st := nw.status("1"); st.State != StateFollower {
		t.Fatalf("removed node is %v", st.State)
	}
}

func TestConfChangeValidation(t *testing.T) {
	nw := newNetwork(t, 1)
	nw.campaign("1")
	l := nw.nodes["1"].core
	for _, cc := range []ConfChange{
		{Type: ConfAddLearner, Node: "1", Addr: "a"},
		{Type: ConfAddLearner, Node: "2"},
		{Type: ConfPromote, Node: "1"},
		{Type: ConfRemove, Node: "1"},
		{Type: ConfRemove, Node: "9"},
	} {
		if _, _, err := l.ProposeConfChange(cc); !errors.Is(err, ErrConfChangeInvalid) {
			t.Fatalf("%+v: %v", cc, err)
		}
	}
	if _, _, err := nw.nodes["1"].core.ProposeConfChange(ConfChange{Type: ConfAddLearner, Node: "2", Addr: "a"}); err != nil {
		t.Fatal(err)
	}
}

func TestJoinThroughSnapshotCarriesConfiguration(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	joiner := nw.join(t, "4")
	nw.cut["4"] = true
	nw.changeConf(t, "1", ConfChange{Type: ConfAddLearner, Node: "4", Addr: "addr4"})
	for range 5 {
		nw.propose(t, "1", "x")
	}
	l := nw.nodes["1"]
	applied := l.core.log.applied
	if _, err := l.storage.CreateSnapshot(applied, l.core.ConfState()); err != nil {
		t.Fatal(err)
	}
	if err := l.storage.Compact(applied); err != nil {
		t.Fatal(err)
	}
	delete(nw.cut, "4")
	nw.tick("1", testHeartbeat)
	nw.flush()
	if cs := joiner.core.ConfState(); !cs.IsLearner("4") || cs.Addrs["4"] != "addr4" || len(cs.Voters) != 3 {
		t.Fatalf("configuration after the snapshot %+v", cs)
	}
	if st := joiner.core.Status(); st.Commit < applied {
		t.Fatalf("joiner commit %d < %d", st.Commit, applied)
	}
}

func TestConfChangeWaitsForEntryOfTerm(t *testing.T) {
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
	cc := ConfChange{Type: ConfAddLearner, Node: "4", Addr: "a"}
	if _, _, err := l.core.ProposeConfChange(cc); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("configuration change before the term's first entry is applied: %v", err)
	}
	delete(nw.manual, "1")
	nw.deliver(l.ready())
	if _, _, err := l.core.ProposeConfChange(cc); err != nil {
		t.Fatalf("after the term's entry: %v", err)
	}
}
