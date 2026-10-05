package raft

import (
	"errors"
	"testing"
)

func TestTransferLeadership(t *testing.T) {
	for _, opts := range [][]func(*Config){nil, {preVote, checkQuorum}} {
		nw := newNetwork(t, 3, opts...)
		nw.campaign("1")
		nw.propose(t, "1", "a")
		if err := nw.nodes["1"].core.TransferLeadership("2"); err != nil {
			t.Fatal(err)
		}
		nw.flush()
		if st := nw.status("2"); st.State != StateLeader {
			t.Fatalf("node 2 is %v after the transfer", st.State)
		}
		if st := nw.status("1"); st.State != StateFollower || st.Lead != "2" {
			t.Fatalf("old leader is %v following %q", st.State, st.Lead)
		}
		nw.propose(t, "2", "b")
		if got := data(nw.nodes["1"].applied); len(got) != 2 || got[1] != "b" {
			t.Fatalf("old leader applied %v", got)
		}
	}
}

func TestTransferWaitsForTargetToCatchUp(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	nw.cut["3"] = true
	for range 5 {
		nw.propose(t, "1", "x")
	}
	delete(nw.cut, "3")
	l := nw.nodes["1"]
	if err := l.core.TransferLeadership("3"); err != nil {
		t.Fatal(err)
	}
	nw.flush()
	nw.tick("1", testHeartbeat)
	if st := nw.status("3"); st.State != StateLeader {
		t.Fatalf("lagging target is %v, last index %d vs %d", st.State, st.LastIndex, nw.status("1").LastIndex)
	}
}

func TestTransferBlocksProposalsUntilItTimesOut(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	nw.cut["2"] = true
	l := nw.nodes["1"]
	if err := l.core.TransferLeadership("2"); err != nil {
		t.Fatal(err)
	}
	nw.flush()
	if _, _, err := l.core.Propose([]byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("proposal during a transfer: %v", err)
	}
	nw.tick("1", testElection)
	if st := nw.status("1"); st.State != StateLeader || st.LeadTransferee != None {
		t.Fatalf("after the timeout: %v, transferee %q", st.State, st.LeadTransferee)
	}
	if _, _, err := l.core.Propose([]byte("x")); err != nil {
		t.Fatalf("proposal after the transfer timed out: %v", err)
	}
}

func TestTransferLeadershipRejects(t *testing.T) {
	s := NewMemoryStorage(ConfState{Voters: ids(3), Learners: []NodeID{"4"}})
	nw := newNetwork(t, 3)
	nw.nodes["1"].core = newTestCore(t, "1", s)
	nw.nodes["1"].storage = s
	nw.campaign("1")
	if err := nw.nodes["1"].core.TransferLeadership("4"); !errors.Is(err, ErrTransferTarget) {
		t.Fatalf("transfer to a learner: %v", err)
	}
	if err := nw.nodes["2"].core.TransferLeadership("3"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("transfer from a follower: %v", err)
	}
}

func TestTransferAbortsWhenTargetDoesNotCatchUp(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	nw.cut["3"] = true
	nw.propose(t, "1", "x")
	if err := nw.nodes["1"].core.TransferLeadership("3"); err != nil {
		t.Fatal(err)
	}
	nw.tick("1", testElection)
	if st := nw.status("1"); st.State != StateLeader || st.LeadTransferee != None || st.TransferSent {
		t.Fatalf("after the catch-up window: %v, transferee %q, sent %v", st.State, st.LeadTransferee, st.TransferSent)
	}
}

func TestTransferElectionTimeoutStartsAtTimeoutNow(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	nw.cut["3"] = true
	nw.propose(t, "1", "x")
	if err := nw.nodes["1"].core.TransferLeadership("3"); err != nil {
		t.Fatal(err)
	}
	nw.tick("1", testElection-2)
	if st := nw.status("1"); st.LeadTransferee != "3" || st.TransferSent {
		t.Fatalf("while the target lags: transferee %q, sent %v", st.LeadTransferee, st.TransferSent)
	}
	delete(nw.cut, "3")
	nw.filter = func(m Message) bool { return m.Type != MsgTimeoutNow }
	nw.tick("1", 1)
	if st := nw.status("1"); st.LeadTransferee != "3" || !st.TransferSent {
		t.Fatalf("after catching up: transferee %q, sent %v", st.LeadTransferee, st.TransferSent)
	}
	nw.tick("1", testElection-1)
	if st := nw.status("1"); st.LeadTransferee != "3" {
		t.Fatalf("transfer aborted %d ticks after MsgTimeoutNow", testElection-1)
	}
	nw.tick("1", 1)
	if st := nw.status("1"); st.State != StateLeader || st.LeadTransferee != None || !st.TransferSent {
		t.Fatalf("after the election window: %v, transferee %q, sent %v", st.State, st.LeadTransferee, st.TransferSent)
	}
}
