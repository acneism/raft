package raft

import (
	"errors"
	"testing"
)

func lease(c *Config) {
	c.CheckQuorum = true
	c.LeaseTicks = testElection / 2
}

func leaseNetwork(t *testing.T) *network {
	nw := newNetwork(t, 3, lease)
	for _, n := range nw.nodes {
		n.core.startupGuard = false
	}
	return nw
}

func heartbeatsIn(msgs []Message) int {
	n := 0
	for _, m := range msgs {
		if m.Type == MsgHeartbeat {
			n++
		}
	}
	return n
}

func TestLeaseReadNeedsNoRound(t *testing.T) {
	nw := leaseNetwork(t)
	nw.campaign("1")
	nw.propose(t, "1", "x")
	nw.tick("1", testHeartbeat)
	l := nw.nodes["1"]
	l.takeReads()
	id, err := l.core.ReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	msgs := l.ready()
	if n := heartbeatsIn(msgs); n != 0 {
		t.Fatalf("lease read sent %d heartbeats", n)
	}
	if reads := l.takeReads(); len(reads) != 1 || reads[0].ID != id || reads[0].Index != l.core.log.committed {
		t.Fatalf("reads %+v", reads)
	}
}

func TestLeaseExpiresWithoutAcks(t *testing.T) {
	nw := leaseNetwork(t)
	nw.campaign("1")
	nw.tick("1", testHeartbeat)
	nw.cut["2"], nw.cut["3"] = true, true
	l := nw.nodes["1"]
	for range testElection / 2 {
		l.core.Tick()
		nw.deliver(l.ready())
	}
	l.takeReads()
	if _, err := l.core.ReadIndex(); err != nil {
		t.Fatal(err)
	}
	msgs := l.ready()
	if heartbeatsIn(msgs) == 0 || len(l.takeReads()) != 0 {
		t.Fatal("read served from an expired lease")
	}
}

func TestLeaseNotUsedAfterTransferStarts(t *testing.T) {
	nw := leaseNetwork(t)
	nw.campaign("1")
	nw.tick("1", testHeartbeat)
	l := nw.nodes["1"]
	nw.filter = func(m Message) bool { return m.Type != MsgTimeoutNow }
	if err := l.core.TransferLeadership("2"); err != nil {
		t.Fatal(err)
	}
	nw.deliver(l.ready())
	l.takeReads()
	if _, err := l.core.ReadIndex(); err != nil {
		t.Fatal(err)
	}
	if heartbeatsIn(l.ready()) == 0 || len(l.takeReads()) != 0 {
		t.Fatal("read served from the lease during a transfer")
	}
}

func TestStartedNodeHoldsVotesForAnElectionTimeout(t *testing.T) {
	s := NewMemoryStorage(ConfState{Voters: ids(3)})
	n := &testNode{core: newTestCore(t, "2", s, lease), storage: s}
	vote := Message{Type: MsgVote, From: "1", To: "2", Term: 5, Index: 10, LogTerm: 4}
	n.core.Step(vote)
	if n.core.term == 5 {
		t.Fatal("voted right after starting")
	}
	for range testElection {
		n.core.Tick()
	}
	n.core.Step(vote)
	if n.core.term != 5 || n.core.vote != "1" {
		t.Fatalf("after an election timeout: term %d vote %q", n.core.term, n.core.vote)
	}
}

func TestLeaseNeedsCheckQuorum(t *testing.T) {
	_, err := New(Config{ID: "1", ElectionTick: 10, HeartbeatTick: 1, Storage: NewMemoryStorage(ConfState{Voters: ids(1)}), LeaseTicks: 5})
	if err == nil || errors.Is(err, ErrNotLeader) {
		t.Fatalf("lease without CheckQuorum: %v", err)
	}
}
