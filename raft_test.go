package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

const (
	testElection  = 10
	testHeartbeat = 1
)

type testNode struct {
	core    *Core
	storage *MemoryStorage
	applied []Entry
}

func newTestCore(t *testing.T, id NodeID, storage *MemoryStorage, opts ...func(*Config)) *Core {
	t.Helper()
	cfg := Config{
		ID:            id,
		ElectionTick:  testElection,
		HeartbeatTick: testHeartbeat,
		Storage:       storage,
		Rand:          rand.New(rand.NewPCG(1, uint64(len(id)))),
	}
	for _, o := range opts {
		o(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func preVote(c *Config)     { c.PreVote = true }
func checkQuorum(c *Config) { c.CheckQuorum = true }

func ids(n int) []NodeID {
	var out []NodeID
	for i := 1; i <= n; i++ {
		out = append(out, NodeID(fmt.Sprint(i)))
	}
	return out
}

func (n *testNode) persist(rd Ready) {
	if rd.Snapshot != nil {
		if err := n.storage.ApplySnapshot(*rd.Snapshot); err != nil {
			panic(err)
		}
	}
	if err := n.storage.Append(rd.Entries); err != nil {
		panic(err)
	}
	if !rd.HardState.IsEmpty() {
		n.storage.SetHardState(rd.HardState)
	}
}

func (n *testNode) ready() []Message {
	var out []Message
	for n.core.HasReady() {
		rd := n.core.Ready()
		out = append(out, rd.Messages...)
		n.persist(rd)
		out = append(out, rd.MessagesAfterPersist...)
		n.applied = append(n.applied, rd.Committed...)
		n.core.Advance(rd)
	}
	return out
}

type network struct {
	t      *testing.T
	nodes  map[NodeID]*testNode
	ids    []NodeID
	cut    map[NodeID]bool
	filter func(Message) bool
	manual map[NodeID]bool
}

func newNetwork(t *testing.T, n int, opts ...func(*Config)) *network {
	nw := &network{t: t, nodes: map[NodeID]*testNode{}, ids: ids(n), cut: map[NodeID]bool{}}
	for _, id := range nw.ids {
		s := NewMemoryStorage(ConfState{Voters: nw.ids})
		nw.nodes[id] = &testNode{core: newTestCore(t, id, s, opts...), storage: s}
	}
	return nw
}

func (nw *network) deliver(msgs []Message) {
	for len(msgs) > 0 {
		m := msgs[0]
		msgs = msgs[1:]
		if nw.cut[m.From] || nw.cut[m.To] || (nw.filter != nil && !nw.filter(m)) {
			continue
		}
		to := nw.nodes[m.To]
		if to == nil {
			continue
		}
		if err := to.core.Step(m); err != nil {
			nw.t.Fatal(err)
		}
		if m.Type == MsgSnap {
			nw.nodes[m.From].core.ReportSnapshot(m.To, false)
			msgs = append(msgs, nw.ready(m.From)...)
		}
		msgs = append(msgs, nw.ready(m.To)...)
	}
}

func (nw *network) ready(id NodeID) []Message {
	if nw.manual[id] {
		return nil
	}
	return nw.nodes[id].ready()
}

func (nw *network) flush() {
	for _, id := range nw.ids {
		nw.deliver(nw.ready(id))
	}
}

func (nw *network) campaign(id NodeID) {
	n := nw.nodes[id]
	n.core.campaign(n.core.preVote)
	nw.deliver(n.ready())
}

func (nw *network) tick(id NodeID, times int) {
	for range times {
		nw.nodes[id].core.Tick()
		nw.flush()
	}
}

func (nw *network) propose(t *testing.T, id NodeID, data string) uint64 {
	t.Helper()
	idx, _, err := nw.nodes[id].core.Propose([]byte(data))
	if err != nil {
		t.Fatalf("propose on %s: %v", id, err)
	}
	nw.flush()
	return idx
}

func (nw *network) status(id NodeID) Status { return nw.nodes[id].core.Status() }

func data(ents []Entry) []string {
	var out []string
	for _, e := range ents {
		if e.Type == EntryNormal {
			out = append(out, string(e.Data))
		}
	}
	return out
}

func TestLeaderElection(t *testing.T) {
	for _, pv := range []bool{false, true} {
		t.Run(fmt.Sprintf("prevote=%v", pv), func(t *testing.T) {
			var opts []func(*Config)
			if pv {
				opts = append(opts, preVote)
			}
			nw := newNetwork(t, 3, opts...)
			nw.campaign("1")
			for _, id := range nw.ids {
				st := nw.status(id)
				if st.Lead != "1" || st.Term != 1 {
					t.Fatalf("%s: lead %q term %d, want 1/1", id, st.Lead, st.Term)
				}
			}
			if st := nw.status("1"); st.State != StateLeader || !st.LeaderReady {
				t.Fatalf("node 1: %+v", st)
			}
		})
	}
}

func TestElectionByTimeout(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.tick("2", 2*testElection)
	if st := nw.status("2"); st.State != StateLeader {
		t.Fatalf("node 2 is %s after 2 election timeouts", st.State)
	}
}

func TestReplication(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	for _, d := range []string{"a", "b", "c"} {
		nw.propose(t, "1", d)
	}
	for _, id := range nw.ids {
		if got := data(nw.nodes[id].applied); !slices.Equal(got, []string{"a", "b", "c"}) {
			t.Fatalf("%s applied %v", id, got)
		}
	}
}

func TestProposeRequiresReadyLeader(t *testing.T) {
	nw := newNetwork(t, 3)
	if _, _, err := nw.nodes["1"].core.Propose(nil); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower propose: %v", err)
	}
	nw.campaign("1")
	c := nw.nodes["1"].core
	idx, term, err := c.Propose([]byte("x"))
	if err != nil || term != 1 || idx != c.log.lastIndex() {
		t.Fatalf("propose = %d, %d, %v", idx, term, err)
	}
	if _, _, err := nw.nodes["2"].core.Propose(nil); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower propose: %v", err)
	}
}

func TestNoopNotAppliedRejectsPropose(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	nw.cut["1"] = true
	nw.filter = func(m Message) bool { return m.Type != MsgAppResp }
	nw.campaign("2")
	n2 := nw.nodes["2"].core
	if n2.state != StateLeader {
		t.Fatal("node 2 did not win")
	}
	if _, _, err := n2.Propose(nil); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("propose before no-op applied: %v", err)
	}
	nw.filter = nil
	nw.tick("2", testHeartbeat)
	if _, _, err := n2.Propose(nil); err != nil {
		t.Fatalf("propose after no-op applied: %v", err)
	}
}

func TestLeaderSendsBeforePersist(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	leader := nw.nodes["1"].core
	if _, _, err := leader.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	rd := leader.Ready()
	if len(rd.Entries) != 1 || len(rd.MessagesAfterPersist) != 0 {
		t.Fatalf("ready: %d entries, %d deferred messages", len(rd.Entries), len(rd.MessagesAfterPersist))
	}
	for _, m := range rd.Messages {
		if m.Type != MsgApp || len(m.Entries) != 1 {
			t.Fatalf("unexpected early message %+v", m)
		}
		f := nw.nodes[m.To].core
		if err := f.Step(m); err != nil {
			t.Fatal(err)
		}
		frd := f.Ready()
		if len(frd.Messages) != 0 || len(frd.MessagesAfterPersist) != 1 {
			t.Fatalf("follower responses must wait for persistence: %+v", frd)
		}
	}
}

func TestLeaderCountsItselfAfterPersist(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	nw.manual = map[NodeID]bool{"1": true}
	ln := nw.nodes["1"]
	idx, _, _ := ln.core.Propose([]byte("x"))
	rd := ln.core.Ready()

	var toTwo []Message
	for _, m := range rd.Messages {
		if m.To == "2" {
			toTwo = append(toTwo, m)
		}
	}
	nw.deliver(toTwo)
	if c := ln.core.log.committed; c >= idx {
		t.Fatalf("committed %d before the leader's fsync", c)
	}
	ln.persist(rd)
	ln.core.Advance(rd)
	if c := ln.core.log.committed; c != idx {
		t.Fatalf("committed %d after the leader's fsync, want %d", c, idx)
	}
}

func TestCommitWithoutLeaderDisk(t *testing.T) {
	nw := newNetwork(t, 5)
	nw.campaign("1")
	nw.manual = map[NodeID]bool{"1": true}
	ln := nw.nodes["1"]
	idx, _, _ := ln.core.Propose([]byte("x"))
	rd := ln.core.Ready()
	nw.deliver(rd.Messages)
	if c := ln.core.log.committed; c != idx {
		t.Fatalf("four follower copies did not commit: committed %d, want %d", c, idx)
	}
	next := ln.core.Ready()
	if len(next.Committed) != 0 {
		t.Fatalf("entries handed out for applying before the leader's disk has them: %+v", next.Committed)
	}
	ln.persist(rd)
	ln.core.AdvancePersist(rd)
	after := ln.core.Ready()
	if len(after.Committed) != 1 || after.Committed[0].Index != idx {
		t.Fatalf("committed entries %+v", after.Committed)
	}
	ln.persist(next)
	ln.core.Advance(next)
	ln.persist(after)
	ln.core.Advance(after)
	ln.core.AdvanceApply(rd)
}

func TestSoleVoterHoldsMessagesUntilTermDurable(t *testing.T) {
	cs := ConfState{Voters: []NodeID{"1"}, Learners: []NodeID{"2"}}
	nw := &network{t: t, ids: ids(2), cut: map[NodeID]bool{}, nodes: map[NodeID]*testNode{}, manual: map[NodeID]bool{"1": true}}
	for _, id := range nw.ids {
		s := NewMemoryStorage(cs)
		nw.nodes[id] = &testNode{core: newTestCore(t, id, s), storage: s}
	}
	n := nw.nodes["1"]
	c := n.core
	c.campaign(false)
	if c.state != StateLeader {
		t.Fatal("sole voter did not win")
	}
	rd := c.Ready()
	if len(rd.Messages) != 0 {
		t.Fatalf("sent %v before the term was durable", rd.Messages)
	}
	if len(rd.MessagesAfterPersist) != 1 || rd.MessagesAfterPersist[0].Type != MsgApp {
		t.Fatalf("deferred %+v", rd.MessagesAfterPersist)
	}
	n.persist(rd)
	c.Advance(rd)
	nw.deliver(rd.MessagesAfterPersist)
	delete(nw.manual, "1")
	nw.flush()
	nw.manual["1"] = true
	if _, _, err := c.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if rd := c.Ready(); len(rd.Messages) != 1 {
		t.Fatalf("messages after the term is durable: %+v / %+v", rd.Messages, rd.MessagesAfterPersist)
	}
}

func TestHandleVote(t *testing.T) {
	tests := []struct {
		name     string
		term     uint64
		vote     NodeID
		log      []Entry
		msg      Message
		wantTerm uint64
		granted  bool
	}{
		{name: "ignore older term", term: 2, msg: Message{Term: 1}, wantTerm: 0},
		{name: "grant vote in new term", term: 1, msg: Message{Term: 2}, wantTerm: 2, granted: true},
		{name: "reject if already voted", term: 2, vote: "3", msg: Message{Term: 2}, wantTerm: 2},
		{name: "repeat vote for the same candidate", term: 2, vote: "2", msg: Message{Term: 2}, wantTerm: 2, granted: true},
		{
			name: "reject if log is not up to date", term: 1,
			log: []Entry{{Index: 1, Term: 1}},
			msg: Message{Term: 1}, wantTerm: 1,
		},
		{
			name: "grant if longer log of the same term", term: 1,
			log: []Entry{{Index: 1, Term: 1}},
			msg: Message{Term: 2, Index: 2, LogTerm: 1}, wantTerm: 2, granted: true,
		},
		{
			name: "reject longer log of an older term", term: 2,
			log: []Entry{{Index: 1, Term: 2}},
			msg: Message{Term: 3, Index: 5, LogTerm: 1}, wantTerm: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemoryStorage(ConfState{Voters: ids(3)})
			s.Append(tt.log)
			s.SetHardState(HardState{Term: tt.term, Vote: tt.vote})
			c := newTestCore(t, "1", s)
			m := tt.msg
			m.Type, m.From, m.To = MsgVote, "2", "1"
			c.Step(m)
			rd := c.Ready()
			if tt.wantTerm == 0 {
				if len(rd.MessagesAfterPersist) != 0 {
					t.Fatalf("responses %+v", rd.MessagesAfterPersist)
				}
				return
			}
			if len(rd.MessagesAfterPersist) != 1 {
				t.Fatalf("responses %+v", rd.MessagesAfterPersist)
			}
			resp := rd.MessagesAfterPersist[0]
			if resp.Reject == tt.granted || resp.Term != tt.wantTerm {
				t.Fatalf("got reject=%v term=%d, want granted=%v term=%d", resp.Reject, resp.Term, tt.granted, tt.wantTerm)
			}
			if tt.granted && tt.vote != "2" && (rd.HardState.Vote != "2" || !rd.MustSync) {
				t.Fatalf("vote not persisted: %+v", rd.HardState)
			}
		})
	}
}

func TestHandleAppendEntries(t *testing.T) {
	tests := []struct {
		name    string
		term    uint64
		log     []Entry
		msg     Message
		success bool
		wantLog []uint64
	}{
		{name: "reject older term", term: 2, msg: Message{Term: 1}, wantLog: nil},
		{name: "heartbeat", term: 1, msg: Message{Term: 1}, success: true},
		{name: "reject if prev entry missing", term: 1, msg: Message{Term: 1, Index: 1, LogTerm: 1}},
		{
			name: "append", term: 1,
			msg:     Message{Term: 1, Entries: []Entry{{Index: 1, Term: 1}}},
			success: true, wantLog: []uint64{1},
		},
		{
			name: "conflict replaces the suffix", term: 1,
			log:     []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}},
			msg:     Message{Term: 2, Entries: []Entry{{Index: 1, Term: 2}}},
			success: true, wantLog: []uint64{2},
		},
		{
			name: "stale message keeps newer entries", term: 1,
			log:     []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}},
			msg:     Message{Term: 1, Entries: []Entry{{Index: 1, Term: 1}}},
			success: true, wantLog: []uint64{1, 1},
		},
		{
			name: "reject mismatching prev term", term: 2,
			log:     []Entry{{Index: 1, Term: 1}},
			msg:     Message{Term: 2, Index: 1, LogTerm: 2},
			wantLog: []uint64{1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemoryStorage(ConfState{Voters: ids(3)})
			s.Append(tt.log)
			s.SetHardState(HardState{Term: tt.term})
			n := &testNode{core: newTestCore(t, "1", s), storage: s}
			m := tt.msg
			m.Type, m.From, m.To = MsgApp, "2", "1"
			n.core.Step(m)
			out := n.ready()
			if tt.msg.Term < tt.term {
				if len(out) != 0 {
					t.Fatalf("answered a stale leader: %+v", out)
				}
				return
			}
			if len(out) != 1 || out[0].Reject == tt.success {
				t.Fatalf("responses %+v, want success=%v", out, tt.success)
			}
			var terms []uint64
			last, _ := s.LastIndex()
			for i := uint64(1); i <= last; i++ {
				tm, _ := s.Term(i)
				terms = append(terms, tm)
			}
			if !slices.Equal(terms, tt.wantLog) {
				t.Fatalf("log terms %v, want %v", terms, tt.wantLog)
			}
		})
	}
}

func appendTerms(s *MemoryStorage, from uint64, term uint64, n int) {
	var ents []Entry
	for i := range n {
		ents = append(ents, Entry{Index: from + uint64(i), Term: term})
	}
	s.Append(ents)
}

func TestFastBackoff(t *testing.T) {
	voters := ids(3)
	mk := func(id NodeID, build func(s *MemoryStorage)) *testNode {
		s := NewMemoryStorage(ConfState{Voters: voters})
		build(s)
		s.SetHardState(HardState{Term: 3})
		return &testNode{core: newTestCore(t, id, s), storage: s}
	}
	leaderLog := func(s *MemoryStorage) { appendTerms(s, 1, 1, 10); appendTerms(s, 11, 3, 90) }
	nw := &network{t: t, ids: voters, cut: map[NodeID]bool{}, nodes: map[NodeID]*testNode{
		"1": mk("1", leaderLog),
		"2": mk("2", func(s *MemoryStorage) { appendTerms(s, 1, 1, 10); appendTerms(s, 11, 2, 50) }),
		"3": mk("3", leaderLog),
	}}
	rejects := 0
	nw.filter = func(m Message) bool {
		if m.Type == MsgAppResp && m.Reject {
			rejects++
		}
		return true
	}
	nw.campaign("1")
	if nw.status("1").State != StateLeader {
		t.Fatal("node 1 did not win")
	}
	if rejects > 2 {
		t.Fatalf("%d rejections to find the match point", rejects)
	}
	if a, b := nw.status("1").LastIndex, nw.status("2").LastIndex; a != b {
		t.Fatalf("follower last index %d, leader %d", b, a)
	}
}

func TestCommitOnlyCurrentTerm(t *testing.T) {
	voters := ids(3)
	s := NewMemoryStorage(ConfState{Voters: voters})
	appendTerms(s, 1, 1, 1)
	s.SetHardState(HardState{Term: 1})
	n := &testNode{core: newTestCore(t, "1", s), storage: s}
	n.core.campaign(false)
	n.ready()
	n.core.Step(Message{Type: MsgVoteResp, From: "2", To: "1", Term: 2})
	n.ready()
	if n.core.state != StateLeader {
		t.Fatal("not leader")
	}
	n.core.Step(Message{Type: MsgAppResp, From: "2", To: "1", Term: 2, Index: 1})
	if n.core.log.committed != 0 {
		t.Fatalf("committed %d from a previous term", n.core.log.committed)
	}
	n.core.Step(Message{Type: MsgAppResp, From: "2", To: "1", Term: 2, Index: 2})
	if n.core.log.committed != 2 {
		t.Fatalf("committed %d, want 2", n.core.log.committed)
	}
}

func TestPreVoteDoesNotDisrupt(t *testing.T) {
	nw := newNetwork(t, 3, preVote, checkQuorum)
	nw.campaign("1")
	nw.cut["3"] = true
	nw.tick("3", 5*testElection)
	if st := nw.status("3"); st.Term != 1 {
		t.Fatalf("isolated node moved to term %d", st.Term)
	}
	delete(nw.cut, "3")
	nw.tick("1", testHeartbeat)
	for _, id := range nw.ids {
		if st := nw.status(id); st.Term != 1 || st.Lead != "1" {
			t.Fatalf("%s: term %d lead %q", id, st.Term, st.Lead)
		}
	}
}

func TestCheckQuorumStepDown(t *testing.T) {
	nw := newNetwork(t, 3, checkQuorum)
	nw.campaign("1")
	nw.cut["1"] = true
	nw.tick("1", 2*testElection+1)
	if st := nw.status("1"); st.State == StateLeader {
		t.Fatal("isolated leader kept its leadership")
	}
}

func TestVoteIgnoredInLease(t *testing.T) {
	nw := newNetwork(t, 3, checkQuorum)
	nw.campaign("1")
	f := nw.nodes["2"].core
	f.Step(Message{Type: MsgVote, From: "3", To: "2", Term: 5, Index: 100, LogTerm: 5})
	if st := f.Status(); st.Term != 1 || st.Vote != "1" && st.Vote != None {
		t.Fatalf("follower in lease reacted to a vote: %+v", st)
	}
}

func TestStaleLeaderStepsDown(t *testing.T) {
	nw := newNetwork(t, 3, checkQuorum)
	nw.campaign("1")
	nw.cut["1"] = true
	for range 3 * testElection {
		nw.tick("2", 1)
		nw.tick("3", 1)
	}
	if nw.status("2").State != StateLeader && nw.status("3").State != StateLeader {
		t.Fatal("no new leader")
	}
	delete(nw.cut, "1")
	nw.tick("1", testHeartbeat)
	if st := nw.status("1"); st.State == StateLeader {
		t.Fatalf("stale leader still leads: %+v", st)
	}
}

func TestSnapshotToLaggingFollower(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	nw.cut["3"] = true
	for i := range 20 {
		nw.propose(t, "1", fmt.Sprint(i))
	}
	ln := nw.nodes["1"]
	applied := ln.core.log.applied
	if _, err := ln.storage.CreateSnapshot(applied, ConfState{Voters: nw.ids}); err != nil {
		t.Fatal(err)
	}
	if err := ln.storage.Compact(applied - 2); err != nil {
		t.Fatal(err)
	}
	delete(nw.cut, "3")
	snaps := 0
	nw.filter = func(m Message) bool {
		if m.Type == MsgSnap {
			snaps++
		}
		return true
	}
	nw.tick("1", testHeartbeat)
	nw.propose(t, "1", "after")
	if snaps != 1 {
		t.Fatalf("%d snapshots sent", snaps)
	}
	f := nw.nodes["3"]
	if snap, _ := f.storage.Snapshot(); snap.Index != applied {
		t.Fatalf("follower snapshot %d, want %d", snap.Index, applied)
	}
	if got := data(f.applied); !slices.Equal(got, []string{"after"}) {
		t.Fatalf("follower applied %v after the snapshot", got)
	}
	if a, b := nw.status("1").Commit, nw.status("3").Commit; a != b {
		t.Fatalf("follower commit %d, leader %d", b, a)
	}
}

func TestRestartReplaysAfterApplied(t *testing.T) {
	s := NewMemoryStorage(ConfState{Voters: ids(3)})
	appendTerms(s, 1, 1, 10)
	s.SetHardState(HardState{Term: 1, Commit: 10})
	c := newTestCore(t, "1", s, func(cfg *Config) { cfg.Applied = 7 })
	rd := c.Ready()
	if len(rd.Committed) != 3 || rd.Committed[0].Index != 8 {
		t.Fatalf("replayed %+v", rd.Committed)
	}
}

func TestRestartClampsCommit(t *testing.T) {
	s := NewMemoryStorage(ConfState{Voters: ids(3)})
	appendTerms(s, 1, 1, 10)
	s.SetHardState(HardState{Term: 1, Commit: 20})
	c := newTestCore(t, "1", s)
	if st := c.Status(); st.Commit != 10 {
		t.Fatalf("commit %d, want 10", st.Commit)
	}
}

func TestPipelinedReadiesWithTruncation(t *testing.T) {
	s := NewMemoryStorage(ConfState{Voters: ids(3)})
	n := &testNode{core: newTestCore(t, "1", s), storage: s}
	c := n.core
	c.Step(Message{Type: MsgApp, From: "2", To: "1", Term: 1, Entries: []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}})
	rd1 := c.Ready()
	c.Step(Message{Type: MsgApp, From: "3", To: "1", Term: 2, Index: 1, LogTerm: 1, Entries: []Entry{{Index: 2, Term: 2}}})
	rd2 := c.Ready()
	if len(rd2.Entries) != 1 || rd2.Entries[0].Index != 2 {
		t.Fatalf("second ready entries %+v", rd2.Entries)
	}
	n.persist(rd1)
	c.Advance(rd1)
	if c.log.lastStable() != 0 {
		t.Fatalf("after first ready: stable %d, want 0", c.log.lastStable())
	}
	if got := rd1.Entries[1].Term; got != 1 {
		t.Fatalf("a handed-out entry was overwritten in place: term %d", got)
	}
	n.persist(rd2)
	c.Advance(rd2)
	if c.log.lastStable() != 2 || c.log.lastIndex() != 2 {
		t.Fatalf("stable %d last %d, want 2/2", c.log.lastStable(), c.log.lastIndex())
	}
}

func TestHardStateCommitCoversOnlyDurableEntries(t *testing.T) {
	s := NewMemoryStorage(ConfState{Voters: ids(3)})
	n := &testNode{core: newTestCore(t, "1", s), storage: s}
	c := n.core
	c.Step(Message{Type: MsgApp, From: "2", To: "1", Term: 1, Commit: 3,
		Entries: []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}})
	rd := c.Ready()
	if len(rd.Entries) != 3 || rd.HardState.Commit != 0 {
		t.Fatalf("first ready: %d entries, persisted commit %d, want 3 and 0", len(rd.Entries), rd.HardState.Commit)
	}
	if len(rd.Committed) != 0 {
		t.Fatalf("committed %d entries before they are durable, want 0", len(rd.Committed))
	}
	n.persist(rd)
	c.Advance(rd)
	rd = c.Ready()
	if rd.HardState.Commit != 3 || rd.MustSync {
		t.Fatalf("second ready: commit %d must-sync %v, want 3 and false", rd.HardState.Commit, rd.MustSync)
	}
	if len(rd.Committed) != 3 {
		t.Fatalf("second ready: committed %d entries, want 3", len(rd.Committed))
	}
}

func TestVoteRequestsAreRetried(t *testing.T) {
	for _, pv := range []bool{false, true} {
		t.Run(fmt.Sprintf("prevote=%v", pv), func(t *testing.T) {
			var opts []func(*Config)
			if pv {
				opts = append(opts, preVote)
			}
			nw := newNetwork(t, 3, opts...)
			dropped := false
			nw.filter = func(m Message) bool {
				if (m.Type == MsgVote || m.Type == MsgPreVote) && !dropped {
					dropped = true
					return false
				}
				return true
			}
			nw.cut["3"] = true
			c := nw.nodes["1"].core
			c.campaign(c.preVote)
			nw.deliver(nw.nodes["1"].ready())
			if c.state == StateLeader {
				t.Fatal("won although the only reachable vote request was dropped")
			}
			nw.tick("1", testHeartbeat)
			if c.state != StateLeader {
				t.Fatalf("still %s after the retry", c.state)
			}
		})
	}
}

func TestPreVoteTieBreak(t *testing.T) {
	nw := newNetwork(t, 3, preVote)
	nw.cut["3"] = true
	n1, n2 := nw.nodes["1"].core, nw.nodes["2"].core
	n1.campaign(true)
	n2.campaign(true)
	nw.deliver(append(nw.nodes["1"].ready(), nw.nodes["2"].ready()...))
	if n2.state != StateLeader || n1.state != StateFollower || n1.term != n2.term || n2.term != 1 {
		t.Fatalf("n1 %s term %d, n2 %s term %d: want a single election won by the higher ID",
			n1.state, n1.term, n2.state, n2.term)
	}
}

func TestHeartbeatResponseDoesNotWaitForDisk(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign("1")
	f := nw.nodes["2"].core
	f.Step(Message{Type: MsgHeartbeat, From: "1", To: "2", Term: 1, Commit: 1})
	rd := f.Ready()
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgHeartbeatResp || len(rd.MessagesAfterPersist) != 0 {
		t.Fatalf("heartbeat response held back: %+v / %+v", rd.Messages, rd.MessagesAfterPersist)
	}
}
