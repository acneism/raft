package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
)

type Config struct {
	ID                       NodeID
	ElectionTick             int
	HeartbeatTick            int
	Storage                  Storage
	Applied                  uint64
	MaxSizePerMsg            uint64
	MaxInflightMsgs          int
	MaxInflightBytes         uint64
	MaxCommittedSizePerReady uint64
	PreVote                  bool
	CheckQuorum              bool
	Rand                     *rand.Rand
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("raft: empty ID")
	}
	if c.HeartbeatTick <= 0 {
		return errors.New("raft: HeartbeatTick must be positive")
	}
	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("raft: ElectionTick must be greater than HeartbeatTick")
	}
	if c.Storage == nil {
		return errors.New("raft: nil Storage")
	}
	if c.MaxSizePerMsg == 0 {
		c.MaxSizePerMsg = 1 << 20
	}
	if c.MaxInflightMsgs <= 0 {
		c.MaxInflightMsgs = 256
	}
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = 16 << 20
	}
	if c.Rand == nil {
		c.Rand = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	return nil
}

type Core struct {
	id    NodeID
	term  uint64
	vote  NodeID
	lead  NodeID
	state StateType

	log *raftLog
	trk tracker

	noopIndex   uint64
	durableTerm uint64
	sendPending bool

	msgs             []Message
	msgsAfterPersist []Message

	electionElapsed           int
	heartbeatElapsed          int
	electionTimeout           int
	heartbeatTimeout          int
	randomizedElectionTimeout int
	checkQuorum               bool
	preVote                   bool
	maxMsgSize                uint64
	maxInflight               int
	maxInflightBytes          uint64
	rand                      *rand.Rand

	prevSoft SoftState
	prevHard HardState
}

func New(cfg Config) (*Core, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	log, err := newLog(cfg.Storage, cfg.MaxCommittedSizePerReady)
	if err != nil {
		return nil, err
	}
	hs, cs, err := cfg.Storage.InitialState()
	if err != nil {
		return nil, err
	}
	c := &Core{
		id:               cfg.ID,
		log:              log,
		trk:              newTracker(cs, cfg.MaxInflightMsgs, cfg.MaxInflightBytes),
		electionTimeout:  cfg.ElectionTick,
		heartbeatTimeout: cfg.HeartbeatTick,
		checkQuorum:      cfg.CheckQuorum,
		preVote:          cfg.PreVote,
		maxMsgSize:       cfg.MaxSizePerMsg,
		maxInflight:      cfg.MaxInflightMsgs,
		maxInflightBytes: cfg.MaxInflightBytes,
		rand:             cfg.Rand,
	}
	if len(c.trk.voters) == 0 {
		return nil, errors.New("raft: configuration has no voters")
	}
	c.term, c.vote, c.durableTerm = hs.Term, hs.Vote, hs.Term
	last := log.lastIndex()
	if cfg.Applied > last {
		return nil, fmt.Errorf("raft: applied index %d is past last index %d", cfg.Applied, last)
	}
	log.committed = max(log.committed, min(hs.Commit, last), cfg.Applied)
	log.appliedTo(cfg.Applied)
	c.becomeFollower(c.term, None)
	c.prevSoft = c.softState()
	c.prevHard = HardState{Term: hs.Term, Vote: hs.Vote, Commit: log.committed}
	return c, nil
}

func (c *Core) softState() SoftState { return SoftState{Lead: c.lead, State: c.state} }

func (c *Core) hardState() HardState {
	commit := min(c.log.committed, c.log.lastStable())
	return HardState{Term: c.term, Vote: c.vote, Commit: max(commit, c.prevHard.Commit)}
}

type Status struct {
	ID NodeID
	HardState
	SoftState
	Applied     uint64
	LastIndex   uint64
	LeaderReady bool
}

func (c *Core) Status() Status {
	return Status{
		ID:          c.id,
		HardState:   HardState{Term: c.term, Vote: c.vote, Commit: c.log.committed},
		SoftState:   c.softState(),
		Applied:     c.log.applied,
		LastIndex:   c.log.lastIndex(),
		LeaderReady: c.leaderReady(),
	}
}

func (c *Core) leaderReady() bool {
	return c.state == StateLeader && c.log.applied >= c.noopIndex
}

func (c *Core) Propose(data []byte) (index, term uint64, err error) {
	if !c.leaderReady() || !c.trk.isVoter(c.id) {
		return 0, 0, ErrNotLeader
	}
	index = c.appendEntry(Entry{Type: EntryNormal, Data: data})
	return index, c.term, nil
}

func (c *Core) appendEntry(e Entry) uint64 {
	e.Index = c.log.lastIndex() + 1
	e.Term = c.term
	c.log.append(e)
	c.sendPending = true
	return e.Index
}

func (c *Core) Tick() {
	if c.state == StateLeader {
		c.tickHeartbeat()
	} else {
		c.tickElection()
	}
}

func (c *Core) tickElection() {
	c.electionElapsed++
	if c.promotable() && c.electionElapsed >= c.randomizedElectionTimeout {
		c.electionElapsed = 0
		c.campaign(c.preVote)
	}
}

func (c *Core) tickHeartbeat() {
	c.heartbeatElapsed++
	c.electionElapsed++
	if c.electionElapsed >= c.electionTimeout {
		c.electionElapsed = 0
		if c.checkQuorum {
			if !c.trk.quorumActive(c.id) {
				c.becomeFollower(c.term, None)
				return
			}
			for _, id := range c.trk.ids {
				if id != c.id {
					c.trk.progress[id].RecentActive = false
				}
			}
		}
	}
	if c.heartbeatElapsed >= c.heartbeatTimeout {
		c.heartbeatElapsed = 0
		c.bcastHeartbeat()
	}
}

func (c *Core) promotable() bool {
	return c.trk.isVoter(c.id) && c.log.unstable.snapshot == nil
}

func (c *Core) resetRandomizedElectionTimeout() {
	c.randomizedElectionTimeout = c.electionTimeout + c.rand.IntN(c.electionTimeout)
}

func (c *Core) reset(term uint64) {
	if c.term != term {
		c.term = term
		c.vote = None
	}
	c.lead = None
	c.electionElapsed = 0
	c.heartbeatElapsed = 0
	c.resetRandomizedElectionTimeout()
	c.trk.resetVotes()
	c.noopIndex = 0
	c.sendPending = false
	last := c.log.lastIndex()
	for _, id := range c.trk.ids {
		pr := c.trk.progress[id]
		pr.reset(ProgressProbe)
		pr.Match, pr.Next = 0, last+1
		pr.RecentActive, pr.forceSend, pr.sentCommit = false, false, 0
		if id == c.id {
			pr.Match = c.log.lastStable()
		}
	}
}

func (c *Core) becomeFollower(term uint64, lead NodeID) {
	c.reset(term)
	c.lead = lead
	c.state = StateFollower
}

func (c *Core) becomePreCandidate() {
	c.trk.resetVotes()
	c.lead = None
	c.state = StatePreCandidate
}

func (c *Core) becomeCandidate() {
	c.reset(c.term + 1)
	c.vote = c.id
	c.state = StateCandidate
}

func (c *Core) becomeLeader() {
	c.reset(c.term)
	c.lead = c.id
	c.state = StateLeader
	self := c.trk.progress[c.id]
	self.becomeReplicate()
	self.RecentActive = true
	c.noopIndex = c.appendEntry(Entry{Type: EntryNoop})
}

func (c *Core) campaign(pre bool) {
	voteType, term := MsgVote, c.term+1
	if pre {
		voteType = MsgPreVote
		c.becomePreCandidate()
	} else {
		c.becomeCandidate()
		term = c.term
	}
	if c.trk.recordVote(c.id, true) == voteWon {
		if pre {
			c.campaign(false)
		} else {
			c.becomeLeader()
		}
		return
	}
	last, lastTerm := c.log.lastIndex(), c.log.lastTerm()
	for _, id := range c.trk.voters {
		if id != c.id {
			c.send(Message{To: id, Type: voteType, Term: term, Index: last, LogTerm: lastTerm})
		}
	}
}

func (c *Core) send(m Message) {
	m.From = c.id
	if m.Term == 0 {
		m.Term = c.term
	}
	early := c.state == StateLeader && c.durableTerm == c.term &&
		(m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap)
	if early {
		c.msgs = append(c.msgs, m)
	} else {
		c.msgsAfterPersist = append(c.msgsAfterPersist, m)
	}
}

func (c *Core) Step(m Message) error {
	switch {
	case m.Term > c.term:
		if m.Type == MsgVote || m.Type == MsgPreVote {
			if c.checkQuorum && c.lead != None && c.electionElapsed < c.electionTimeout {
				return nil
			}
		}
		switch {
		case m.Type == MsgPreVote:
		case m.Type == MsgPreVoteResp && !m.Reject:
		case m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap:
			c.becomeFollower(m.Term, m.From)
		default:
			c.becomeFollower(m.Term, None)
		}
	case m.Term < c.term:
		switch {
		case (c.checkQuorum || c.preVote) && (m.Type == MsgApp || m.Type == MsgHeartbeat):
			c.send(Message{To: m.From, Type: MsgAppResp})
		case m.Type == MsgPreVote:
			c.send(Message{To: m.From, Type: MsgPreVoteResp, Reject: true})
		}
		return nil
	}

	switch m.Type {
	case MsgVote, MsgPreVote:
		c.handleVote(m)
		return nil
	}
	switch c.state {
	case StateLeader:
		c.stepLeader(m)
	case StateCandidate, StatePreCandidate:
		c.stepCandidate(m)
	default:
		c.stepFollower(m)
	}
	return nil
}

func (c *Core) handleVote(m Message) {
	respType := MsgVoteResp
	if m.Type == MsgPreVote {
		respType = MsgPreVoteResp
	}
	canVote := c.vote == m.From ||
		(c.vote == None && c.lead == None) ||
		(m.Type == MsgPreVote && m.Term > c.term)
	if !canVote || !c.log.isUpToDate(m.Index, m.LogTerm) {
		c.send(Message{To: m.From, Type: respType, Reject: true})
		return
	}
	c.send(Message{To: m.From, Type: respType, Term: m.Term})
	if m.Type == MsgVote {
		c.electionElapsed = 0
		c.vote = m.From
	}
}

func (c *Core) stepFollower(m Message) {
	switch m.Type {
	case MsgApp:
		c.electionElapsed = 0
		c.lead = m.From
		c.handleAppendEntries(m)
	case MsgHeartbeat:
		c.electionElapsed = 0
		c.lead = m.From
		c.handleHeartbeat(m)
	case MsgSnap:
		c.electionElapsed = 0
		c.lead = m.From
		c.handleSnapshot(m)
	}
}

func (c *Core) stepCandidate(m Message) {
	respType := MsgVoteResp
	if c.state == StatePreCandidate {
		respType = MsgPreVoteResp
	}
	switch m.Type {
	case MsgApp:
		c.becomeFollower(m.Term, m.From)
		c.handleAppendEntries(m)
	case MsgHeartbeat:
		c.becomeFollower(m.Term, m.From)
		c.handleHeartbeat(m)
	case MsgSnap:
		c.becomeFollower(m.Term, m.From)
		c.handleSnapshot(m)
	case respType:
		switch c.trk.recordVote(m.From, !m.Reject) {
		case voteWon:
			if c.state == StatePreCandidate {
				c.campaign(false)
			} else {
				c.becomeLeader()
			}
		case voteLost:
			c.becomeFollower(c.term, None)
		}
	}
}

func (c *Core) stepLeader(m Message) {
	pr := c.trk.progress[m.From]
	if pr == nil || m.From == c.id {
		return
	}
	switch m.Type {
	case MsgAppResp:
		pr.RecentActive = true
		c.sendPending = true
		if m.Reject {
			next := m.RejectHint
			if m.LogTerm > 0 {
				next, _ = c.log.findConflictByTerm(m.RejectHint, m.LogTerm)
			}
			if pr.maybeDecrTo(m.Index, next) && pr.State == ProgressReplicate {
				pr.becomeProbe()
			}
			return
		}
		if pr.maybeUpdate(m.Index) || (pr.Match == m.Index && pr.State == ProgressProbe) {
			switch {
			case pr.State == ProgressProbe:
				pr.becomeReplicate()
			case pr.State == ProgressSnapshot && pr.Match+1 >= c.log.firstIndex():
				pr.becomeReplicate()
			case pr.State == ProgressReplicate:
				pr.inflights.freeLE(m.Index)
			}
			c.maybeCommit()
		}
	case MsgHeartbeatResp:
		pr.RecentActive = true
		pr.probeSent = false
		if pr.Match < c.log.lastIndex() || pr.State == ProgressProbe {
			if pr.State == ProgressReplicate && pr.inflights.full() {
				pr.inflights.freeFirst()
			}
			pr.forceSend = true
			c.sendPending = true
		}
	}
}

func (c *Core) maybeCommit() {
	if c.log.maybeCommit(c.trk.committed(), c.term) {
		c.sendPending = true
	}
}

func (c *Core) bcastHeartbeat() {
	for _, id := range c.trk.ids {
		if id == c.id {
			continue
		}
		commit := min(c.trk.progress[id].Match, c.log.committed)
		c.send(Message{To: id, Type: MsgHeartbeat, Commit: commit})
	}
}

func (c *Core) sendAppends() {
	for _, id := range c.trk.ids {
		if id == c.id {
			continue
		}
		pr := c.trk.progress[id]
		for c.maybeSendAppend(id, pr, false) {
		}
		if pr.forceSend || pr.sentCommit < c.log.committed {
			c.maybeSendAppend(id, pr, true)
		}
		pr.forceSend = false
	}
}

func (c *Core) maybeSendAppend(to NodeID, pr *Progress, sendIfEmpty bool) bool {
	if pr.isPaused() {
		return false
	}
	prevIndex := pr.Next - 1
	prevTerm, errt := c.log.term(prevIndex)
	ents, erre := c.log.entries(pr.Next, c.maxMsgSize)
	if errt != nil || erre != nil {
		return c.maybeSendSnapshot(to, pr)
	}
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}
	c.send(Message{To: to, Type: MsgApp, Index: prevIndex, LogTerm: prevTerm, Entries: ents, Commit: c.log.committed})
	pr.sentEntries(len(ents), entsSize(ents))
	pr.sentCommit = c.log.committed
	return true
}

func (c *Core) maybeSendSnapshot(to NodeID, pr *Progress) bool {
	if !pr.RecentActive {
		return false
	}
	snap, err := c.log.snapshot()
	if errors.Is(err, ErrSnapshotTemporarilyUnavailable) {
		return false
	}
	if err != nil {
		panic(err)
	}
	if snap.Index == 0 || snap.Index+1 < c.log.firstIndex() {
		panic(fmt.Sprintf("raft: snapshot %d does not cover the compacted log (first index %d)", snap.Index, c.log.firstIndex()))
	}
	pr.becomeSnapshot(snap.Index)
	c.send(Message{To: to, Type: MsgSnap, Snapshot: &snap})
	return true
}

func (c *Core) ReportSnapshot(id NodeID, failed bool) {
	pr := c.trk.progress[id]
	if c.state != StateLeader || pr == nil || pr.State != ProgressSnapshot {
		return
	}
	if failed {
		pr.PendingSnapshot = 0
	}
	pr.becomeProbe()
	pr.probeSent = true
}

func (c *Core) ReportUnreachable(id NodeID) {
	pr := c.trk.progress[id]
	if c.state == StateLeader && pr != nil && pr.State == ProgressReplicate {
		pr.becomeProbe()
	}
}

func (c *Core) handleAppendEntries(m Message) {
	if m.Index < c.log.committed {
		c.send(Message{To: m.From, Type: MsgAppResp, Index: c.log.committed})
		return
	}
	if last, ok := c.log.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries); ok {
		c.send(Message{To: m.From, Type: MsgAppResp, Index: last})
		return
	}
	hint, hintTerm := c.log.findConflictByTerm(min(m.Index, c.log.lastIndex()), m.LogTerm)
	c.send(Message{To: m.From, Type: MsgAppResp, Index: m.Index, Reject: true, RejectHint: hint, LogTerm: hintTerm})
}

func (c *Core) handleHeartbeat(m Message) {
	c.log.commitTo(m.Commit)
	c.send(Message{To: m.From, Type: MsgHeartbeatResp})
}

func (c *Core) handleSnapshot(m Message) {
	if m.Snapshot == nil {
		return
	}
	if c.restore(*m.Snapshot) {
		c.send(Message{To: m.From, Type: MsgAppResp, Index: c.log.lastIndex()})
	} else {
		c.send(Message{To: m.From, Type: MsgAppResp, Index: c.log.committed})
	}
}

func (c *Core) restore(s SnapshotMeta) bool {
	if s.Index <= c.log.committed {
		return false
	}
	if c.log.matchTerm(s.Index, s.Term) {
		c.log.commitTo(s.Index)
		return false
	}
	trk := newTracker(s.Conf, c.maxInflight, c.maxInflightBytes)
	if trk.progress[c.id] == nil {
		return false
	}
	c.log.restore(s)
	c.trk = trk
	return true
}

func (c *Core) HasReady() bool {
	if c.softState() != c.prevSoft {
		return true
	}
	if hs := c.hardState(); hs != c.prevHard {
		return true
	}
	return len(c.msgs) > 0 || len(c.msgsAfterPersist) > 0 ||
		(c.state == StateLeader && c.sendPending) ||
		len(c.log.unstable.nextEntries()) > 0 || c.log.unstable.nextSnapshot() != nil ||
		c.log.hasNextCommittedEnts()
}

func (c *Core) Ready() Ready {
	if c.state == StateLeader && c.sendPending {
		c.sendPending = false
		c.sendAppends()
	}
	rd := Ready{
		Entries:              c.log.unstable.nextEntries(),
		Snapshot:             c.log.unstable.nextSnapshot(),
		Committed:            c.log.nextCommittedEnts(),
		Messages:             c.msgs,
		MessagesAfterPersist: c.msgsAfterPersist,
	}
	c.msgs, c.msgsAfterPersist = nil, nil
	if ss := c.softState(); ss != c.prevSoft {
		rd.SoftState = &ss
		c.prevSoft = ss
	}
	if hs := c.hardState(); hs != c.prevHard {
		rd.HardState = hs
		rd.MustSync = hs.Term != c.prevHard.Term || hs.Vote != c.prevHard.Vote
		c.prevHard = hs
	}
	rd.MustSync = rd.MustSync || len(rd.Entries) > 0 || rd.Snapshot != nil
	c.log.unstable.acceptInProgress()
	if n := len(rd.Committed); n > 0 {
		c.log.applying = rd.Committed[n-1].Index
	}
	return rd
}

func (c *Core) Advance(rd Ready) {
	c.durableTerm = max(c.durableTerm, rd.HardState.Term)
	if rd.Snapshot != nil {
		c.log.unstable.stableSnapTo(rd.Snapshot.Index)
		c.log.appliedTo(rd.Snapshot.Index)
	}
	if n := len(rd.Entries); n > 0 {
		c.log.unstable.stableTo(rd.Entries[n-1].Index, rd.Entries[n-1].Term)
	}
	if n := len(rd.Committed); n > 0 {
		c.log.appliedTo(rd.Committed[n-1].Index)
	}
	if c.state == StateLeader {
		if c.trk.progress[c.id].maybeUpdate(c.log.lastStable()) {
			c.maybeCommit()
		}
	}
}
