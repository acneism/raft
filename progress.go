package raft

import "slices"

type ProgressState uint8

const (
	ProgressProbe ProgressState = iota
	ProgressReplicate
	ProgressSnapshot
)

type Progress struct {
	Match, Next     uint64
	State           ProgressState
	PendingSnapshot uint64
	RecentActive    bool
	probeSent       bool
	forceSend       bool
	sentCommit      uint64
	readAck         uint64
	inflights       inflights
	IsLearner       bool
}

func (pr *Progress) reset(state ProgressState) {
	pr.probeSent = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.inflights.reset()
}

func (pr *Progress) becomeProbe() {
	if pr.State == ProgressSnapshot {
		pending := pr.PendingSnapshot
		pr.reset(ProgressProbe)
		pr.Next = max(pr.Match+1, pending+1)
		return
	}
	pr.reset(ProgressProbe)
	pr.Next = pr.Match + 1
}

func (pr *Progress) becomeReplicate() {
	pr.reset(ProgressReplicate)
	pr.Next = pr.Match + 1
}

func (pr *Progress) becomeSnapshot(i uint64) {
	pr.reset(ProgressSnapshot)
	pr.PendingSnapshot = i
	pr.Next = i + 1
}

func (pr *Progress) maybeUpdate(n uint64) bool {
	pr.Next = max(pr.Next, n+1)
	if n <= pr.Match {
		return false
	}
	pr.Match = n
	pr.probeSent = false
	return true
}

func (pr *Progress) maybeDecrTo(rejected, matchHint uint64) bool {
	if pr.State == ProgressReplicate {
		if rejected <= pr.Match {
			return false
		}
		pr.Next = pr.Match + 1
		return true
	}
	if pr.Next-1 != rejected {
		return false
	}
	pr.Next = max(min(rejected, matchHint+1), pr.Match+1)
	pr.probeSent = false
	return true
}

func (pr *Progress) isPaused() bool {
	switch pr.State {
	case ProgressProbe:
		return pr.probeSent
	case ProgressReplicate:
		return pr.inflights.full()
	}
	return true
}

func (pr *Progress) sentEntries(n int, bytes uint64) {
	if n == 0 {
		return
	}
	switch pr.State {
	case ProgressReplicate:
		pr.Next += uint64(n)
		pr.inflights.add(pr.Next-1, bytes)
	case ProgressProbe:
		pr.probeSent = true
	}
}

type inflights struct {
	maxCount int
	maxBytes uint64
	bytes    uint64
	buf      []inflight
}

type inflight struct {
	index uint64
	bytes uint64
}

func (in *inflights) add(index, bytes uint64) {
	in.buf = append(in.buf, inflight{index, bytes})
	in.bytes += bytes
}

func (in *inflights) freeLE(index uint64) {
	n := 0
	for n < len(in.buf) && in.buf[n].index <= index {
		in.bytes -= in.buf[n].bytes
		n++
	}
	in.buf = in.buf[n:]
	if len(in.buf) == 0 {
		in.buf = in.buf[:0:0]
	}
}

func (in *inflights) freeFirst() {
	if len(in.buf) > 0 {
		in.freeLE(in.buf[0].index)
	}
}

func (in *inflights) full() bool {
	return len(in.buf) >= in.maxCount || (in.maxBytes != 0 && in.bytes >= in.maxBytes)
}

func (in *inflights) reset() {
	in.buf = in.buf[:0:0]
	in.bytes = 0
}

type tracker struct {
	voters   []NodeID
	ids      []NodeID
	progress map[NodeID]*Progress
	votes    map[NodeID]bool
	matchBuf []uint64
}

func newTracker(cs ConfState, maxInflight int, maxInflightBytes uint64) tracker {
	t := tracker{
		voters:   slices.Sorted(slices.Values(cs.Voters)),
		progress: make(map[NodeID]*Progress),
		votes:    make(map[NodeID]bool),
	}
	t.voters = slices.Compact(t.voters)
	for _, id := range t.voters {
		t.progress[id] = &Progress{}
	}
	for _, id := range cs.Learners {
		if _, ok := t.progress[id]; !ok {
			t.progress[id] = &Progress{IsLearner: true}
		}
	}
	for id, pr := range t.progress {
		pr.inflights = inflights{maxCount: maxInflight, maxBytes: maxInflightBytes}
		t.ids = append(t.ids, id)
	}
	slices.Sort(t.ids)
	return t
}

func (t *tracker) isVoter(id NodeID) bool {
	_, ok := slices.BinarySearch(t.voters, id)
	return ok
}

func (t *tracker) quorum() int { return len(t.voters)/2 + 1 }

func (t *tracker) committed() uint64 {
	t.matchBuf = t.matchBuf[:0]
	for _, id := range t.voters {
		t.matchBuf = append(t.matchBuf, t.progress[id].Match)
	}
	slices.Sort(t.matchBuf)
	return t.matchBuf[len(t.matchBuf)-t.quorum()]
}

type voteResult uint8

const (
	votePending voteResult = iota
	voteWon
	voteLost
)

func (t *tracker) recordVote(id NodeID, granted bool) voteResult {
	if _, ok := t.votes[id]; !ok && t.isVoter(id) {
		t.votes[id] = granted
	}
	var yes, no int
	for _, v := range t.votes {
		if v {
			yes++
		} else {
			no++
		}
	}
	switch q := t.quorum(); {
	case yes >= q:
		return voteWon
	case no > len(t.voters)-q:
		return voteLost
	}
	return votePending
}

func (t *tracker) resetVotes() { clear(t.votes) }

func (t *tracker) quorumActive(self NodeID) bool {
	n := 0
	for _, id := range t.voters {
		if id == self || t.progress[id].RecentActive {
			n++
		}
	}
	return n >= t.quorum()
}

func (t *tracker) readConfirmed(self NodeID) uint64 {
	t.matchBuf = t.matchBuf[:0]
	for _, id := range t.voters {
		ack := t.progress[id].readAck
		if id == self {
			ack = ^uint64(0)
		}
		t.matchBuf = append(t.matchBuf, ack)
	}
	slices.Sort(t.matchBuf)
	return t.matchBuf[len(t.matchBuf)-t.quorum()]
}

func (t *tracker) update(cs ConfState, maxInflight int, maxInflightBytes uint64, last uint64) {
	next := newTracker(cs, maxInflight, maxInflightBytes)
	for id, pr := range next.progress {
		if old, ok := t.progress[id]; ok {
			old.IsLearner = pr.IsLearner
			next.progress[id] = old
		} else {
			pr.Next = last + 1
		}
	}
	next.votes = t.votes
	*t = next
}
