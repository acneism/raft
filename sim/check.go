package sim

import (
	"github.com/acneism/raft"
)

type entryKey struct{ index, term uint64 }

type entryInfo struct{ hash, prevTerm uint64 }

type checker struct {
	s          *Sim
	leaders    map[uint64]raft.NodeID
	entries    map[entryKey]entryInfo
	commitTerm []uint64
	commitObs  []uint64
	commitAt   []uint64
	states     []uint64
	maxCommit  uint64
}

func newChecker(s *Sim) checker {
	return checker{s: s, leaders: map[uint64]raft.NodeID{}, entries: map[entryKey]entryInfo{}}
}

func at(sl []uint64, i uint64) uint64 {
	if i < uint64(len(sl)) {
		return sl[i]
	}
	return 0
}

func set(sl []uint64, i, v uint64) []uint64 {
	for uint64(len(sl)) <= i {
		sl = append(sl, 0)
	}
	sl[i] = v
	return sl
}

func (c *checker) ready(n *node, rd raft.Ready) {
	st := n.core.Status()
	v := &n.view
	if s := rd.Snapshot; s != nil {
		if s.Index <= n.commit {
			c.s.fail("%s: snapshot %d at or below commit %d", n.id, s.Index, n.commit)
			return
		}
		if t := at(c.commitTerm, s.Index); t != 0 && t != s.Term {
			c.s.fail("%s: snapshot (%d, %d) contradicts committed term %d", n.id, s.Index, s.Term, t)
			return
		}
		v.reset(s.Index, s.Term)
	}
	if len(rd.Entries) > 0 {
		first := rd.Entries[0].Index
		switch {
		case first > v.last()+1:
			c.s.fail("%s: entries start at %d after last index %d", n.id, first, v.last())
			return
		case first <= n.commit:
			c.s.fail("%s: entries from %d overwrite committed index %d", n.id, first, n.commit)
			return
		case first <= v.last() && st.State == raft.StateLeader && n.lastReady == st.Term:
			c.s.fail("Leader Append-Only: leader %s of term %d overwrites from %d, last %d", n.id, st.Term, first, v.last())
			return
		}
		v.terms = v.terms[:first-v.base]
		for _, e := range rd.Entries {
			prev := v.terms[len(v.terms)-1]
			if e.Index != v.last()+1 || e.Term < prev {
				c.s.fail("%s: malformed entry (%d, %d) after (%d, %d)", n.id, e.Index, e.Term, v.last(), prev)
				return
			}
			key, info := entryKey{e.Index, e.Term}, entryInfo{dataHash(e), prev}
			if old, ok := c.entries[key]; ok && old != info {
				c.s.fail("Log Matching: %s has entry (%d, %d) %+v, seen before as %+v", n.id, e.Index, e.Term, info, old)
				return
			}
			c.entries[key] = info
			v.terms = append(v.terms, e.Term)
		}
	}
	if st.LastIndex != v.last() {
		c.s.fail("%s: core last index %d, handed out up to %d", n.id, st.LastIndex, v.last())
		return
	}
	if st.Commit > n.commit {
		c.committed(n, n.commit+1, st.Commit, st.Term)
		n.commit = st.Commit
	}
	n.lastReady = 0
	if st.State == raft.StateLeader {
		n.lastReady = st.Term
	}
}

func (c *checker) committed(n *node, lo, hi, term uint64) {
	for i := lo; i <= hi; i++ {
		t, ok := n.view.term(i)
		if !ok {
			if i < n.view.base {
				continue
			}
			c.s.fail("%s: commit %d past its log (last %d)", n.id, hi, n.view.last())
			return
		}
		if known := at(c.commitTerm, i); known != 0 {
			if known != t {
				c.s.fail("State Machine Safety: %s commits (%d, %d), committed before with term %d", n.id, i, t, known)
				return
			}
			continue
		}
		c.commitTerm = set(c.commitTerm, i, t)
		c.commitObs = set(c.commitObs, i, term)
		c.commitAt = set(c.commitAt, i, uint64(c.s.now))
		c.maxCommit = max(c.maxCommit, i)
		if !c.durableOnQuorum(i, t) {
			c.s.fail("%s commits (%d, %d) that is not on the disks of a quorum", n.id, i, t)
			return
		}
	}
}

func (c *checker) durableOnQuorum(i, t uint64) bool {
	count := 0
	for _, n := range c.s.nodes {
		if snap, _ := n.disk.Snapshot(); snap.Index >= i {
			count++
		} else if dt, err := n.disk.Term(i); err == nil && dt == t {
			count++
		}
	}
	return count >= len(c.s.nodes)/2+1
}

func (c *checker) apply(n *node, e raft.Entry) {
	switch {
	case e.Index != n.fsm.index+1:
		c.s.fail("%s: applies %d after %d", n.id, e.Index, n.fsm.index)
		return
	case e.Index > n.commit:
		c.s.fail("%s: applies %d before commit reached it (%d)", n.id, e.Index, n.commit)
		return
	}
	if t := at(c.commitTerm, e.Index); t != 0 && t != e.Term {
		c.s.fail("State Machine Safety: %s applies (%d, %d), committed term %d", n.id, e.Index, e.Term, t)
		return
	}
	h := mix(n.fsm.hash, e)
	if known := at(c.states, e.Index); known != 0 && known != h {
		c.s.fail("State Machine Safety: %s diverges at %d", n.id, e.Index)
		return
	}
	c.states = set(c.states, e.Index, h)
}

func (c *checker) restarted(n *node) {
	hs, _, _ := n.disk.InitialState()
	last, _ := n.disk.LastIndex()
	for i := n.fsm.index + 1; i <= min(hs.Commit, last); i++ {
		t, _ := n.disk.Term(i)
		if ct := at(c.commitTerm, i); ct != 0 && ct != t {
			c.s.fail("%s restarts with commit %d over (%d, %d), committed term %d", n.id, hs.Commit, i, t, ct)
			return
		}
	}
}

func (c *checker) restored(n *node, st fsmState) {
	if known := at(c.states, st.index); known != st.hash {
		c.s.fail("State Machine Safety: %s restores snapshot %d with state %x, expected %x", n.id, st.index, st.hash, known)
	}
}

func (c *checker) status(n *node) {
	st := n.core.Status()
	if st.State != raft.StateLeader {
		return
	}
	if l, ok := c.leaders[st.Term]; ok && l != n.id {
		c.s.fail("Election Safety: %s and %s are both leaders of term %d", l, n.id, st.Term)
		return
	}
	c.leaders[st.Term] = n.id
	if n.leaderIn != st.Term {
		n.leaderIn = st.Term
		c.leaderCompleteness(n, st.Term)
	}
}

func (c *checker) leaderCompleteness(n *node, term uint64) {
	v := &n.view
	for i := max(v.base, 1); i <= c.maxCommit; i++ {
		ct := at(c.commitTerm, i)
		if ct == 0 || at(c.commitObs, i) >= term {
			continue
		}
		if t, ok := v.term(i); !ok || t != ct {
			c.s.fail("Leader Completeness: leader %s of term %d lacks committed (%d, %d)", n.id, term, i, ct)
			return
		}
	}
}

func (c *checker) prune() {
	low := ^uint64(0)
	for _, n := range c.s.nodes {
		first, _ := n.disk.FirstIndex()
		low = min(low, first-1)
		if n.up {
			low = min(low, n.view.base)
		}
	}
	for k := range c.entries {
		if k.index < low {
			delete(c.entries, k)
		}
	}
}
