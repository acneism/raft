package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
)

type ConfChangeType uint8

const (
	ConfAddLearner ConfChangeType = iota + 1
	ConfPromote
	ConfRemove
)

type ConfChange struct {
	Type ConfChangeType
	Node NodeID
	Addr string
}

var (
	ErrConfChangePending = errors.New("raft: a configuration change is in progress")
	ErrConfChangeInvalid = errors.New("raft: invalid configuration change")
)

func (cs ConfState) Clone() ConfState {
	return ConfState{Voters: slices.Clone(cs.Voters), Learners: slices.Clone(cs.Learners), Addrs: maps.Clone(cs.Addrs)}
}

func (cs ConfState) IsVoter(id NodeID) bool { return slices.Contains(cs.Voters, id) }

func (cs ConfState) IsLearner(id NodeID) bool { return slices.Contains(cs.Learners, id) }

func (cs ConfState) Apply(cc ConfChange) (ConfState, error) {
	next := cs.Clone()
	if next.Addrs == nil {
		next.Addrs = map[NodeID]string{}
	}
	switch {
	case cc.Node == None:
		return cs, fmt.Errorf("%w: empty node ID", ErrConfChangeInvalid)
	case cc.Type == ConfAddLearner:
		if cs.IsVoter(cc.Node) || cs.IsLearner(cc.Node) {
			return cs, fmt.Errorf("%w: %s is already a member", ErrConfChangeInvalid, cc.Node)
		}
		if cc.Addr == "" {
			return cs, fmt.Errorf("%w: no address for %s", ErrConfChangeInvalid, cc.Node)
		}
		next.Learners = append(next.Learners, cc.Node)
		next.Addrs[cc.Node] = cc.Addr
	case cc.Type == ConfPromote:
		if !cs.IsLearner(cc.Node) {
			return cs, fmt.Errorf("%w: %s is not a learner", ErrConfChangeInvalid, cc.Node)
		}
		next.Learners = slices.DeleteFunc(next.Learners, func(id NodeID) bool { return id == cc.Node })
		next.Voters = append(next.Voters, cc.Node)
	case cc.Type == ConfRemove:
		switch {
		case cs.IsLearner(cc.Node):
			next.Learners = slices.DeleteFunc(next.Learners, func(id NodeID) bool { return id == cc.Node })
		case cs.IsVoter(cc.Node) && len(cs.Voters) > 1:
			next.Voters = slices.DeleteFunc(next.Voters, func(id NodeID) bool { return id == cc.Node })
		case cs.IsVoter(cc.Node):
			return cs, fmt.Errorf("%w: %s is the last voter", ErrConfChangeInvalid, cc.Node)
		default:
			return cs, fmt.Errorf("%w: %s is not a member", ErrConfChangeInvalid, cc.Node)
		}
		delete(next.Addrs, cc.Node)
	default:
		return cs, fmt.Errorf("%w: unknown type %d", ErrConfChangeInvalid, cc.Type)
	}
	slices.Sort(next.Voters)
	slices.Sort(next.Learners)
	return next, nil
}

func AppendConfState(b []byte, cs ConfState) []byte {
	b = appendConfIDs(b, cs.Voters)
	b = appendConfIDs(b, cs.Learners)
	ids := slices.Sorted(maps.Keys(cs.Addrs))
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {
		b = appendConfString(b, string(id))
		b = appendConfString(b, cs.Addrs[id])
	}
	return b
}

func DecodeConfState(b []byte) (ConfState, []byte, error) {
	d := confDecoder{b: b}
	cs := ConfState{Voters: d.ids(), Learners: d.ids()}
	if n := d.count(); n > 0 {
		cs.Addrs = make(map[NodeID]string, n)
		for range n {
			id := NodeID(d.str())
			cs.Addrs[id] = d.str()
		}
	}
	if d.err != nil {
		return ConfState{}, nil, d.err
	}
	return cs, d.b, nil
}

func appendConfString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendConfIDs(b []byte, ids []NodeID) []byte {
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {
		b = appendConfString(b, string(id))
	}
	return b
}

var errConfDecode = errors.New("raft: malformed configuration")

type confDecoder struct {
	b   []byte
	err error
}

func (d *confDecoder) count() int {
	n, k := binary.Uvarint(d.b)
	if k <= 0 || n > uint64(len(d.b)) {
		d.err, d.b = errConfDecode, nil
		return 0
	}
	d.b = d.b[k:]
	return int(n)
}

func (d *confDecoder) str() string {
	n := d.count()
	if n > len(d.b) {
		d.err, d.b = errConfDecode, nil
		return ""
	}
	s := string(d.b[:n])
	d.b = d.b[n:]
	return s
}

func (d *confDecoder) ids() []NodeID {
	n := d.count()
	var out []NodeID
	for range n {
		out = append(out, NodeID(d.str()))
	}
	return out
}

func (c *Core) ProposeConfChange(cc ConfChange) (index, term uint64, err error) {
	if !c.leaderReady() || !c.trk.isVoter(c.id) || c.leadTransferee != None {
		return 0, 0, ErrNotLeader
	}
	if c.pendingConf > c.log.applied {
		return 0, 0, ErrConfChangePending
	}
	next, err := c.conf.Apply(cc)
	if err != nil {
		return 0, 0, err
	}
	index = c.appendEntry(Entry{Type: EntryConfChange, Data: AppendConfState(nil, next)})
	c.pendingConf = index
	return index, c.term, nil
}

func (c *Core) ConfState() ConfState { return c.conf.Clone() }

func (c *Core) applyConf(cs ConfState) {
	c.conf = cs.Clone()
	c.trk.update(cs, c.maxInflight, c.maxInflightBytes, c.log.lastIndex())
	if c.state != StateLeader {
		return
	}
	if !c.trk.isVoter(c.id) {
		c.becomeFollower(c.term, None)
		return
	}
	if c.leadTransferee != None && !c.trk.isVoter(c.leadTransferee) {
		c.leadTransferee = None
	}
	c.maybeCommit()
	c.confirmReads()
	c.sendPending = true
}

func (c *Core) Match(id NodeID) (uint64, bool) {
	if c.state != StateLeader {
		return 0, false
	}
	pr := c.trk.progress[id]
	if pr == nil {
		return 0, false
	}
	return pr.Match, true
}
