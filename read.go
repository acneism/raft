package raft

type readRound struct {
	id     uint64
	index  uint64
	local  bool
	remote []remoteRead
}

type remoteRead struct {
	to NodeID
	id uint64
}

func (c *Core) committedInTerm() bool {
	return c.state == StateLeader && c.noopIndex > 0 && c.log.committed >= c.noopIndex
}

func (c *Core) ReadIndex() (uint64, error) {
	switch {
	case c.state == StateLeader:
		if !c.committedInTerm() {
			return 0, ErrNotLeader
		}
		r := c.openRead()
		r.local = true
		id := r.id
		c.confirmReads()
		return id, nil
	case c.lead != None:
		if !c.readOpen || len(c.forwarded) == 0 {
			c.readSeq++
			c.readOpen = true
			c.forwarded = append(c.forwarded, c.readSeq)
			c.send(Message{To: c.lead, Type: MsgReadIndex, Index: c.readSeq})
		}
		return c.forwarded[len(c.forwarded)-1], nil
	}
	return 0, ErrNotLeader
}

func (c *Core) openRead() *readRound {
	if c.readOpen && len(c.reads) > 0 {
		r := &c.reads[len(c.reads)-1]
		r.index = c.log.committed
		return r
	}
	c.readSeq++
	c.readOpen = true
	c.reads = append(c.reads, readRound{id: c.readSeq, index: c.log.committed})
	c.bcastHeartbeat()
	return &c.reads[len(c.reads)-1]
}

func (c *Core) confirmReads() {
	if len(c.reads) == 0 {
		return
	}
	upTo := c.trk.readConfirmed(c.id)
	n := 0
	for ; n < len(c.reads) && c.reads[n].id <= upTo; n++ {
		r := &c.reads[n]
		if r.local {
			c.readStates = append(c.readStates, ReadState{ID: r.id, Index: r.index})
		}
		for _, rr := range r.remote {
			c.send(Message{To: rr.to, Type: MsgReadIndexResp, Index: rr.id, Commit: r.index})
		}
	}
	c.reads = c.reads[:copy(c.reads, c.reads[n:])]
}

func (c *Core) handleReadIndex(m Message) {
	if !c.committedInTerm() {
		c.send(Message{To: m.From, Type: MsgReadIndexResp, Index: m.Index, Reject: true})
		return
	}
	r := c.openRead()
	r.remote = append(r.remote, remoteRead{to: m.From, id: m.Index})
	c.confirmReads()
}

func (c *Core) handleReadIndexResp(m Message) {
	for i, id := range c.forwarded {
		if id == m.Index {
			c.forwarded = append(c.forwarded[:i], c.forwarded[i+1:]...)
			c.readStates = append(c.readStates, ReadState{ID: id, Index: m.Commit, Failed: m.Reject})
			return
		}
	}
}

func (c *Core) failReads() {
	for _, r := range c.reads {
		if r.local {
			c.readStates = append(c.readStates, ReadState{ID: r.id, Failed: true})
		}
		for _, rr := range r.remote {
			c.send(Message{To: rr.to, Type: MsgReadIndexResp, Index: rr.id, Reject: true})
		}
	}
	for _, id := range c.forwarded {
		c.readStates = append(c.readStates, ReadState{ID: id, Failed: true})
	}
	c.reads, c.forwarded, c.readOpen = c.reads[:0], c.forwarded[:0], false
}
