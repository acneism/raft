package raft

import "errors"

var ErrTransferTarget = errors.New("raft: leadership transfer target is not a voter")

func (c *Core) TransferLeadership(to NodeID) error {
	if c.state != StateLeader {
		return ErrNotLeader
	}
	if to == c.id {
		c.leadTransferee = None
		return nil
	}
	if !c.trk.isVoter(to) {
		return ErrTransferTarget
	}
	c.leadTransferee, c.transferElapsed, c.transferSent = to, 0, false
	if pr := c.trk.progress[to]; pr.Match == c.log.lastIndex() {
		c.sendTimeoutNow()
	} else {
		pr.forceSend = true
		c.sendPending = true
	}
	return nil
}

func (c *Core) sendTimeoutNow() {
	c.send(Message{To: c.leadTransferee, Type: MsgTimeoutNow})
	if !c.transferSent {
		c.transferSent, c.transferElapsed = true, 0
	}
	c.transferTried = true
}

func (c *Core) campaignTransfer() {
	c.becomeCandidate()
	c.transferVote = true
	if c.trk.recordVote(c.id, true) == voteWon {
		c.becomeLeader()
		return
	}
	c.requestVotes(false)
}
