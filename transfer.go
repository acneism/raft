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
	c.leadTransferee, c.transferElapsed = to, 0
	if pr := c.trk.progress[to]; pr.Match == c.log.lastIndex() {
		c.send(Message{To: to, Type: MsgTimeoutNow})
		c.transferTried = true
	} else {
		pr.forceSend = true
		c.sendPending = true
	}
	return nil
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
