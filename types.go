package raft

import "errors"

type NodeID string

const None NodeID = ""

var (
	ErrNotLeader                      = errors.New("raft: not leader")
	ErrCompacted                      = errors.New("raft: requested index is compacted")
	ErrUnavailable                    = errors.New("raft: requested entry is unavailable")
	ErrSnapshotTemporarilyUnavailable = errors.New("raft: snapshot is temporarily unavailable")
)

type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryNoop
	EntryConfChange
)

type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

const entryOverhead = 24

func entsSize(ents []Entry) uint64 {
	var n uint64
	for i := range ents {
		n += entryOverhead + uint64(len(ents[i].Data))
	}
	return n
}

func limitSize(ents []Entry, maxSize uint64) []Entry {
	if len(ents) == 0 {
		return ents
	}
	size := entryOverhead + uint64(len(ents[0].Data))
	n := 1
	for ; n < len(ents); n++ {
		size += entryOverhead + uint64(len(ents[n].Data))
		if size > maxSize {
			break
		}
	}
	return ents[:n:n]
}

type HardState struct {
	Term   uint64
	Vote   NodeID
	Commit uint64
}

func (h HardState) IsEmpty() bool { return h == HardState{} }

type ConfState struct {
	Voters   []NodeID
	Learners []NodeID
}

type SnapshotMeta struct {
	Index uint64
	Term  uint64
	Conf  ConfState
}

type StateType uint8

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
)

func (s StateType) String() string {
	switch s {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	case StatePreCandidate:
		return "PreCandidate"
	}
	return "Unknown"
}

type SoftState struct {
	Lead  NodeID
	State StateType
}

type MessageType uint8

const (
	MsgApp MessageType = iota + 1
	MsgAppResp
	MsgVote
	MsgVoteResp
	MsgPreVote
	MsgPreVoteResp
	MsgHeartbeat
	MsgHeartbeatResp
	MsgSnap
	MsgReadIndex
	MsgReadIndexResp
	MsgTimeoutNow
)

var msgNames = [...]string{"", "MsgApp", "MsgAppResp", "MsgVote", "MsgVoteResp", "MsgPreVote", "MsgPreVoteResp", "MsgHeartbeat", "MsgHeartbeatResp", "MsgSnap", "MsgReadIndex", "MsgReadIndexResp", "MsgTimeoutNow"}

func (t MessageType) String() string {
	if int(t) < len(msgNames) && t != 0 {
		return msgNames[t]
	}
	return "MsgUnknown"
}

type Message struct {
	Type       MessageType
	From       NodeID
	To         NodeID
	Term       uint64
	Index      uint64
	LogTerm    uint64
	Commit     uint64
	Entries    []Entry
	Snapshot   *SnapshotMeta
	Reject     bool
	RejectHint uint64
	Transfer   bool
}

type Ready struct {
	SoftState            *SoftState
	HardState            HardState
	Entries              []Entry
	Snapshot             *SnapshotMeta
	Committed            []Entry
	Messages             []Message
	MessagesAfterPersist []Message
	ReadStates           []ReadState
	MustSync             bool
}

type ReadState struct {
	ID     uint64
	Index  uint64
	Failed bool
}
