package node

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/acneism/raft"
	"github.com/acneism/raft/transport"
	"github.com/acneism/raft/wal"
)

var (
	ErrNotLeader      = raft.ErrNotLeader
	ErrLost           = errors.New("node: proposal lost")
	ErrUnknown        = errors.New("node: proposal outcome unknown")
	ErrClosed         = errors.New("node: closed")
	ErrTransferFailed = errors.New("node: leadership transfer failed")
	ErrRemoved        = errors.New("node: removed from the cluster")

	ErrTransferBehind    = fmt.Errorf("%w: the target did not catch up within an election timeout", ErrTransferFailed)
	ErrTransferTimeout   = fmt.Errorf("%w: the target did not win an election within an election timeout", ErrTransferFailed)
	ErrTransferPreempted = fmt.Errorf("%w: another node won the election", ErrTransferFailed)
)

type Proposal struct {
	Index uint64
	Term  uint64
}

type Event struct {
	Term   uint64
	Leader raft.NodeID
	Ready  bool
}

type SnapshotSource struct {
	Meta raft.SnapshotMeta
	Dir  string
}

type StateMachine interface {
	Apply(entries []raft.Entry) error
	DurableIndex() uint64
	Snapshot(dir string) (raft.SnapshotMeta, error)
	Restore(src SnapshotSource) error
}

type Config struct {
	ID              raft.NodeID
	ClusterID       string
	Dir             string
	Listen          string
	Peers           map[raft.NodeID]string
	Join            bool
	TLS             *tls.Config
	StateMachine    StateMachine
	TickInterval    time.Duration
	ElectionTicks   int
	HeartbeatTicks  int
	PreVote         bool
	CheckQuorum     bool
	LeaseReads      bool
	MaxClockDrift   float64
	SegmentSize     int64
	NoSync          bool
	MaxSizePerMsg   uint64
	MaxInflightMsgs int
	CompactEntries  uint64
	TrailingEntries uint64
	KeepSnapshots   int
	SerialPersist   bool
	Logger          *slog.Logger
}

type Node struct {
	cfg      Config
	id       raft.NodeID
	conf     raft.ConfState
	log      *wal.Log
	tr       *transport.Transport
	fsm      StateMachine
	snapRoot string
	logger   *slog.Logger

	mu         sync.Mutex
	core       *raft.Core
	obs        Event
	reads      map[uint64][]chan readResult
	changed    chan struct{}
	tickStart  time.Time
	ticks      int
	transferee raft.NodeID

	recvc      chan raft.Message
	notifyc    chan struct{}
	stopc      chan struct{}
	persistedc chan []raft.Ready
	donec      chan []raft.Ready
	pq         queue[raft.Ready]
	aq         queue[applyJob]
	wg         sync.WaitGroup
	closeOnce  sync.Once

	repMu   sync.Mutex
	reports []report

	wmu        sync.Mutex
	waiters    map[uint64][]waiter
	applied    uint64
	appliedCh  chan struct{}
	leaderTerm uint64
	closed     bool
	err        error

	events     eventQueue
	eventOut   chan Event
	snapWanted atomic.Bool
	peerAddrs  map[raft.NodeID]string
	removed    map[raft.NodeID]bool
	member     bool
	removedc   chan raft.NodeID
}

type report struct {
	id       raft.NodeID
	snapshot bool
	failed   bool
}

func HasState(dir string) (bool, error) { return wal.Exists(filepath.Join(dir, "wal")) }

func Open(cfg Config) (*Node, error) {
	if cfg.ID == raft.None || cfg.Dir == "" || cfg.StateMachine == nil {
		return nil, errors.New("node: ID, Dir and StateMachine are required")
	}
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, errors.New("node: Peers must include the node itself")
	}
	if cfg.Listen == "" {
		cfg.Listen = cfg.Peers[cfg.ID]
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 10 * time.Millisecond
	}
	if cfg.ElectionTicks <= 0 {
		cfg.ElectionTicks = 100
	}
	if cfg.HeartbeatTicks <= 0 {
		cfg.HeartbeatTicks = 10
	}
	if cfg.CompactEntries == 0 {
		cfg.CompactEntries = 8192
	}
	if cfg.TrailingEntries == 0 {
		cfg.TrailingEntries = 1024
	}
	if cfg.KeepSnapshots <= 0 {
		cfg.KeepSnapshots = 2
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	var boot raft.ConfState
	if !cfg.Join {
		for id := range cfg.Peers {
			boot.Voters = append(boot.Voters, id)
		}
		slices.Sort(boot.Voters)
		boot.Addrs = maps.Clone(cfg.Peers)
	}
	n := &Node{
		cfg:        cfg,
		id:         cfg.ID,
		fsm:        cfg.StateMachine,
		snapRoot:   filepath.Join(cfg.Dir, "snap"),
		logger:     cfg.Logger.With("node", cfg.ID),
		recvc:      make(chan raft.Message, 4096),
		notifyc:    make(chan struct{}, 1),
		stopc:      make(chan struct{}),
		persistedc: make(chan []raft.Ready, 64),
		donec:      make(chan []raft.Ready, 64),
		waiters:    map[uint64][]waiter{},
		appliedCh:  make(chan struct{}),
		eventOut:   make(chan Event, 64),
		reads:      map[uint64][]chan readResult{},
		changed:    make(chan struct{}),
		removed:    map[raft.NodeID]bool{},
		removedc:   make(chan raft.NodeID, 1),
	}
	n.pq.init()
	n.aq.init()
	n.events.notify = make(chan struct{}, 1)
	if err := os.MkdirAll(n.snapRoot, 0o700); err != nil {
		return nil, err
	}
	log, err := wal.Open(filepath.Join(cfg.Dir, "wal"), boot, wal.Options{SegmentSize: cfg.SegmentSize, NoSync: cfg.NoSync})
	if err != nil {
		return nil, err
	}
	n.log = log
	if err := n.start(); err != nil {
		log.Close()
		return nil, err
	}
	return n, nil
}

func (n *Node) start() error {
	if err := n.finishRestore(); err != nil {
		return err
	}
	snap, _ := n.log.Snapshot()
	first, _ := n.log.FirstIndex()
	applied := n.fsm.DurableIndex()
	if applied+1 < first {
		if snap.Index+1 < first {
			return fmt.Errorf("node: state machine is at %d, the log starts at %d and no snapshot covers the gap", applied, first)
		}
		if err := n.fsm.Restore(SnapshotSource{Meta: snap, Dir: n.snapDir(snap)}); err != nil {
			return fmt.Errorf("node: restore snapshot %d: %w", snap.Index, err)
		}
		applied = snap.Index
	}
	lease := 0
	if n.cfg.LeaseReads {
		drift := n.cfg.MaxClockDrift
		if drift <= 0 {
			drift = 0.1
		}
		if lease = int(float64(n.cfg.ElectionTicks-2) / (1 + drift)); !n.cfg.CheckQuorum || lease < 1 {
			return errors.New("node: LeaseReads needs CheckQuorum and at least 4 ElectionTicks")
		}
	}
	core, err := raft.New(raft.Config{
		ID:              n.id,
		ElectionTick:    n.cfg.ElectionTicks,
		HeartbeatTick:   n.cfg.HeartbeatTicks,
		Storage:         storage{n.log, n},
		Applied:         applied,
		MaxSizePerMsg:   n.cfg.MaxSizePerMsg,
		MaxInflightMsgs: n.cfg.MaxInflightMsgs,
		PreVote:         n.cfg.PreVote,
		CheckQuorum:     n.cfg.CheckQuorum,
		LeaseTicks:      lease,
	})
	if err != nil {
		return err
	}
	n.core = core
	n.applied = applied
	_, conf, _ := n.log.InitialState()
	n.conf = conf
	n.member = conf.IsVoter(n.id) || conf.IsLearner(n.id)
	addrs := n.cfg.Peers
	if len(conf.Addrs) > 0 {
		addrs = conf.Addrs
	}
	peers := map[raft.NodeID]string{}
	for id, addr := range addrs {
		if id != n.id {
			peers[id] = addr
		}
	}
	n.peerAddrs = maps.Clone(peers)
	tr, err := transport.New(transport.Config{
		ID:          n.id,
		ClusterID:   n.cfg.ClusterID,
		Listen:      n.cfg.Listen,
		Advertise:   addrs[n.id],
		Peers:       peers,
		TLS:         n.cfg.TLS,
		Handler:     n,
		SnapshotDir: n.snapRoot,
		UnknownPeer: n.admit,
		IsRemoved:   n.isRemoved,
		OnRemoved:   n.onRemoved,
		Logger:      n.logger,
	})
	if err != nil {
		return err
	}
	n.wmu.Lock()
	n.tr = tr
	n.wmu.Unlock()
	n.tickStart = time.Now()
	n.wg.Add(4)
	go n.run()
	go n.persister()
	go n.applier()
	go n.pumpEvents()
	return nil
}

func (n *Node) Addr() string { return n.tr.Addr().String() }

func (n *Node) Propose(data []byte) (Proposal, error) {
	n.mu.Lock()
	idx, term, err := n.core.Propose(data)
	n.mu.Unlock()
	if err != nil {
		return Proposal{}, err
	}
	n.wake()
	return Proposal{Index: idx, Term: term}, nil
}

func (n *Node) ProposeIn(term uint64, data []byte) (Proposal, error) {
	n.mu.Lock()
	if n.core.Status().Term != term {
		n.mu.Unlock()
		return Proposal{}, ErrNotLeader
	}
	idx, t, err := n.core.Propose(data)
	n.mu.Unlock()
	if err != nil {
		return Proposal{}, err
	}
	n.wake()
	return Proposal{Index: idx, Term: t}, nil
}

type Status struct {
	raft.Status
	FirstIndex uint64
	Snapshot   raft.SnapshotMeta
	Progress   map[raft.NodeID]raft.PeerProgress
}

func (n *Node) Status() Status {
	n.mu.Lock()
	st := Status{Status: n.core.Status(), Progress: n.core.Progress()}
	n.mu.Unlock()
	st.FirstIndex, _ = n.log.FirstIndex()
	st.Snapshot, _ = n.log.Snapshot()
	st.Snapshot.Conf = st.Snapshot.Conf.Clone()
	return st
}

func (n *Node) Events() <-chan Event { return n.eventOut }

func (n *Node) Err() error {
	n.wmu.Lock()
	defer n.wmu.Unlock()
	return n.err
}

func (n *Node) wake() {
	select {
	case n.notifyc <- struct{}{}:
	default:
	}
}

func (n *Node) Receive(m raft.Message) {
	select {
	case n.recvc <- m:
	default:
	}
}

func (n *Node) ReceiveSnapshot(m raft.Message, dir string) { n.Receive(m) }

func (n *Node) ReportUnreachable(id raft.NodeID) { n.addReport(report{id: id}) }

func (n *Node) ReportSnapshot(id raft.NodeID, failed bool) {
	n.addReport(report{id: id, snapshot: true, failed: failed})
}

func (n *Node) addReport(r report) {
	n.repMu.Lock()
	n.reports = append(n.reports, r)
	n.repMu.Unlock()
	n.wake()
}

func (n *Node) fail(err error) {
	n.wmu.Lock()
	if n.err == nil {
		n.err = err
		n.logger.Error("stopping after a fatal error", "err", err)
	}
	n.wmu.Unlock()
	go n.Close()
}

func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		close(n.stopc)
		n.tr.Close()
		n.pq.close()
		n.aq.close()
		n.wg.Wait()
		n.log.Close()
		n.wmu.Lock()
		n.closed = true
		for _, ws := range n.waiters {
			for _, w := range ws {
				w.ch <- ErrClosed
			}
		}
		n.waiters = nil
		close(n.appliedCh)
		n.wmu.Unlock()
	})
	return n.Err()
}
