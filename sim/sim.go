package sim

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/acneism/raft"
)

type Bug uint8

const (
	NoBug Bug = iota
	BugSendBeforePersist
	BugForgetVote
)

type Options struct {
	Seed        uint64
	Nodes       int
	PreVote     bool
	CheckQuorum bool
	Bug         Bug
}

const (
	us            = 1
	ms            = 1000 * us
	second        = 1000 * ms
	tickInterval  = 10 * ms
	electionTick  = 10
	heartbeatTick = 1
	calmDuration  = 3 * second
)

type eventKind uint8

const (
	evTick eventKind = iota
	evDeliver
	evSync
	evPropose
	evFault
	evRestart
	evFSMSync
	evCompact
	evCalmStart
	evCalmEnd
)

var eventNames = [...]string{"tick", "deliver", "sync", "propose", "fault", "restart", "fsm-sync", "compact", "calm-start", "calm-end"}

type event struct {
	at   int64
	seq  uint64
	kind eventKind
	node *node
	gen  uint64
	env  *envelope
}

type eventHeap []*event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(*event)) }
func (h *eventHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return e
}

type envelope struct {
	m    raft.Message
	gen  uint64
	snap fsmState
}

type fsmState struct {
	index, hash uint64
}

type node struct {
	id       raft.NodeID
	up       bool
	gen      uint64
	core     *raft.Core
	disk     *raft.MemoryStorage
	diskFull bool
	slowDisk bool
	skew     float64

	queue      []raft.Ready
	batch      []raft.Ready
	syncing    bool
	processing bool

	fsm      fsmState
	fsmDur   fsmState
	snap     fsmState
	recvSnap map[uint64]fsmState

	view      logView
	commit    uint64
	leaderIn  uint64
	lastReady uint64
}

type netState struct {
	blocked map[[2]int]bool
	drop    float64
	dup     float64
	slow    bool
}

type Sim struct {
	opts   Options
	rng    *rand.Rand
	now    int64
	seq    uint64
	events eventHeap
	nodes  []*node
	byID   map[raft.NodeID]*node
	index  map[*node]int
	conf   raft.ConfState
	net    netState
	chk    checker
	steps  int64
	err    error
	trace  trace

	proposals uint64
	calm      bool
	calmFrom  uint64
	calmOK    bool
	calms     int
}

func New(opts Options) *Sim {
	if opts.Nodes == 0 {
		opts.Nodes = 3
	}
	s := &Sim{
		opts:  opts,
		rng:   rand.New(rand.NewPCG(opts.Seed, 0x5eed)),
		byID:  map[raft.NodeID]*node{},
		index: map[*node]int{},
		net:   netState{blocked: map[[2]int]bool{}},
	}
	s.chk = newChecker(s)
	for i := range opts.Nodes {
		s.conf.Voters = append(s.conf.Voters, raft.NodeID(fmt.Sprintf("n%d", i+1)))
	}
	for i, id := range s.conf.Voters {
		n := &node{
			id:       id,
			disk:     raft.NewMemoryStorage(s.conf),
			skew:     0.8 + 0.4*s.rng.Float64(),
			recvSnap: map[uint64]fsmState{},
		}
		s.nodes = append(s.nodes, n)
		s.byID[id] = n
		s.index[n] = i
	}
	for _, n := range s.nodes {
		s.restart(n)
	}
	s.schedule(s.now+ms, evPropose, nil)
	s.schedule(s.now+100*ms, evFault, nil)
	s.schedule(s.now+s.between(5*second, 10*second), evCalmStart, nil)
	return s
}

func (s *Sim) Steps() int64 { return s.steps }

func (s *Sim) Calms() int { return s.calms }

func (s *Sim) Run(steps int64) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = s.failure(fmt.Sprintf("panic: %v", r))
		}
	}()
	for s.steps < steps && s.err == nil {
		ev := heap.Pop(&s.events).(*event)
		s.now = ev.at
		s.steps++
		s.trace.add(s, ev)
		s.handle(ev)
		if s.steps&(1<<16-1) == 0 {
			s.chk.prune()
		}
	}
	return s.err
}

func (s *Sim) fail(format string, args ...any) {
	if s.err == nil {
		s.err = s.failure(fmt.Sprintf(format, args...))
	}
}

func (s *Sim) failure(msg string) error {
	return fmt.Errorf("seed %d, step %d, t=%.3fs: %s\nlast events:\n%s", s.opts.Seed, s.steps, float64(s.now)/second, msg, s.trace.String())
}

func (s *Sim) schedule(at int64, kind eventKind, n *node) *event {
	ev := &event{at: at, seq: s.rng.Uint64(), kind: kind, node: n}
	if n != nil {
		ev.gen = n.gen
	}
	heap.Push(&s.events, ev)
	return ev
}

func (s *Sim) between(lo, hi int64) int64 { return lo + s.rng.Int64N(hi-lo) }

func (s *Sim) live(ev *event) bool { return ev.node.up && ev.gen == ev.node.gen }

func (s *Sim) handle(ev *event) {
	n := ev.node
	switch ev.kind {
	case evTick:
		if !s.live(ev) {
			return
		}
		n.core.Tick()
		s.process(n)
		jitter := 0.9 + 0.2*s.rng.Float64()
		s.schedule(s.now+int64(float64(tickInterval)*n.skew*jitter), evTick, n)
	case evDeliver:
		s.deliver(ev.env)
	case evSync:
		if s.live(ev) {
			s.sync(n)
		}
	case evPropose:
		s.propose()
		s.schedule(s.now+s.between(100*us, 2*ms), evPropose, nil)
	case evFault:
		if !s.calm {
			s.fault()
		}
		s.schedule(s.now+s.between(10*ms, 200*ms), evFault, nil)
	case evRestart:
		if !n.up && ev.gen == n.gen && !n.diskFull {
			s.restart(n)
		}
	case evFSMSync:
		if s.live(ev) {
			n.fsmDur = n.fsm
			s.schedule(s.now+s.between(5*ms, 50*ms), evFSMSync, n)
		}
	case evCompact:
		if s.live(ev) {
			s.compact(n)
			s.schedule(s.now+s.between(10*ms, 100*ms), evCompact, n)
		}
	case evCalmStart:
		s.startCalm()
		s.schedule(s.now+calmDuration, evCalmEnd, nil)
	case evCalmEnd:
		s.endCalm()
		s.schedule(s.now+s.between(5*second, 10*second), evCalmStart, nil)
	}
}

func (s *Sim) restart(n *node) {
	snap, _ := n.disk.Snapshot()
	if n.fsmDur.index < snap.Index {
		n.fsmDur = n.snap
	}
	n.fsm = n.fsmDur
	s.chk.restarted(n)
	n.gen++
	core, err := raft.New(raft.Config{
		ID:                       n.id,
		ElectionTick:             electionTick,
		HeartbeatTick:            heartbeatTick,
		Storage:                  n.disk,
		Applied:                  n.fsm.index,
		MaxSizePerMsg:            uint64(64 + s.rng.IntN(512)),
		MaxInflightMsgs:          1 + s.rng.IntN(8),
		MaxCommittedSizePerReady: uint64(64 + s.rng.IntN(512)),
		PreVote:                  s.opts.PreVote,
		CheckQuorum:              s.opts.CheckQuorum,
		Rand:                     rand.New(rand.NewPCG(s.rng.Uint64(), s.rng.Uint64())),
	})
	if err != nil {
		s.fail("restart %s: %v", n.id, err)
		return
	}
	n.core = core
	n.up = true
	n.view.load(n.disk)
	n.commit = core.Status().Commit
	n.leaderIn, n.lastReady = 0, 0
	s.schedule(s.now+int64(float64(tickInterval)*n.skew*s.rng.Float64()), evTick, n)
	s.schedule(s.now+s.between(5*ms, 50*ms), evFSMSync, n)
	s.schedule(s.now+s.between(10*ms, 100*ms), evCompact, n)
	s.process(n)
}

func (s *Sim) crash(n *node, torn bool) {
	if torn && n.syncing {
		s.tornWrite(n)
	}
	n.up = false
	n.gen++
	n.core = nil
	n.queue, n.batch, n.syncing = nil, nil, false
	n.fsm = n.fsmDur
	n.recvSnap = map[uint64]fsmState{}
	s.schedule(s.now+s.between(10*ms, second), evRestart, n)
}

func (s *Sim) tornWrite(n *node) {
	var ops []func()
	var hss []raft.HardState
	for _, rd := range n.batch {
		if rd.Snapshot != nil {
			snap := *rd.Snapshot
			ops = append(ops, func() { s.installSnapshot(n, snap) })
		}
		for _, e := range rd.Entries {
			ops = append(ops, func() { n.disk.Append([]raft.Entry{e}) })
		}
		if !rd.HardState.IsEmpty() && s.opts.Bug != BugForgetVote {
			hss = append(hss, rd.HardState)
		}
	}
	for _, op := range ops[:s.rng.IntN(len(ops)+1)] {
		op()
	}
	if j := s.rng.IntN(len(hss)+1) - 1; j >= 0 {
		n.disk.SetHardState(hss[j])
	}
}

func (s *Sim) process(n *node) {
	if n.processing {
		return
	}
	n.processing = true
	for n.up && n.core.HasReady() {
		rd := n.core.Ready()
		s.chk.ready(n, rd)
		n.queue = append(n.queue, rd)
		for _, m := range rd.Messages {
			s.send(n, m)
		}
		if s.opts.Bug == BugSendBeforePersist {
			for _, m := range rd.MessagesAfterPersist {
				s.send(n, m)
			}
		}
	}
	n.processing = false
	if !n.up {
		return
	}
	s.startSync(n)
	s.chk.status(n)
}

func (s *Sim) startSync(n *node) {
	if n.syncing || len(n.queue) == 0 {
		return
	}
	k := 0
	for k < len(n.queue) && n.queue[k].Snapshot == nil {
		k++
	}
	if k == 0 {
		k = 1
	}
	n.batch = slices.Clone(n.queue[:k])
	n.queue = slices.Clone(n.queue[k:])
	n.syncing = true
	d := s.between(100*us, 3*ms)
	if n.slowDisk {
		d += s.between(50*ms, 500*ms)
	} else if s.rng.IntN(100) == 0 {
		d += s.between(10*ms, 100*ms)
	}
	s.schedule(s.now+d, evSync, n)
}

func (s *Sim) sync(n *node) {
	if n.diskFull {
		s.crash(n, true)
		return
	}
	for _, rd := range n.batch {
		if rd.Snapshot != nil {
			s.installSnapshot(n, *rd.Snapshot)
		}
		if err := n.disk.Append(rd.Entries); err != nil {
			s.fail("%s: append: %v", n.id, err)
			return
		}
		if !rd.HardState.IsEmpty() && s.opts.Bug != BugForgetVote {
			n.disk.SetHardState(rd.HardState)
		}
	}
	if !s.calm && s.rng.IntN(2000) == 0 {
		s.crash(n, false)
		return
	}
	batch := n.batch
	n.batch, n.syncing = nil, false
	for _, rd := range batch {
		if s.opts.Bug != BugSendBeforePersist {
			for _, m := range rd.MessagesAfterPersist {
				s.send(n, m)
			}
		}
		s.apply(n, rd.Committed)
		n.core.Advance(rd)
	}
	s.process(n)
}

func (s *Sim) installSnapshot(n *node, snap raft.SnapshotMeta) {
	st, ok := n.recvSnap[snap.Index]
	if !ok {
		s.fail("%s: installing snapshot %d that was never received", n.id, snap.Index)
		return
	}
	if err := n.disk.ApplySnapshot(snap); err != nil {
		s.fail("%s: %v", n.id, err)
		return
	}
	s.chk.restored(n, st)
	n.snap, n.fsm, n.fsmDur = st, st, st
}

func (s *Sim) apply(n *node, ents []raft.Entry) {
	for _, e := range ents {
		s.chk.apply(n, e)
		n.fsm = fsmState{index: e.Index, hash: mix(n.fsm.hash, e)}
		if s.calm && e.Type == raft.EntryNormal && binary.BigEndian.Uint64(e.Data) >= s.calmFrom {
			s.calmOK = true
		}
	}
}

func (s *Sim) compact(n *node) {
	snap, _ := n.disk.Snapshot()
	if d := n.fsmDur; d.index > snap.Index {
		if _, err := n.disk.CreateSnapshot(d.index, s.conf); err != nil {
			s.fail("%s: create snapshot: %v", n.id, err)
			return
		}
		n.snap = d
		snap.Index = d.index
	}
	first, _ := n.disk.FirstIndex()
	to := snap.Index - min(snap.Index, uint64(s.rng.IntN(20)))
	if to >= first {
		if err := n.disk.Compact(to); err != nil {
			s.fail("%s: compact %d: %v", n.id, to, err)
			return
		}
		n.view.compact(to)
	}
}

func (s *Sim) propose() {
	n := s.nodes[s.rng.IntN(len(s.nodes))]
	if !n.up {
		return
	}
	data := binary.BigEndian.AppendUint64(nil, s.proposals)
	if _, _, err := n.core.Propose(data); err == nil {
		s.proposals++
		s.process(n)
	}
}

func (s *Sim) blocked(from, to *node) bool {
	return s.net.blocked[[2]int{s.index[from], s.index[to]}]
}

func (s *Sim) latency() int64 {
	d := s.between(50*us, 2*ms)
	if s.net.slow && s.rng.IntN(5) == 0 {
		d += s.between(ms, 50*ms)
	}
	return d
}

func (s *Sim) send(from *node, m raft.Message) {
	env := &envelope{m: m, gen: from.gen}
	if m.Type == raft.MsgSnap {
		switch i := m.Snapshot.Index; {
		case from.snap.index == i:
			env.snap = from.snap
		default:
			st, ok := from.recvSnap[i]
			if !ok {
				s.fail("%s sends snapshot %d it does not have", from.id, i)
				return
			}
			env.snap = st
		}
	}
	to := s.byID[m.To]
	if s.blocked(from, to) || s.rng.Float64() < s.net.drop {
		s.lost(env)
		return
	}
	s.schedule(s.now+s.latency(), evDeliver, nil).env = env
	if s.rng.Float64() < s.net.dup {
		s.schedule(s.now+s.latency(), evDeliver, nil).env = env
	}
}

func (s *Sim) deliver(env *envelope) {
	m := env.m
	from, to := s.byID[m.From], s.byID[m.To]
	if !to.up || s.blocked(from, to) {
		s.lost(env)
		return
	}
	if m.Type == raft.MsgSnap {
		to.recvSnap[m.Snapshot.Index] = env.snap
	}
	to.core.Step(m)
	s.process(to)
	if m.Type == raft.MsgSnap && from.up && from.gen == env.gen {
		from.core.ReportSnapshot(m.To, false)
		s.process(from)
	}
}

func (s *Sim) lost(env *envelope) {
	from := s.byID[env.m.From]
	if !from.up || from.gen != env.gen {
		return
	}
	switch env.m.Type {
	case raft.MsgSnap:
		from.core.ReportSnapshot(env.m.To, true)
	case raft.MsgApp:
		if s.rng.IntN(4) != 0 {
			return
		}
		from.core.ReportUnreachable(env.m.To)
	default:
		return
	}
	s.process(from)
}

func (s *Sim) fault() {
	n := s.nodes[s.rng.IntN(len(s.nodes))]
	switch s.rng.IntN(13) {
	case 0, 1:
		if n.up {
			s.crash(n, true)
		}
	case 2:
		if !n.up && !n.diskFull {
			s.restart(n)
		}
	case 3:
		clear(s.net.blocked)
		side := make([]bool, len(s.nodes))
		for i := range side {
			side[i] = s.rng.IntN(2) == 0
		}
		for i := range s.nodes {
			for j := range s.nodes {
				if side[i] != side[j] {
					s.net.blocked[[2]int{i, j}] = true
				}
			}
		}
	case 4:
		for range 1 + s.rng.IntN(3) {
			i, j := s.rng.IntN(len(s.nodes)), s.rng.IntN(len(s.nodes))
			if i != j {
				s.net.blocked[[2]int{i, j}] = true
			}
		}
	case 5, 6:
		clear(s.net.blocked)
	case 7:
		s.net.drop = []float64{0, 0, 0.01, 0.1, 0.3}[s.rng.IntN(5)]
		s.net.dup = []float64{0, 0, 0.01, 0.05}[s.rng.IntN(4)]
	case 8:
		s.net.slow = !s.net.slow
	case 9:
		n.diskFull = true
	case 10:
		if n.diskFull {
			n.diskFull = false
			if !n.up {
				s.schedule(s.now+s.between(ms, 100*ms), evRestart, n)
			}
		}
	case 11:
		n.skew = 0.5 + 1.5*s.rng.Float64()
	case 12:
		n.slowDisk = !n.slowDisk
	}
}

func (s *Sim) startCalm() {
	s.calm = true
	s.calmFrom = s.proposals
	s.calmOK = false
	clear(s.net.blocked)
	s.net = netState{blocked: s.net.blocked}
	for _, n := range s.nodes {
		n.skew = 0.9 + 0.2*s.rng.Float64()
		n.diskFull = false
		n.slowDisk = false
		if !n.up {
			s.restart(n)
		}
	}
}

func (s *Sim) endCalm() {
	s.calm = false
	if !s.calmOK {
		var st []string
		for _, n := range s.nodes {
			if n.up {
				c := n.core.Status()
				st = append(st, fmt.Sprintf("%s: %s term %d lead %q commit %d applied %d last %d", n.id, c.State, c.Term, c.Lead, c.Commit, c.Applied, c.LastIndex))
			} else {
				st = append(st, fmt.Sprintf("%s: down", n.id))
			}
		}
		s.fail("liveness: nothing committed during a calm window of %v\n%v", calmDuration/second, st)
		return
	}
	s.calms++
}

func mix(h uint64, e raft.Entry) uint64 {
	x := h ^ e.Index*0x9e3779b97f4a7c15 ^ e.Term*0xbf58476d1ce4e5b9 ^ dataHash(e)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x | 1
}

func dataHash(e raft.Entry) uint64 {
	h := uint64(14695981039346656037) ^ uint64(e.Type)
	for _, b := range e.Data {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return h
}

type logView struct {
	base  uint64
	terms []uint64
}

func (v *logView) last() uint64 { return v.base + uint64(len(v.terms)) - 1 }

func (v *logView) term(i uint64) (uint64, bool) {
	if i < v.base || i > v.last() {
		return 0, false
	}
	return v.terms[i-v.base], true
}

func (v *logView) load(d *raft.MemoryStorage) {
	first, _ := d.FirstIndex()
	last, _ := d.LastIndex()
	v.base = first - 1
	v.terms = v.terms[:0]
	for i := v.base; i <= last; i++ {
		t, _ := d.Term(i)
		v.terms = append(v.terms, t)
	}
}

func (v *logView) reset(index, term uint64) {
	v.base = index
	v.terms = append(v.terms[:0], term)
}

func (v *logView) compact(i uint64) {
	if i <= v.base || i > v.last() {
		return
	}
	v.terms = append(v.terms[:0], v.terms[i-v.base:]...)
	v.base = i
}

type trace struct {
	buf [64]traceRec
	n   int
}

type traceRec struct {
	at   int64
	kind eventKind
	node raft.NodeID
	m    raft.Message
}

func (t *trace) add(s *Sim, ev *event) {
	r := traceRec{at: ev.at, kind: ev.kind}
	if ev.node != nil {
		r.node = ev.node.id
	}
	if ev.env != nil {
		r.m = ev.env.m
		r.m.Entries = nil
	}
	t.buf[t.n%len(t.buf)] = r
	t.n++
}

func (t *trace) String() string {
	var out []byte
	for i := max(0, t.n-len(t.buf)); i < t.n; i++ {
		r := t.buf[i%len(t.buf)]
		out = fmt.Appendf(out, "  %.6fs %-8s %s", float64(r.at)/second, eventNames[r.kind], r.node)
		if r.m.Type != 0 {
			out = fmt.Appendf(out, "%s %s->%s term %d index %d logterm %d commit %d reject %v", r.m.Type, r.m.From, r.m.To, r.m.Term, r.m.Index, r.m.LogTerm, r.m.Commit, r.m.Reject)
		}
		out = append(out, '\n')
	}
	return string(out)
}
