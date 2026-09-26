package node_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/acneism/raft"
	"github.com/acneism/raft/internal/kvfsm"
	"github.com/acneism/raft/node"
)

type member struct {
	id  raft.NodeID
	dir string
	fsm *kvfsm.FSM
	n   *node.Node
}

type cluster struct {
	t       *testing.T
	peers   map[raft.NodeID]string
	members map[raft.NodeID]*member
	tweak   func(*node.Config)
	fsmSync int
	wrap    func(raft.NodeID, *kvfsm.FSM) node.StateMachine
}

func freeAddrs(t *testing.T, n int) []string {
	var out []string
	var ls []net.Listener
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ls = append(ls, l)
		out = append(out, l.Addr().String())
	}
	for _, l := range ls {
		l.Close()
	}
	return out
}

func newCluster(t *testing.T, size int, tweak func(*node.Config)) *cluster {
	return newClusterFSM(t, size, 2, tweak)
}

func newClusterFSM(t *testing.T, size, fsmSync int, tweak func(*node.Config)) *cluster {
	c := &cluster{t: t, peers: map[raft.NodeID]string{}, members: map[raft.NodeID]*member{}, tweak: tweak, fsmSync: fsmSync}
	for i, addr := range freeAddrs(t, size) {
		id := raft.NodeID(fmt.Sprintf("n%d", i+1))
		c.peers[id] = addr
		c.members[id] = &member{id: id, dir: t.TempDir()}
	}
	for id := range c.members {
		c.start(id)
	}
	t.Cleanup(func() {
		for id := range c.members {
			c.stop(id)
		}
	})
	return c
}

func (c *cluster) start(id raft.NodeID) {
	c.t.Helper()
	m := c.members[id]
	fsm, err := kvfsm.Open(filepath.Join(m.dir, "fsm"), c.fsmSync)
	if err != nil {
		c.t.Fatal(err)
	}
	var sm node.StateMachine = fsm
	if c.wrap != nil {
		sm = c.wrap(id, fsm)
	}
	cfg := node.Config{
		ID:              id,
		Dir:             filepath.Join(m.dir, "raft"),
		Peers:           c.peers,
		StateMachine:    sm,
		TickInterval:    10 * time.Millisecond,
		ElectionTicks:   10,
		HeartbeatTicks:  1,
		PreVote:         true,
		CheckQuorum:     true,
		SegmentSize:     64 << 10,
		SnapshotEntries: 200,
		TrailingEntries: 20,
	}
	if c.tweak != nil {
		c.tweak(&cfg)
	}
	var n *node.Node
	for range 50 {
		if n, err = node.Open(cfg); err == nil || !isAddrInUse(err) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		fsm.Close()
		c.t.Fatal(err)
	}
	m.fsm, m.n = fsm, n
}

func isAddrInUse(err error) bool {
	var op *net.OpError
	return errors.As(err, &op)
}

func (c *cluster) stop(id raft.NodeID) {
	m := c.members[id]
	if m.n == nil {
		return
	}
	m.n.Close()
	m.fsm.Close()
	m.n, m.fsm = nil, nil
}

func (c *cluster) leader(timeout time.Duration) *member {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range c.members {
			if m.n != nil {
				if st := m.n.Status(); st.State == raft.StateLeader && st.LeaderReady {
					return m
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatal("no ready leader")
	return nil
}

func (c *cluster) write(key, value string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		l := c.leader(10 * time.Second)
		p, err := l.n.Propose(kvfsm.Command(key, value))
		if err != nil {
			continue
		}
		if err := l.n.Wait(ctx, p); err == nil {
			return
		}
	}
	c.t.Fatalf("write %s timed out", key)
}

func (c *cluster) converged(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var want uint64
		var idx uint64
		ok := true
		first := true
		for _, m := range c.members {
			if m.fsm == nil {
				continue
			}
			i, d := m.fsm.Digest()
			if first {
				want, idx, first = d, i, false
			} else if d != want || i != idx {
				ok = false
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			for _, m := range c.members {
				if m.fsm != nil {
					i, d := m.fsm.Digest()
					c.t.Logf("%s applied %d digest %x", m.id, i, d)
				}
			}
			c.t.Fatal("state machines did not converge")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestReplication(t *testing.T) {
	c := newCluster(t, 3, nil)
	for i := range 100 {
		c.write(fmt.Sprintf("k%d", i), fmt.Sprint(i))
	}
	c.converged(5 * time.Second)
	for _, m := range c.members {
		if v, _ := m.fsm.Get("k99"); v != "99" {
			t.Fatalf("%s: k99 = %q", m.id, v)
		}
	}
}

func TestConcurrentProposals(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(5 * time.Second)
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for w := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var last node.Proposal
			for i := range 20 {
				p, err := l.n.Propose(kvfsm.Command(fmt.Sprintf("w%d-%d", w, i), "x"))
				if err != nil {
					errs <- err
					return
				}
				if p.Index <= last.Index {
					errs <- fmt.Errorf("indexes not increasing: %d after %d", p.Index, last.Index)
					return
				}
				last = p
				if err := l.n.Wait(ctx, p); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	c.converged(5 * time.Second)
	if n := l.fsm.Len(); n != 32*20 {
		t.Fatalf("%d keys", n)
	}
}

func TestFollowerRejectsProposals(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(5 * time.Second)
	for id, m := range c.members {
		if id != l.id {
			if _, err := m.n.Propose(nil); !errors.Is(err, node.ErrNotLeader) {
				t.Fatalf("follower propose: %v", err)
			}
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.write("before", "1")
	old := c.leader(5 * time.Second)
	c.stop(old.id)
	start := time.Now()
	nl := c.leader(5 * time.Second)
	if d := time.Since(start); d > 2*10*10*time.Millisecond+300*time.Millisecond {
		t.Logf("new leader after %v", d)
	}
	if nl.id == old.id {
		t.Fatal("stopped node is leader")
	}
	c.write("after", "2")
	c.start(old.id)
	c.converged(5 * time.Second)
	if v, _ := c.members[old.id].fsm.Get("after"); v != "2" {
		t.Fatalf("restarted node did not catch up: %q", v)
	}
}

func TestWaitOutcomes(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(5 * time.Second)
	p, err := l.n.Propose(kvfsm.Command("a", "1"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.n.Wait(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := l.n.Wait(ctx, p); err != nil {
		t.Fatalf("waiting again on an applied proposal: %v", err)
	}
	if err := l.n.Wait(ctx, node.Proposal{Index: p.Index, Term: p.Term + 7}); !errors.Is(err, node.ErrLost) {
		t.Fatalf("different term at an applied index: %v", err)
	}
	for id := range c.members {
		if id != l.id {
			c.stop(id)
		}
	}
	p2, err := l.n.Propose(kvfsm.Command("b", "2"))
	if err != nil {
		t.Fatal(err)
	}
	res := make(chan error, 1)
	go func() { res <- l.n.Wait(context.Background(), p2) }()
	select {
	case err := <-res:
		if !errors.Is(err, node.ErrUnknown) {
			t.Fatalf("leader without quorum: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait hangs after the leader lost its quorum")
	}
	p3, err := l.n.Propose(nil)
	if err == nil {
		go func() { res <- l.n.Wait(context.Background(), p3) }()
	} else {
		go func() { res <- node.ErrClosed }()
	}
	c.stop(l.id)
	select {
	case err := <-res:
		if !errors.Is(err, node.ErrClosed) && !errors.Is(err, node.ErrUnknown) {
			t.Fatalf("after close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait hangs after Close")
	}
}

func TestEvents(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(5 * time.Second)
	sawReady := false
	timeout := time.After(5 * time.Second)
	for !sawReady {
		select {
		case e := <-l.n.Events():
			if e.Ready && e.Leader == l.id {
				sawReady = true
			}
		case <-timeout:
			t.Fatal("leader never reported Ready")
		}
	}
	var follower *member
	for id, m := range c.members {
		if id != l.id {
			follower = m
		}
	}
	term := l.n.Status().Term
	c.stop(l.id)
	timeout = time.After(5 * time.Second)
	for {
		select {
		case e := <-follower.n.Events():
			if e.Term > term && e.Leader != l.id && e.Leader != raft.None {
				return
			}
		case <-timeout:
			t.Fatal("follower saw no new leader")
		}
	}
}

func TestSnapshotCatchUp(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(5 * time.Second)
	var lagging raft.NodeID
	for id := range c.members {
		if id != l.id {
			lagging = id
			break
		}
	}
	c.stop(lagging)
	for i := range 600 {
		c.write(fmt.Sprintf("k%d", i), "v")
	}
	c.start(lagging)
	c.converged(10 * time.Second)
	if _, restores := c.members[lagging].fsm.Stats(); restores == 0 {
		t.Fatal("lagging follower caught up without a snapshot")
	}
	c.write("after", "1")
	c.converged(5 * time.Second)
}

func TestRestartReplaysAfterDurableIndex(t *testing.T) {
	c := newCluster(t, 3, func(cfg *node.Config) { cfg.SnapshotEntries = 1 << 20 })
	for i := range 50 {
		c.write(fmt.Sprintf("k%d", i), "v")
	}
	c.converged(5 * time.Second)
	l := c.leader(time.Second)
	durable := l.fsm.DurableIndex()
	c.stop(l.id)
	c.start(l.id)
	c.converged(5 * time.Second)
	applies, _ := c.members[l.id].fsm.Stats()
	applied := c.members[l.id].fsm.Applied()
	if uint64(applies) > applied-durable+5 {
		t.Fatalf("replayed %d entries, durable index was %d of %d", applies, durable, applied)
	}
}

type failingRestore struct {
	*kvfsm.FSM
	fail bool
}

func (f *failingRestore) Restore(src node.SnapshotSource) error {
	if f.fail {
		return errors.New("simulated crash in Restore")
	}
	return f.FSM.Restore(src)
}

func TestInterruptedRestoreIsRepeated(t *testing.T) {
	var victim raft.NodeID
	crash := true
	c := newCluster(t, 3, nil)
	c.wrap = func(id raft.NodeID, f *kvfsm.FSM) node.StateMachine {
		return &failingRestore{FSM: f, fail: id == victim && crash}
	}
	l := c.leader(5 * time.Second)
	for id := range c.members {
		if id != l.id {
			victim = id
			break
		}
	}
	c.stop(victim)
	for i := range 600 {
		c.write(fmt.Sprintf("k%d", i), "v")
	}
	c.start(victim)
	m := c.members[victim]
	deadline := time.Now().Add(10 * time.Second)
	for m.n.Err() == nil {
		if time.Now().After(deadline) {
			t.Fatal("the failing restore was never attempted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.stop(victim)
	crash = false
	entries, _ := os.ReadDir(filepath.Join(m.dir, "raft", "snap"))
	if len(entries) == 0 {
		t.Fatal("no snapshot files kept for the retry")
	}
	c.start(victim)
	if _, restores := c.members[victim].fsm.Stats(); restores == 0 {
		t.Fatal("restart did not repeat the restore")
	}
	c.converged(10 * time.Second)
}

func TestProposeInRejectsOtherTerm(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(5 * time.Second)
	term := l.n.Status().Term
	if _, err := l.n.ProposeIn(term+1, nil); !errors.Is(err, node.ErrNotLeader) {
		t.Fatalf("proposal for a future term: %v", err)
	}
	p, err := l.n.ProposeIn(term, kvfsm.Command("k", "v"))
	if err != nil || p.Term != term {
		t.Fatalf("proposal in the current term: %+v, %v", p, err)
	}
}
