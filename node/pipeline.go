package node

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/acneism/raft"
)

type queue[T any] struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []T
	closed bool
}

func (q *queue[T]) init() { q.cond = sync.NewCond(&q.mu) }

func (q *queue[T]) push(items ...T) {
	if len(items) == 0 {
		return
	}
	q.mu.Lock()
	q.items = append(q.items, items...)
	q.mu.Unlock()
	q.cond.Signal()
}

func (q *queue[T]) popAll() ([]T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	if q.closed {
		return nil, false
	}
	items := q.items
	q.items = nil
	return items, true
}

func (q *queue[T]) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

type applyJob struct {
	rds    []raft.Ready
	paused chan struct{}
	resume chan struct{}
}

func (n *Node) run() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	start, ticks := time.Now(), 0
	for {
		select {
		case <-n.stopc:
			return
		case now := <-ticker.C:
			due := int(now.Sub(start) / n.cfg.TickInterval)
			ticks = max(ticks, due-2*n.cfg.ElectionTicks)
			n.mu.Lock()
			for ; ticks < due; ticks++ {
				n.core.Tick()
				n.observe()
			}
			n.mu.Unlock()
		case m := <-n.recvc:
			n.mu.Lock()
			n.step(m)
			for more := true; more; {
				select {
				case m := <-n.recvc:
					n.step(m)
				default:
					more = false
				}
			}
			n.mu.Unlock()
		case <-n.notifyc:
			n.repMu.Lock()
			reps := n.reports
			n.reports = nil
			n.repMu.Unlock()
			n.mu.Lock()
			for _, r := range reps {
				if r.snapshot {
					n.core.ReportSnapshot(r.id, r.failed)
				} else {
					n.core.ReportUnreachable(r.id)
				}
			}
			n.mu.Unlock()
		case rds := <-n.persistedc:
			n.mu.Lock()
			for _, rd := range rds {
				n.core.AdvancePersist(rd)
			}
			n.observe()
			n.mu.Unlock()
		case rds := <-n.donec:
			n.mu.Lock()
			for _, rd := range rds {
				n.core.AdvanceApply(rd)
			}
			n.observe()
			n.mu.Unlock()
		}
		n.processReady()
	}
}

func (n *Node) step(m raft.Message) {
	if err := n.core.Step(m); err != nil {
		n.logger.Warn("step", "type", m.Type, "from", m.From, "err", err)
	}
	n.observe()
}

func (n *Node) observe() {
	st := n.core.Status()
	e := Event{Term: st.Term, Leader: st.Lead, Ready: st.LeaderReady}
	if e == n.obs {
		return
	}
	n.obs = e
	n.events.push(e)
	leaderTerm := uint64(0)
	if st.State == raft.StateLeader {
		leaderTerm = st.Term
	}
	n.setLeaderTerm(leaderTerm, st.Commit)
}

func (n *Node) processReady() {
	n.mu.Lock()
	var rds []raft.Ready
	for n.core.HasReady() {
		rds = append(rds, n.core.Ready())
	}
	n.observe()
	n.mu.Unlock()
	var job applyJob
	for i := range rds {
		if len(rds[i].Committed) > 0 {
			job.rds = append(job.rds, raft.Ready{Committed: rds[i].Committed})
		}
		if n.cfg.SerialPersist {
			rds[i].MessagesAfterPersist = append(rds[i].Messages, rds[i].MessagesAfterPersist...)
			rds[i].Messages = nil
			continue
		}
		n.send(rds[i].Messages)
	}
	if len(job.rds) > 0 || n.snapWanted.Load() {
		n.aq.push(job)
	}
	n.pq.push(rds...)
}

func (n *Node) send(msgs []raft.Message) {
	for i := range msgs {
		m := msgs[i]
		if m.Type == raft.MsgSnap {
			n.tr.SendSnapshot(m, n.snapDir(*m.Snapshot))
			continue
		}
		j := i + 1
		for j < len(msgs) && msgs[j].Type != raft.MsgSnap {
			j++
		}
		n.tr.Send(msgs[i:j])
		i = j - 1
	}
}

func (n *Node) persister() {
	defer n.wg.Done()
	for {
		rds, ok := n.pq.popAll()
		if !ok {
			return
		}
		for len(rds) > 0 {
			k := 1
			if rds[0].Snapshot == nil {
				for k < len(rds) && rds[k].Snapshot == nil {
					k++
				}
			}
			group := rds[:k]
			rds = rds[k:]
			if s := group[0].Snapshot; s != nil {
				if err := n.installSnapshot(*s); err != nil {
					n.fail(err)
					return
				}
				n.aq.push(applyJob{rds: []raft.Ready{{Snapshot: s}}})
			}
			if err := n.persist(group); err != nil {
				n.fail(err)
				return
			}
			for i := range group {
				n.send(group[i].MessagesAfterPersist)
			}
			select {
			case n.persistedc <- group:
			case <-n.stopc:
				return
			}
		}
	}
}

func (n *Node) persist(group []raft.Ready) error {
	var ents []raft.Entry
	var hs raft.HardState
	sync := false
	for i := range group {
		rd := &group[i]
		if len(rd.Entries) > 0 {
			if len(ents) > 0 && rd.Entries[0].Index != ents[len(ents)-1].Index+1 {
				if err := n.log.Append(ents); err != nil {
					return err
				}
				ents = nil
			}
			if ents == nil {
				ents = slices.Clip(rd.Entries)
			} else {
				ents = append(ents, rd.Entries...)
			}
		}
		if !rd.HardState.IsEmpty() {
			hs = rd.HardState
		}
		sync = sync || rd.MustSync
	}
	if err := n.log.Append(ents); err != nil {
		return err
	}
	if err := n.log.SetHardState(hs); err != nil {
		return err
	}
	if sync {
		return n.log.Sync()
	}
	return nil
}

func (n *Node) pauseApplier() (resume func(), ok bool) {
	job := applyJob{paused: make(chan struct{}), resume: make(chan struct{})}
	n.aq.push(job)
	select {
	case <-job.paused:
		return func() { close(job.resume) }, true
	case <-n.stopc:
		return nil, false
	}
}

func (n *Node) applier() {
	defer n.wg.Done()
	for {
		jobs, ok := n.aq.popAll()
		if !ok {
			return
		}
		var ents []raft.Entry
		var done []raft.Ready
		flush := func() bool {
			if len(ents) > 0 {
				if err := n.fsm.Apply(ents); err != nil {
					n.fail(fmt.Errorf("node: apply: %w", err))
					return false
				}
				n.appliedTo(ents[len(ents)-1].Index, ents)
				ents = nil
			}
			if err := n.maybeCompact(); err != nil {
				n.fail(err)
				return false
			}
			if err := n.maybeSnapshot(); err != nil {
				n.fail(err)
				return false
			}
			if len(done) > 0 {
				select {
				case n.donec <- done:
				case <-n.stopc:
					return false
				}
				done = nil
			}
			return true
		}
		for _, job := range jobs {
			if job.paused != nil {
				if !flush() {
					return
				}
				close(job.paused)
				select {
				case <-job.resume:
				case <-n.stopc:
					return
				}
				continue
			}
			for _, rd := range job.rds {
				if rd.Snapshot != nil {
					if !flush() {
						return
					}
					n.appliedTo(rd.Snapshot.Index, nil)
				}
				ents = append(ents, rd.Committed...)
			}
			done = append(done, job.rds...)
		}
		if !flush() {
			return
		}
	}
}
