package node

import (
	"fmt"
	"sync"
	"time"

	"github.com/acneism/raft"
)

type readyQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []raft.Ready
	closed bool
}

func (q *readyQueue) push(rds []raft.Ready) {
	if len(rds) == 0 {
		return
	}
	q.mu.Lock()
	q.items = append(q.items, rds...)
	q.mu.Unlock()
	q.cond.Signal()
}

func (q *readyQueue) popAll() ([]raft.Ready, bool) {
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

func (q *readyQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

type applyJob struct {
	rds     []raft.Ready
	barrier chan struct{}
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
		case rds := <-n.donec:
			n.mu.Lock()
			for _, rd := range rds {
				n.core.Advance(rd)
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
	for i := range rds {
		if n.cfg.SerialPersist {
			rds[i].MessagesAfterPersist = append(rds[i].Messages, rds[i].MessagesAfterPersist...)
			rds[i].Messages = nil
			continue
		}
		n.send(rds[i].Messages)
	}
	n.pq.push(rds)
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
	defer close(n.applyc)
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
			if err := n.persist(group); err != nil {
				n.fail(err)
				return
			}
			for i := range group {
				n.send(group[i].MessagesAfterPersist)
			}
			select {
			case n.applyc <- applyJob{rds: group}:
			case <-n.stopc:
				return
			}
		}
	}
}

func (n *Node) persist(group []raft.Ready) error {
	if s := group[0].Snapshot; s != nil {
		if err := n.installSnapshot(*s); err != nil {
			return err
		}
	}
	sync := false
	for i := range group {
		rd := &group[i]
		if err := n.log.Append(rd.Entries); err != nil {
			return err
		}
		if err := n.log.SetHardState(rd.HardState); err != nil {
			return err
		}
		sync = sync || rd.MustSync
	}
	if sync {
		return n.log.Sync()
	}
	return nil
}

func (n *Node) barrier() bool {
	done := make(chan struct{})
	select {
	case n.applyc <- applyJob{barrier: done}:
	case <-n.stopc:
		return false
	}
	select {
	case <-done:
		return true
	case <-n.stopc:
		return false
	}
}

func (n *Node) applier() {
	defer n.wg.Done()
	for job := range n.applyc {
		if job.barrier != nil {
			close(job.barrier)
			continue
		}
		for i := range job.rds {
			rd := &job.rds[i]
			if rd.Snapshot != nil {
				n.appliedTo(rd.Snapshot.Index, nil)
			}
			if len(rd.Committed) > 0 {
				if err := n.fsm.Apply(rd.Committed); err != nil {
					n.fail(fmt.Errorf("node: apply: %w", err))
					return
				}
				n.appliedTo(rd.Committed[len(rd.Committed)-1].Index, rd.Committed)
			}
		}
		if err := n.maybeCompact(); err != nil {
			n.fail(err)
			return
		}
		if err := n.maybeSnapshot(); err != nil {
			n.fail(err)
			return
		}
		select {
		case n.donec <- job.rds:
		case <-n.stopc:
			return
		}
	}
}
