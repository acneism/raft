package node

import (
	"context"
	"slices"
	"sync"

	"github.com/acneism/raft"
)

type waiter struct {
	term uint64
	ch   chan error
}

func (n *Node) Wait(ctx context.Context, p Proposal) error {
	n.wmu.Lock()
	if n.closed {
		n.wmu.Unlock()
		return ErrClosed
	}
	if p.Index <= n.applied {
		n.wmu.Unlock()
		t, err := n.log.Term(p.Index)
		switch {
		case err != nil:
			return ErrUnknown
		case t == p.Term:
			return nil
		}
		return ErrLost
	}
	if n.leaderTerm != p.Term {
		n.wmu.Unlock()
		return ErrUnknown
	}
	w := waiter{term: p.Term, ch: make(chan error, 1)}
	n.waiters[p.Index] = append(n.waiters[p.Index], w)
	n.wmu.Unlock()
	select {
	case err := <-w.ch:
		return err
	case <-ctx.Done():
		n.wmu.Lock()
		if ws := n.waiters[p.Index]; ws != nil {
			n.waiters[p.Index] = slices.DeleteFunc(ws, func(x waiter) bool { return x.ch == w.ch })
			if len(n.waiters[p.Index]) == 0 {
				delete(n.waiters, p.Index)
			}
		}
		n.wmu.Unlock()
		select {
		case err := <-w.ch:
			return err
		default:
			return ctx.Err()
		}
	}
}

func (n *Node) WaitApplied(ctx context.Context, index uint64) error {
	for {
		n.wmu.Lock()
		if n.closed {
			n.wmu.Unlock()
			return ErrClosed
		}
		if n.applied >= index {
			n.wmu.Unlock()
			return nil
		}
		ch := n.appliedCh
		n.wmu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (n *Node) appliedTo(index uint64, ents []raft.Entry) {
	n.wmu.Lock()
	defer n.wmu.Unlock()
	for _, e := range ents {
		for _, w := range n.waiters[e.Index] {
			if w.term == e.Term {
				w.ch <- nil
			} else {
				w.ch <- ErrLost
			}
		}
		delete(n.waiters, e.Index)
	}
	if ents == nil {
		for i, ws := range n.waiters {
			if i <= index {
				for _, w := range ws {
					w.ch <- ErrUnknown
				}
				delete(n.waiters, i)
			}
		}
	}
	if index > n.applied {
		n.applied = index
		close(n.appliedCh)
		n.appliedCh = make(chan struct{})
	}
}

func (n *Node) setLeaderTerm(term, committed uint64) {
	n.wmu.Lock()
	defer n.wmu.Unlock()
	if n.leaderTerm != 0 && n.leaderTerm != term {
		for i, ws := range n.waiters {
			if i > committed {
				for _, w := range ws {
					w.ch <- ErrUnknown
				}
				delete(n.waiters, i)
			}
		}
	}
	n.leaderTerm = term
}

type eventQueue struct {
	mu     sync.Mutex
	items  []Event
	notify chan struct{}
}

func (q *eventQueue) push(e Event) {
	q.mu.Lock()
	q.items = append(q.items, e)
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (n *Node) pumpEvents() {
	defer n.wg.Done()
	defer close(n.eventOut)
	for {
		select {
		case <-n.stopc:
			return
		case <-n.events.notify:
		}
		n.events.mu.Lock()
		items := n.events.items
		n.events.items = nil
		n.events.mu.Unlock()
		for _, e := range items {
			select {
			case n.eventOut <- e:
			case <-n.stopc:
				return
			}
		}
	}
}
