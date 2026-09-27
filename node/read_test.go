package node_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/acneism/raft/node"
)

func readOn(t *testing.T, m *member, key string) (string, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		idx, err := m.n.ReadIndex(ctx)
		if errors.Is(err, node.ErrNotLeader) && ctx.Err() == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("%s: ReadIndex: %v", m.id, err)
		}
		if err := m.n.WaitApplied(ctx, idx); err != nil {
			t.Fatalf("%s: WaitApplied(%d): %v", m.id, idx, err)
		}
		return m.fsm.Get(key)
	}
}

func TestReadIndexOnEveryNode(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.write("k", "v1")
	for _, m := range c.members {
		if v, ok := readOn(t, m, "k"); !ok || v != "v1" {
			t.Fatalf("%s read %q %v", m.id, v, ok)
		}
	}
}

func TestReadIndexOnFollowerSeesAcknowledgedWrites(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(5 * time.Second)
	var followers []*member
	for id, m := range c.members {
		if id != l.id {
			followers = append(followers, m)
		}
	}
	for i := range 100 {
		want := fmt.Sprint(i)
		c.write("k", want)
		f := followers[i%len(followers)]
		if v, _ := readOn(t, f, "k"); v != want {
			t.Fatalf("write %s acknowledged, then %s read %q", want, f.id, v)
		}
	}
}

func TestReadIndexWithoutQuorumFails(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(5 * time.Second)
	c.write("k", "v")
	for id := range c.members {
		if id != l.id {
			c.stop(id)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	idx, err := l.n.ReadIndex(ctx)
	if err == nil {
		t.Fatalf("read confirmed at %d with both followers stopped", idx)
	}
	if !errors.Is(err, node.ErrNotLeader) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected error %v", err)
	}
}
