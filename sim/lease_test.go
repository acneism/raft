package sim

import (
	"strings"
	"testing"

	"github.com/acneism/raft"
)

func leaseScenario(t *testing.T, seed uint64, bug Bug, leaderSkew, followerSkew float64) error {
	opts := config(seed)
	opts.KV, opts.Lease, opts.CheckQuorum, opts.Bug = true, true, true, bug
	s := New(opts)
	if err := s.Run(50_000); err != nil {
		t.Fatal(err)
	}
	var l *node
	for _, n := range s.nodes {
		if n.up && n.core.Status().State == raft.StateLeader {
			l = n
		}
	}
	if l == nil {
		return nil
	}
	s.calm = true
	for _, n := range s.nodes {
		n.skew = followerSkew
	}
	l.skew = leaderSkew
	for i := range s.nodes {
		if s.nodes[i] != l {
			s.net.blocked[[2]int{s.index[l], i}] = true
			s.net.blocked[[2]int{i, s.index[l]}] = true
		}
	}
	return s.Run(s.steps + 20_000)
}

func TestSimLeaseDrift(t *testing.T) {
	for seed := uint64(2); seed <= 60; seed += 3 {
		if err := leaseScenario(t, seed, NoBug, 1.07, 0.93); err != nil {
			t.Fatalf("seed %d, drift within the bound: %v", seed, err)
		}
	}
	for seed := uint64(2); seed <= 60; seed += 3 {
		err := leaseScenario(t, seed, NoBug, 2, 0.5)
		if err != nil && strings.Contains(err.Error(), "committed before it began") {
			t.Logf("drift beyond the bound caught with seed %d", seed)
			return
		}
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
	t.Fatal("stale lease reads beyond the drift bound were not detected")
}
