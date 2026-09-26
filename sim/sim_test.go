package sim

import (
	"flag"
	"fmt"
	"testing"
)

var (
	steps = flag.Int64("sim.steps", 300_000, "events per simulation run")
	seeds = flag.Int("sim.seeds", 8, "number of seeds to run")
	seed  = flag.Uint64("sim.seed", 0, "run only this seed")
)

func config(seed uint64) Options {
	return Options{
		Seed:        seed,
		Nodes:       []int{3, 5, 3, 4}[seed%4],
		PreVote:     seed%3 != 0,
		CheckQuorum: seed%5 != 0,
	}
}

func TestSim(t *testing.T) {
	run := func(seed uint64) {
		opts := config(seed)
		name := fmt.Sprintf("seed=%d/nodes=%d/prevote=%v/checkquorum=%v", seed, opts.Nodes, opts.PreVote, opts.CheckQuorum)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := New(opts)
			if err := s.Run(*steps); err != nil {
				t.Fatal(err)
			}
			if *steps >= 1_000_000 && s.Calms() == 0 {
				t.Fatal("no calm window completed")
			}
			t.Logf("%d steps, %d calm windows", s.Steps(), s.Calms())
		})
	}
	if *seed != 0 {
		run(*seed)
		return
	}
	for i := range *seeds {
		run(uint64(i + 1))
	}
}

func TestSimDeterministic(t *testing.T) {
	a, b := New(config(7)), New(config(7))
	if err := a.Run(50_000); err != nil {
		t.Fatal(err)
	}
	if err := b.Run(50_000); err != nil {
		t.Fatal(err)
	}
	if a.now != b.now || a.proposals != b.proposals || a.chk.maxCommit != b.chk.maxCommit {
		t.Fatalf("runs diverged: t=%d/%d proposals=%d/%d commit=%d/%d", a.now, b.now, a.proposals, b.proposals, a.chk.maxCommit, b.chk.maxCommit)
	}
}

func TestSimCatchesBugs(t *testing.T) {
	for _, bug := range []Bug{BugSendBeforePersist, BugForgetVote} {
		t.Run(fmt.Sprint(bug), func(t *testing.T) {
			t.Parallel()
			for seed := uint64(1); seed <= 20; seed++ {
				opts := config(seed)
				opts.Bug = bug
				if err := New(opts).Run(2_000_000); err != nil {
					t.Logf("caught with seed %d: %.200s", seed, err)
					return
				}
			}
			t.Fatal("violation not detected")
		})
	}
}
