package sim

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

var (
	linSteps = flag.Int64("sim.lin.steps", 200_000, "events per run of TestSimLinearizable")
	linOut   = flag.String("sim.lin.out", "", "directory for Porcupine visualizations of failed checks")
)

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := make([][]porcupine.Operation, kvKeys)
		for _, op := range history {
			k := op.Input.(Op).Key
			byKey[k] = append(byKey[k], op)
		}
		return byKey
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		st, in := state.(string), input.(Op)
		switch in.Kind {
		case 'g':
			return output.(Op).Output == st, st
		case 'p':
			return true, in.Value
		}
		return true, st + in.Value
	},
	DescribeOperation: func(input, output any) string {
		in, out := input.(Op), output.(Op)
		switch {
		case in.Kind == 'g':
			return fmt.Sprintf("get(k%d) -> %q", in.Key, out.Output)
		case in.Kind == 'p' && out.Unknown:
			return fmt.Sprintf("put(k%d, %q) -> ?", in.Key, in.Value)
		case in.Kind == 'p':
			return fmt.Sprintf("put(k%d, %q)", in.Key, in.Value)
		case out.Unknown:
			return fmt.Sprintf("append(k%d, %q) -> ?", in.Key, in.Value)
		}
		return fmt.Sprintf("append(k%d, %q)", in.Key, in.Value)
	},
}

func checkLinearizable(t *testing.T, s *Sim, name string) (porcupine.CheckResult, int, int) {
	var history []porcupine.Operation
	unknown := 0
	for i, op := range s.History() {
		ret := op.Return
		if op.Unknown {
			ret = math.MaxInt64
			unknown++
		}
		history = append(history, porcupine.Operation{ClientId: i, Input: op, Call: op.Call, Output: op, Return: ret})
	}
	res, info := porcupine.CheckOperationsVerbose(kvModel, history, time.Minute)
	if res != porcupine.Ok && *linOut != "" {
		os.MkdirAll(*linOut, 0o755)
		path := filepath.Join(*linOut, name+".html")
		if err := porcupine.VisualizePath(kvModel, info, path); err != nil {
			t.Log(err)
		} else {
			t.Logf("visualization in %s", path)
		}
	}
	return res, len(history), unknown
}

func TestSimLinearizable(t *testing.T) {
	run := func(seed uint64) {
		opts := config(seed)
		opts.KV, opts.Members = true, seed%2 == 0
		name := fmt.Sprintf("seed=%d/members=%v", seed, opts.Members)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := New(opts)
			if err := s.Run(*linSteps); err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			res, ops, unknown := checkLinearizable(t, s, name)
			t.Logf("%d steps, %d operations, %d with unknown outcome, checked in %v: %s", s.Steps(), ops, unknown, time.Since(start).Round(time.Millisecond), res)
			if res != porcupine.Ok {
				t.Fatalf("history is %s", res)
			}
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

func TestSimLinearizableCatchesBugs(t *testing.T) {
	for _, bug := range []Bug{BugEarlyAck, BugStaleRead} {
		t.Run(fmt.Sprint(bug), func(t *testing.T) {
			for seed := uint64(1); seed <= 20; seed++ {
				opts := config(seed)
				opts.KV, opts.Bug = true, bug
				s := New(opts)
				if err := s.Run(100_000); err != nil {
					t.Logf("caught with seed %d: %.150s", seed, err)
					return
				}
				if res, ops, _ := checkLinearizable(t, s, fmt.Sprintf("bug%d-%d", bug, seed)); res == porcupine.Illegal {
					t.Logf("caught with seed %d among %d operations", seed, ops)
					return
				}
			}
			t.Fatal("not detected")
		})
	}
}
