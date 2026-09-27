package node_test

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/acneism/raft/internal/fsx"
	"github.com/acneism/raft/internal/kvfsm"
	"github.com/acneism/raft/node"
)

var roundN = flag.Int("round.n", 0, "rounds per measurement in TestRoundTime; 0 skips it")

type dist []time.Duration

func (d dist) pct(p float64) time.Duration {
	s := slices.Clone(d)
	slices.Sort(s)
	return s[min(len(s)-1, int(float64(len(s))*p))]
}

func (d dist) String() string {
	var sum time.Duration
	for _, x := range d {
		sum += x
	}
	return fmt.Sprintf("mean %7.3fms  p50 %7.3fms  p99 %7.3fms", ms(sum/time.Duration(len(d))), ms(d.pct(0.5)), ms(d.pct(0.99)))
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func measureFsync(t *testing.T, n int) dist {
	f, err := os.Create(filepath.Join(t.TempDir(), "fsync"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 128)
	if err := fsx.Preallocate(f, int64(n*len(buf))); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	var d dist
	for i := range n {
		start := time.Now()
		if _, err := f.WriteAt(buf, int64(i*len(buf))); err != nil {
			t.Fatal(err)
		}
		if err := fsx.Datasync(f); err != nil {
			t.Fatal(err)
		}
		d = append(d, time.Since(start))
	}
	return d
}

func measureRTT(t *testing.T, n int) dist {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		io.Copy(c, c)
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	buf := make([]byte, 128)
	var d dist
	for range n {
		start := time.Now()
		c.Write(buf)
		io.ReadFull(c, buf)
		d = append(d, time.Since(start))
	}
	return d
}

type roundMode struct {
	name   string
	serial bool
	noSync bool
}

var roundModes = []roundMode{
	{name: "parallel"},
	{name: "serial", serial: true},
	{name: "no fsync", noSync: true},
}

func measureRounds(t *testing.T, mode roundMode, writers, n int) (dist, float64) {
	c := newClusterFSM(t, 3, 1<<30, func(cfg *node.Config) {
		cfg.SerialPersist = mode.serial
		cfg.NoSync = mode.noSync
		cfg.SnapshotEntries = 1 << 30
		cfg.TickInterval = 50 * time.Millisecond
	})
	l := c.leader(5 * time.Second)
	c.write("warmup", "x")
	var mu sync.Mutex
	var d dist
	var wg sync.WaitGroup
	start := time.Now()
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for i := range n / writers {
				begin := time.Now()
				p, err := l.n.Propose(kvfsm.Put(fmt.Sprintf("w%d-%d", w, i), "value"))
				if err == nil {
					err = l.n.Wait(ctx, p)
				}
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				d = append(d, time.Since(begin))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	rate := float64(len(d)) / time.Since(start).Seconds()
	for id := range c.members {
		c.stop(id)
	}
	return d, rate
}

func TestRoundTime(t *testing.T) {
	if *roundN == 0 {
		t.Skip("run with -round.n=N")
	}
	n := *roundN
	fsync := measureFsync(t, n)
	rtt := measureRTT(t, n)
	t.Logf("fsync                 %v", fsync)
	t.Logf("tcp rtt               %v", rtt)
	for _, writers := range []int{1, 128} {
		for _, mode := range roundModes {
			d, rate := measureRounds(t, mode, writers, n*writers/min(writers, 8))
			t.Logf("%3d writers, %-8s %v  %8.0f ops/s", writers, mode.name, d, rate)
		}
	}
}

var failovers = flag.Int("failover.n", 0, "leader failovers in TestFailoverTime; 0 skips it")

func TestFailoverTime(t *testing.T) {
	if *failovers == 0 {
		t.Skip("run with -failover.n=N")
	}
	const tick = 2 * time.Millisecond
	election := 50 * tick
	c := newClusterFSM(t, 3, 1<<30, func(cfg *node.Config) { cfg.TickInterval = tick; cfg.ElectionTicks = 50; cfg.HeartbeatTicks = 5 })
	var d dist
	for i := range *failovers {
		old := c.leader(5 * time.Second)
		c.write(fmt.Sprintf("before%d", i), "x")
		start := time.Now()
		c.stop(old.id)
		c.write(fmt.Sprintf("after%d", i), "x")
		d = append(d, time.Since(start))
		c.start(old.id)
		c.converged(5 * time.Second)
	}
	t.Logf("election timeout %v, first write after the leader died: %v, max %v (%.2f election timeouts)",
		election, d, slices.Max(d), float64(slices.Max(d))/float64(election))
}
