package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	killCycles  = flag.Int("kill.cycles", 0, "kill -9 cycles; 0 skips TestKillCycles")
	killClients = flag.Int("kill.clients", 16, "concurrent writers")
	killNoSync  = flag.Bool("kill.nosync", false, "run the nodes with --unsafe-no-fsync")
	killBin     = flag.String("kill.bin", "", "prebuilt node binary; built from source when empty")
)

type proc struct {
	id   int
	http string
	cmd  *exec.Cmd
	out  *os.File
}

type harness struct {
	t     *testing.T
	bin   string
	peers string
	dir   string
	procs []*proc
	mu    sync.Mutex
	hc    *http.Client
}

func freePorts(t *testing.T, n int) []string {
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

func buildBinary(t *testing.T) string {
	goBin, err := exec.LookPath("go")
	if err != nil {
		goBin = filepath.Join(runtime.GOROOT(), "bin", "go")
	}
	bin := filepath.Join(t.TempDir(), "node")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	out, err := exec.Command(goBin, "build", "-o", bin, ".").CombinedOutput()
	if err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

func (h *harness) start(p *proc) {
	h.t.Helper()
	args := []string{"--id", fmt.Sprint(p.id), "--peers", h.peers, "--dir", h.dir, "--http", p.http, "--election-timeout", "200ms"}
	if *killNoSync {
		args = append(args, "--unsafe-no-fsync")
	}
	p.cmd = exec.Command(h.bin, args...)
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	if err := p.cmd.Start(); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) kill(p *proc) {
	p.cmd.Process.Kill()
	p.cmd.Wait()
	p.cmd = nil
}

func (h *harness) put(addr, key, value string) (int, string) {
	req, _ := http.NewRequest(http.MethodPut, "http://"+addr+"/kv/"+key, strings.NewReader(value))
	resp, err := h.hc.Do(req)
	if err != nil {
		return 0, ""
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, resp.Header.Get("X-Raft-Leader")
}

func (h *harness) getJSON(addr, path string, v any) error {
	resp, err := h.hc.Get("http://" + addr + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(v)
}

func newHarness(t *testing.T) *harness {
	bin := *killBin
	if bin == "" {
		bin = buildBinary(t)
	}
	h := &harness{t: t, bin: bin, dir: t.TempDir(), hc: &http.Client{Timeout: 3 * time.Second}}
	addrs := freePorts(t, 6)
	var peers []string
	for i := range 3 {
		peers = append(peers, fmt.Sprintf("%d=%s", i+1, addrs[i]))
		out, err := os.Create(filepath.Join(h.dir, fmt.Sprintf("node%d.log", i+1)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { out.Close() })
		h.procs = append(h.procs, &proc{id: i + 1, http: addrs[3+i], out: out})
	}
	h.peers = strings.Join(peers, ",")
	for _, p := range h.procs {
		h.start(p)
	}
	t.Cleanup(func() {
		for _, p := range h.procs {
			if p.cmd != nil {
				h.kill(p)
			}
		}
	})
	return h
}

func (h *harness) nemesis(until time.Time) {
	for time.Now().Before(until) {
		time.Sleep(time.Duration(300+rand.IntN(1200)) * time.Millisecond)
		victim := h.procs[rand.IntN(len(h.procs))]
		h.kill(victim)
		time.Sleep(time.Duration(100+rand.IntN(700)) * time.Millisecond)
		h.start(victim)
	}
}

func TestKillCycles(t *testing.T) {
	if *killCycles == 0 {
		t.Skip("run with -kill.cycles=N")
	}
	h := newHarness(t)

	var acked sync.Map
	var nAcked, nUnknown atomic.Int64
	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for c := range *killClients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			target := h.procs[c%3].http
			for seq := 0; ctx.Err() == nil; seq++ {
				key, value := fmt.Sprintf("c%d-%d", c, seq), fmt.Sprint(rand.Uint64())
				for ctx.Err() == nil {
					code, leader := h.put(target, key, value)
					if code == http.StatusNoContent {
						acked.Store(key, value)
						nAcked.Add(1)
						break
					}
					if code == http.StatusGatewayTimeout || code == http.StatusConflict {
						nUnknown.Add(1)
						break
					}
					target = h.procs[rand.IntN(3)].http
					if leader != "" {
						for _, p := range h.procs {
							if fmt.Sprint(p.id) == leader {
								target = p.http
							}
						}
					}
					if code != http.StatusServiceUnavailable {
						time.Sleep(20 * time.Millisecond)
					}
				}
			}
		}()
	}

	start := time.Now()
	for cycle := range *killCycles {
		time.Sleep(time.Duration(300+rand.IntN(1200)) * time.Millisecond)
		victim := h.procs[rand.IntN(3)]
		h.kill(victim)
		time.Sleep(time.Duration(100+rand.IntN(700)) * time.Millisecond)
		h.start(victim)
		if cycle%10 == 9 {
			t.Logf("cycle %d: %d acked, %d unknown", cycle+1, nAcked.Load(), nUnknown.Load())
		}
	}
	time.Sleep(time.Second)
	stop()
	wg.Wait()
	elapsed := time.Since(start)

	type digest struct {
		Applied uint64 `json:"applied"`
		Digest  string `json:"digest"`
		Keys    int    `json:"keys"`
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var ds []digest
		for _, p := range h.procs {
			var d digest
			if err := h.getJSON(p.http, "/digest", &d); err == nil {
				ds = append(ds, d)
			}
		}
		if len(ds) == 3 && ds[0] == ds[1] && ds[1] == ds[2] {
			t.Logf("converged: applied %d, %d keys, digest %s", ds[0].Applied, ds[0].Keys, ds[0].Digest)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("nodes did not converge: %+v", ds)
		}
		time.Sleep(100 * time.Millisecond)
	}

	missing := 0
	acked.Range(func(k, v any) bool {
		for _, p := range h.procs {
			resp, err := h.hc.Get("http://" + p.http + "/kv/" + k.(string))
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK || string(body) != v.(string) {
				missing++
				if missing < 10 {
					t.Errorf("node %d lost acknowledged write %s=%s (got %d %q)", p.id, k, v, resp.StatusCode, body)
				}
			}
		}
		return true
	})
	snapshots := 0
	for _, p := range h.procs {
		b, _ := os.ReadFile(p.out.Name())
		snapshots += strings.Count(string(b), "created a snapshot")
	}
	t.Logf("%d kill cycles in %v: %d acknowledged writes (%.0f/s), %d with unknown outcome, %d missing, %d snapshots",
		*killCycles, elapsed.Round(time.Second), nAcked.Load(), float64(nAcked.Load())/elapsed.Seconds(), nUnknown.Load(), missing, snapshots)
	if missing > 0 {
		t.Fatalf("%d acknowledged writes missing; logs in %s", missing, h.dir)
	}
}
