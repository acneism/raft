package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

var memberDuration = flag.Duration("member.duration", 0, "load duration of TestMembership; 0 skips it")

type membershipStats struct {
	added, addFailed, removed, removeFailed int
}

func (h *harness) live() []*proc {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*proc
	for _, p := range h.procs {
		if !p.removed {
			out = append(out, p)
		}
	}
	return out
}

func (h *harness) leaderProc() *proc {
	for _, p := range h.live() {
		var st struct {
			State string `json:"state"`
		}
		if h.getJSON(p.http, "/status", &st) == nil && st.State == "Leader" {
			return p
		}
	}
	return nil
}

func (h *harness) voters() []string {
	l := h.leaderProc()
	if l == nil {
		return nil
	}
	var m struct {
		Voters []string `json:"voters"`
	}
	if h.getJSON(l.http, "/members", &m) != nil {
		return nil
	}
	return m.Voters
}

func (h *harness) changeMembers(method, path string) bool {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		l := h.leaderProc()
		if l == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		req, _ := http.NewRequest(method, "http://"+l.http+path, nil)
		resp, err := h.hc.Do(req)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusNoContent, http.StatusBadRequest:
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func (h *harness) addMember(stats *membershipStats) {
	ports := freePorts(h.t, 2)
	var peers []string
	for _, p := range h.live() {
		peers = append(peers, fmt.Sprintf("%d=%s", p.id, p.raft))
	}
	h.mu.Lock()
	id := len(h.procs) + 1
	h.mu.Unlock()
	advertised := h.net.listen(h.t, fmt.Sprint(id), ports[0])
	peers = append(peers, fmt.Sprintf("%d=%s", id, advertised))
	out, err := os.Create(filepath.Join(h.dir, fmt.Sprintf("node%d.log", id)))
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { out.Close() })
	p := &proc{id: id, http: ports[1], raft: advertised, listen: ports[0], peers: strings.Join(peers, ","), join: true, out: out}
	h.start(p)
	h.mu.Lock()
	h.procs = append(h.procs, p)
	h.mu.Unlock()
	if h.changeMembers(http.MethodPost, fmt.Sprintf("/members/%d?addr=%s", id, advertised)) &&
		h.changeMembers(http.MethodPost, fmt.Sprintf("/members/%d/promote", id)) {
		stats.added++
		return
	}
	stats.addFailed++
}

func (h *harness) removeMember(voters []string, stats *membershipStats) {
	id := voters[rand.IntN(len(voters))]
	if !h.changeMembers(http.MethodDelete, "/members/"+id) {
		stats.removeFailed++
		return
	}
	stats.removed++
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, p := range h.procs {
		if fmt.Sprint(p.id) == id && !p.removed {
			p.removed = true
			if p.cmd != nil {
				h.kill(p)
			}
		}
	}
}

func TestMembership(t *testing.T) {
	if *memberDuration == 0 {
		t.Skip("run with -member.duration=D")
	}
	h := newHarness(t)
	var stats membershipStats
	h.linearizability(*memberDuration, func(until time.Time) {
		for time.Now().Before(until) {
			time.Sleep(time.Duration(500+rand.IntN(1000)) * time.Millisecond)
			voters := h.voters()
			switch {
			case len(voters) == 0:
			case len(voters) < 5 && (len(voters) <= 3 || rand.IntN(2) == 0):
				h.addMember(&stats)
			case len(voters) > 3:
				h.removeMember(voters, &stats)
			}
		}
	})
	final := h.voters()
	slices.Sort(final)
	t.Logf("%d members added (%d failed), %d removed (%d failed); voters at the end: %v",
		stats.added, stats.addFailed, stats.removed, stats.removeFailed, final)
	if stats.added == 0 || stats.removed == 0 {
		t.Fatal("membership did not change")
	}
}
