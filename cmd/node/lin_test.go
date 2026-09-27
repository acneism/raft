package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

var (
	linDuration = flag.Duration("lin.duration", 0, "load duration of TestLinearizability; 0 skips it")
	linOut      = flag.String("lin.out", "", "file for the Porcupine visualization when the check fails")
)

type kvInput struct {
	op    byte
	key   string
	value string
}

type kvOutput struct {
	value   string
	unknown bool
}

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).key
			byKey[k] = append(byKey[k], op)
		}
		var out [][]porcupine.Operation
		for _, ops := range byKey {
			out = append(out, ops)
		}
		return out
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		st, in := state.(string), input.(kvInput)
		switch in.op {
		case 'g':
			return output.(kvOutput).value == st, st
		case 'p':
			return true, in.value
		}
		return true, st + in.value
	},
	DescribeOperation: func(input, output any) string {
		in, out := input.(kvInput), output.(kvOutput)
		res := out.value
		if out.unknown {
			res = "?"
		}
		switch in.op {
		case 'g':
			return fmt.Sprintf("get(%s) -> %q", in.key, res)
		case 'p':
			return fmt.Sprintf("put(%s, %q)", in.key, in.value)
		}
		return fmt.Sprintf("append(%s, %q)", in.key, in.value)
	},
}

func (h *harness) do(addr string, in kvInput) (int, string, string) {
	var req *http.Request
	switch in.op {
	case 'g':
		req, _ = http.NewRequest(http.MethodGet, "http://"+addr+"/kv/"+in.key+"?consistent=1", nil)
	case 'p':
		req, _ = http.NewRequest(http.MethodPut, "http://"+addr+"/kv/"+in.key, strings.NewReader(in.value))
	default:
		req, _ = http.NewRequest(http.MethodPost, "http://"+addr+"/kv/"+in.key+"/append", strings.NewReader(in.value))
	}
	resp, err := h.hc.Do(req)
	if err != nil {
		var op *net.OpError
		if errors.As(err, &op) && op.Op == "dial" {
			return http.StatusServiceUnavailable, "", ""
		}
		return 0, "", ""
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", ""
	}
	return resp.StatusCode, string(body), resp.Header.Get("X-Raft-Leader")
}

func TestLinearizability(t *testing.T) {
	if *linDuration == 0 {
		t.Skip("run with -lin.duration=D")
	}
	h := newHarness(t)
	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }
	deadline := start.Add(*linDuration)
	var mu sync.Mutex
	var history []porcupine.Operation
	var clients, acked, unknown atomic.Int64
	record := func(op porcupine.Operation) {
		mu.Lock()
		history = append(history, op)
		mu.Unlock()
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := int(clients.Add(1) - 1)
			target := h.procs[rand.IntN(len(h.procs))].http
			for seq := 0; time.Now().Before(deadline); seq++ {
				in := kvInput{key: fmt.Sprintf("k%d", rand.IntN(50))}
				switch r := rand.IntN(10); {
				case r < 5:
					in.op = 'g'
				case r < 7:
					in.op, in.value = 'p', fmt.Sprintf("%d.%d;", client, seq)
				default:
					in.op, in.value = 'a', fmt.Sprintf("%d.%d;", client, seq)
				}
				call := now()
				code, body, leader := h.do(target, in)
				ret := now()
				switch {
				case code == http.StatusOK || code == http.StatusNotFound && in.op == 'g':
					if code == http.StatusNotFound {
						body = ""
					}
					record(porcupine.Operation{ClientId: client, Input: in, Call: call, Output: kvOutput{value: body}, Return: ret})
					acked.Add(1)
				case code == http.StatusNoContent:
					record(porcupine.Operation{ClientId: client, Input: in, Call: call, Output: kvOutput{}, Return: ret})
					acked.Add(1)
				case code == http.StatusServiceUnavailable || code == http.StatusConflict:
					target = h.procs[rand.IntN(len(h.procs))].http
					for _, p := range h.procs {
						if fmt.Sprint(p.id) == leader {
							target = p.http
						}
					}
					if leader == "" {
						time.Sleep(20 * time.Millisecond)
					}
				default:
					if in.op != 'g' {
						record(porcupine.Operation{ClientId: client, Input: in, Call: call, Output: kvOutput{unknown: true}, Return: math.MaxInt64})
						unknown.Add(1)
						client = int(clients.Add(1) - 1)
					}
					target = h.procs[rand.IntN(len(h.procs))].http
				}
			}
		}()
	}
	h.nemesis(deadline)
	wg.Wait()

	checkStart := time.Now()
	res, info := porcupine.CheckOperationsVerbose(kvModel, history, 5*time.Minute)

	t.Logf("%d operations completed, %d with unknown outcome; checked in %v: %s", acked.Load(), unknown.Load(), time.Since(checkStart).Round(time.Millisecond), res)
	if res != porcupine.Ok {
		path := *linOut
		if path == "" {
			path = filepath.Join(t.TempDir(), "linearizability.html")
		}
		if err := porcupine.VisualizePath(kvModel, info, path); err != nil {
			t.Log(err)
		}
		t.Fatalf("history is %s; visualization in %s", res, path)
	}
}
