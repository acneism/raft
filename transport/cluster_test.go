package transport

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/acneism/raft"
)

func TestClusterIDChecked(t *testing.T) {
	var lb logBuf
	rb := newRecorder()
	b, err := New(Config{ID: "b", Listen: "127.0.0.1:0", ClusterID: "y", Handler: rb, SnapshotDir: t.TempDir(),
		Logger: slog.New(slog.NewTextHandler(&lb, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.AddPeer("a", "127.0.0.1:1")
	send := func(cluster string, term uint64) {
		a, err := New(Config{ID: "a", Listen: "127.0.0.1:0", ClusterID: cluster, Handler: newRecorder(), SnapshotDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close()
		a.AddPeer("b", b.Addr().String())
		a.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: "a", To: "b", Term: term}})
		time.Sleep(300 * time.Millisecond)
	}
	send("x", 1)
	send("", 2)
	send("y", 3)
	rb.mu.Lock()
	var terms []uint64
	for _, m := range rb.msgs {
		terms = append(terms, m.Term)
	}
	rb.mu.Unlock()
	if len(terms) != 2 || terms[0] != 2 || terms[1] != 3 {
		t.Fatalf("delivered terms %v, want [2 3]", terms)
	}
	logs := lb.String()
	if !strings.Contains(logs, "cluster=x") || !strings.Contains(logs, "without a cluster ID") {
		t.Fatalf("acceptor logged:\n%s", logs)
	}
}
