package transport

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/acneism/raft"
)

func TestRemovedPeerIsToldOnItsNextDial(t *testing.T) {
	told := make(chan raft.NodeID, 4)
	a, err := New(Config{ID: "a", Listen: "127.0.0.1:0", Handler: newRecorder(), SnapshotDir: t.TempDir(),
		OnRemoved: func(by raft.NodeID) { told <- by }})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	rb := newRecorder()
	b, err := New(Config{ID: "b", Listen: "127.0.0.1:0", Handler: rb, SnapshotDir: t.TempDir(),
		IsRemoved: func(id raft.NodeID) bool { return id == "a" }})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.AddPeer("a", a.Addr().String())
	a.AddPeer("b", b.Addr().String())
	a.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: "a", To: "b", Term: 1}})
	rb.wait(t, func() bool { return len(rb.msgs) == 1 })

	b.RemovePeer("a")
	select {
	case by := <-told:
		if by != "b" {
			t.Fatalf("told by %s", by)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the removed peer was not told without sending anything")
	}

	c, err := net.Dial("tcp", b.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	w := bufio.NewWriter(c)
	writeFrame(w, frameHello, hello{kind: streamMessages, from: "a", to: "b"}.encode())
	w.Flush()
	c.SetReadDeadline(time.Now().Add(time.Second))
	if n, _ := c.Read(make([]byte, 16)); n != 0 {
		t.Fatalf("an old dialer got %d bytes", n)
	}
}
