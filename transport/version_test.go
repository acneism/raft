package transport

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/acneism/raft"
)

func TestVersionNegotiatedWithNewPeer(t *testing.T) {
	a, _, _, rb := newPair(t, nil, nil)
	start := time.Now()
	a.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: "a", To: "b", Term: 1}})
	rb.wait(t, func() bool { return len(rb.msgs) == 1 })
	if d := time.Since(start); d >= versionWait {
		t.Fatalf("first message took %v", d)
	}
	if v := a.peer("b").version.Load(); v != protocolVersion {
		t.Fatalf("negotiated version %d", v)
	}
}

func TestOldAcceptorGetsVersionOne(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan raft.Message, 1)
	announced := make(chan uint64, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		r := bufio.NewReader(c)
		p, err := expectFrame(r, frameHello)
		if err != nil {
			return
		}
		h, _ := decodeHello(p)
		announced <- h.max
		p, err = expectFrame(r, frameMessages)
		if err != nil {
			return
		}
		if msgs, err := decodeBatch(p); err == nil && len(msgs) > 0 {
			got <- msgs[0]
		}
	}()
	a, _, _, _ := newPair(t, nil, nil)
	a.AddPeer("old", ln.Addr().String())
	a.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: "a", To: "old", Term: 2}})
	if max := <-announced; max != protocolVersion {
		t.Fatalf("hello announced %d", max)
	}
	select {
	case m := <-got:
		if m.Term != 2 {
			t.Fatalf("message %+v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message not delivered to a peer that does not answer the version")
	}
	if v := a.peer("old").version.Load(); v != 1 {
		t.Fatalf("version with an old peer %d", v)
	}
}

func TestOldDialerIsServed(t *testing.T) {
	_, b, _, rb := newPair(t, nil, nil)
	c, err := net.Dial("tcp", b.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	w := bufio.NewWriter(c)
	writeFrame(w, frameHello, hello{kind: streamMessages, from: "a", to: "b"}.encode())
	writeFrame(w, frameMessages, appendBatch(nil, []raft.Message{{Type: raft.MsgHeartbeat, From: "a", To: "b", Term: 3}}))
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	rb.wait(t, func() bool { return len(rb.msgs) == 1 && rb.msgs[0].Term == 3 })
	c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if n, _ := c.Read(make([]byte, 16)); n != 0 {
		t.Fatalf("the transport wrote %d bytes to a peer that did not announce a version", n)
	}
}
