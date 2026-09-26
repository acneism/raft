package transport

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"

	"github.com/gliedabrennung/raft"
)

var sampleMessages = []raft.Message{
	{Type: raft.MsgHeartbeat, From: "a", To: "b", Term: 3, Commit: 7},
	{Type: raft.MsgApp, From: "a", To: "b", Term: 1 << 40, Index: 9, LogTerm: 2, Commit: 8, Entries: []raft.Entry{
		{Index: 10, Term: 3, Type: raft.EntryNoop},
		{Index: 11, Term: 3, Data: []byte("payload")},
	}},
	{Type: raft.MsgAppResp, From: "b", To: "a", Term: 3, Index: 5, Reject: true, RejectHint: 4, LogTerm: 2},
	{Type: raft.MsgSnap, From: "a", To: "c", Term: 4, Snapshot: &raft.SnapshotMeta{Index: 100, Term: 4, Conf: raft.ConfState{
		Voters: []raft.NodeID{"a", "b", "c"}, Learners: []raft.NodeID{"d"},
	}}},
	{Type: raft.MsgPreVote, From: "c", To: "a", Term: 5, Index: 11, LogTerm: 3},
}

func TestMessageRoundTrip(t *testing.T) {
	for _, m := range sampleMessages {
		got, err := DecodeMessage(AppendMessage(nil, &m))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, m) {
			t.Fatalf("round trip:\n got %+v\nwant %+v", got, m)
		}
	}
	batch, err := decodeBatch(appendBatch(nil, sampleMessages))
	if err != nil || !reflect.DeepEqual(batch, sampleMessages) {
		t.Fatalf("batch round trip: %v", err)
	}
}

func TestFrameRejectsCorruption(t *testing.T) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	writeFrame(w, frameMessages, appendBatch(nil, sampleMessages))
	w.Flush()
	raw := buf.Bytes()
	for _, i := range []int{4, 6, frameHeader + 3, len(raw) - 1} {
		c := bytes.Clone(raw)
		c[i] ^= 0x40
		if _, _, err := readFrame(bufio.NewReader(bytes.NewReader(c))); err == nil {
			t.Fatalf("corruption at byte %d not detected", i)
		}
	}
}

func FuzzDecodeMessage(f *testing.F) {
	for _, m := range sampleMessages {
		f.Add(AppendMessage(nil, &m))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := DecodeMessage(b)
		if err != nil {
			return
		}
		again, err := DecodeMessage(AppendMessage(nil, &m))
		if err != nil || !reflect.DeepEqual(again, m) {
			t.Fatalf("re-encoding a decoded message changed it: %v", err)
		}
	})
}

func FuzzDecodeBatch(f *testing.F) {
	f.Add(appendBatch(nil, sampleMessages))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0x0f})
	f.Fuzz(func(t *testing.T, b []byte) {
		msgs, err := decodeBatch(b)
		if err != nil {
			return
		}
		if _, err := decodeBatch(appendBatch(nil, msgs)); err != nil {
			t.Fatal(err)
		}
	})
}

func FuzzDecodeOffer(f *testing.F) {
	f.Add(encodeOffer(&sampleMessages[3], []SnapshotFile{{Name: "data", Size: 10, CRC: 7}}))
	f.Fuzz(func(t *testing.T, b []byte) {
		m, files, err := decodeOffer(b)
		if err != nil {
			return
		}
		if _, _, err := decodeOffer(encodeOffer(&m, files)); err != nil {
			t.Fatal(err)
		}
	})
}

func FuzzReadFrame(f *testing.F) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	writeFrame(w, frameHello, hello{kind: streamMessages, from: "a", to: "b"}.encode())
	w.Flush()
	f.Add(buf.Bytes())
	f.Fuzz(func(t *testing.T, b []byte) {
		r := bufio.NewReader(bytes.NewReader(b))
		for {
			typ, p, err := readFrame(r)
			if err != nil {
				return
			}
			if typ == frameHello {
				decodeHello(p)
			}
		}
	})
}
