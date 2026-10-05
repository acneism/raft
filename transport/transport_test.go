package transport

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/acneism/raft"
)

type recorder struct {
	mu          sync.Mutex
	msgs        []raft.Message
	snaps       []string
	unreachable int
	reports     []bool
	notify      chan struct{}
}

func newRecorder() *recorder { return &recorder{notify: make(chan struct{}, 1024)} }

func (r *recorder) signal() {
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

func (r *recorder) Receive(m raft.Message) {
	r.mu.Lock()
	r.msgs = append(r.msgs, m)
	r.mu.Unlock()
	r.signal()
}

func (r *recorder) ReceiveSnapshot(m raft.Message, dir string) {
	r.mu.Lock()
	r.snaps = append(r.snaps, dir)
	r.mu.Unlock()
	r.signal()
}

func (r *recorder) ReportUnreachable(raft.NodeID) {
	r.mu.Lock()
	r.unreachable++
	r.mu.Unlock()
}

func (r *recorder) ReportSnapshot(_ raft.NodeID, failed bool) {
	r.mu.Lock()
	r.reports = append(r.reports, failed)
	r.mu.Unlock()
	r.signal()
}

func (r *recorder) wait(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		r.mu.Lock()
		ok := cond()
		r.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-r.notify:
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatal("timed out")
		}
	}
}

func newPair(t *testing.T, tlsA, tlsB *tls.Config) (*Transport, *Transport, *recorder, *recorder) {
	t.Helper()
	ra, rb := newRecorder(), newRecorder()
	a, err := New(Config{ID: "a", Listen: "127.0.0.1:0", TLS: tlsA, Handler: ra, SnapshotDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(Config{ID: "b", Listen: "127.0.0.1:0", TLS: tlsB, Handler: rb, SnapshotDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	a.AddPeer("b", b.Addr().String())
	b.AddPeer("a", a.Addr().String())
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b, ra, rb
}

func testPKI(t *testing.T, ids ...string) map[string]*tls.Config {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	now := time.Now()
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "raft test ca"},
		NotBefore:             now.Add(-24 * time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	out := map[string]*tls.Config{}
	for i, id := range ids {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i + 2)),
			Subject:      pkix.Name{CommonName: id},
			DNSNames:     []string{id},
			NotBefore:    now.Add(-24 * time.Hour),
			NotAfter:     now.Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		out[id] = &tls.Config{
			Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
			RootCAs:      pool,
			ClientCAs:    pool,
			MinVersion:   tls.VersionTLS13,
		}
	}
	return out
}

func TestSendReceive(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "mtls"}[secure], func(t *testing.T) {
			var ca, cb *tls.Config
			if secure {
				pki := testPKI(t, "a", "b")
				ca, cb = pki["a"], pki["b"]
			}
			a, b, ra, rb := newPair(t, ca, cb)
			var sent []raft.Message
			for i := range 100 {
				sent = append(sent, raft.Message{Type: raft.MsgApp, From: "a", To: "b", Term: 1, Index: uint64(i),
					Entries: []raft.Entry{{Index: uint64(i + 1), Term: 1, Data: bytes.Repeat([]byte{byte(i)}, i)}}})
			}
			a.Send(sent)
			rb.wait(t, func() bool { return len(rb.msgs) == len(sent) })
			for i, m := range rb.msgs {
				if m.Index != uint64(i) || !bytes.Equal(m.Entries[0].Data, sent[i].Entries[0].Data) {
					t.Fatalf("message %d arrived out of order or damaged", i)
				}
			}
			b.Send([]raft.Message{{Type: raft.MsgAppResp, From: "b", To: "a", Term: 1, Index: 100}})
			ra.wait(t, func() bool { return len(ra.msgs) == 1 })
		})
	}
}

func TestTLSRejectsForeignIdentity(t *testing.T) {
	pki := testPKI(t, "a", "b")
	rogue := testPKI(t, "a")
	a, b, _, rb := newPair(t, rogue["a"], pki["b"])
	a.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: "a", To: "b", Term: 1}})
	time.Sleep(300 * time.Millisecond)
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if len(rb.msgs) != 0 {
		t.Fatal("accepted a certificate from another CA")
	}
	_ = b
}

func TestTLSIdentityMustMatchHello(t *testing.T) {
	pki := testPKI(t, "a", "b", "c")
	rb := newRecorder()
	b, err := New(Config{ID: "b", Listen: "127.0.0.1:0", TLS: pki["b"], Handler: rb, SnapshotDir: t.TempDir(), Peers: map[raft.NodeID]string{"a": "127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	c, err := New(Config{ID: "a", Listen: "127.0.0.1:0", TLS: pki["c"], Handler: newRecorder(), SnapshotDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.AddPeer("b", b.Addr().String())
	c.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: "a", To: "b", Term: 1}})
	time.Sleep(300 * time.Millisecond)
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if len(rb.msgs) != 0 {
		t.Fatal("node c got in by claiming to be a")
	}
}

func TestUnknownDialerOfferedAfterIdentityCheck(t *testing.T) {
	pki := testPKI(t, "a", "b", "c")
	offered := make(chan string, 8)
	b, err := New(Config{ID: "b", Listen: "127.0.0.1:0", TLS: pki["b"], Handler: newRecorder(), SnapshotDir: t.TempDir(),
		UnknownPeer: func(id raft.NodeID, addr string) { offered <- string(id) + "=" + addr }})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	dial := func(cert, addr string) {
		d, err := New(Config{ID: "a", Listen: "127.0.0.1:0", Advertise: addr, TLS: pki[cert], Handler: newRecorder(), SnapshotDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { d.Close() })
		d.AddPeer("b", b.Addr().String())
	}
	dial("c", "10.0.0.3:7000")
	time.Sleep(300 * time.Millisecond)
	dial("a", "10.0.0.1:7000")
	select {
	case got := <-offered:
		if got != "a=10.0.0.1:7000" {
			t.Fatalf("offered %s", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the dialer with a matching certificate was not offered")
	}
}

func TestSpoofedSenderDropped(t *testing.T) {
	a, _, _, rb := newPair(t, nil, nil)
	a.Send([]raft.Message{
		{Type: raft.MsgHeartbeat, From: "a", To: "b", Term: 1},
		{Type: raft.MsgHeartbeat, From: "c", To: "b", Term: 9},
		{Type: raft.MsgHeartbeat, From: "a", To: "b", Term: 2},
	})
	rb.wait(t, func() bool { return len(rb.msgs) >= 1 })
	time.Sleep(100 * time.Millisecond)
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for _, m := range rb.msgs {
		if m.From != "a" {
			t.Fatalf("delivered a message from %q over a's connection", m.From)
		}
	}
}

func TestHeartbeatCoalescing(t *testing.T) {
	tr := &Transport{cfg: Config{MaxQueueMsgs: 100, MaxQueueBytes: 1 << 20, Handler: newRecorder()}}
	p := &peer{t: tr, id: "b", notify: make(chan struct{}, 1), hb: -1, hbResp: -1}
	for i := range 5 {
		p.enqueue(raft.Message{Type: raft.MsgHeartbeat, To: "b", Commit: uint64(i)})
		p.enqueue(raft.Message{Type: raft.MsgApp, To: "b", Index: uint64(i)})
	}
	q := p.take()
	if len(q) != 6 || q[0].Type != raft.MsgHeartbeat || q[0].Commit != 4 {
		t.Fatalf("queue %+v", q)
	}
}

func TestQueueOverflowReportsUnreachable(t *testing.T) {
	rec := newRecorder()
	tr := &Transport{cfg: Config{MaxQueueMsgs: 2, MaxQueueBytes: 1 << 20, Handler: rec}}
	p := &peer{t: tr, id: "b", notify: make(chan struct{}, 1), hb: -1, hbResp: -1}
	for i := range 4 {
		p.enqueue(raft.Message{Type: raft.MsgApp, To: "b", Index: uint64(i)})
	}
	if len(p.take()) != 2 || rec.unreachable != 2 {
		t.Fatalf("unreachable reports %d", rec.unreachable)
	}
}

func TestReconnect(t *testing.T) {
	ra := newRecorder()
	a, err := New(Config{ID: "a", Listen: "127.0.0.1:0", Handler: ra, SnapshotDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	rb := newRecorder()
	b, err := New(Config{ID: "b", Listen: "127.0.0.1:0", Handler: rb, SnapshotDir: t.TempDir(), Peers: map[raft.NodeID]string{"a": a.Addr().String()}})
	if err != nil {
		t.Fatal(err)
	}
	addr := b.Addr().String()
	a.AddPeer("b", addr)
	a.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: "a", To: "b", Term: 1}})
	rb.wait(t, func() bool { return len(rb.msgs) == 1 })
	b.Close()
	rb2 := newRecorder()
	var b2 *Transport
	for range 50 {
		if b2, err = New(Config{ID: "b", Listen: addr, Handler: rb2, SnapshotDir: t.TempDir()}); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	b2.AddPeer("a", a.Addr().String())
	rb2.wait(t, func() bool {
		a.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: "a", To: "b", Term: 2}})
		return len(rb2.msgs) > 0
	})
}

func writeSnapshotFiles(t *testing.T, sizes ...int) string {
	t.Helper()
	dir := t.TempDir()
	rng := mrand.New(mrand.NewPCG(1, 2))
	for i, n := range sizes {
		b := make([]byte, n)
		for j := range b {
			b[j] = byte(rng.Uint32())
		}
		name := filepath.Join(dir, "file"+string(rune('a'+i)))
		if i%2 == 1 {
			name = filepath.Join(dir, "sub", "deeper", "file"+string(rune('a'+i)))
			if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(name, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func sameDirs(t *testing.T, a, b string) {
	t.Helper()
	ma, _ := Manifest(a)
	mb, _ := Manifest(b)
	if len(ma) != len(mb) {
		t.Fatalf("manifests differ: %v vs %v", ma, mb)
	}
	for i := range ma {
		if ma[i] != mb[i] {
			t.Fatalf("file %s differs", ma[i].Name)
		}
	}
}

var snapMsg = raft.Message{Type: raft.MsgSnap, From: "a", To: "b", Term: 3,
	Snapshot: &raft.SnapshotMeta{Index: 42, Term: 3, Conf: raft.ConfState{Voters: []raft.NodeID{"a", "b"}}}}

func TestSnapshotTransfer(t *testing.T) {
	pki := testPKI(t, "a", "b")
	a, _, ra, rb := newPair(t, pki["a"], pki["b"])
	src := writeSnapshotFiles(t, 3*chunkSize+123, 0, 17)
	a.SendSnapshot(snapMsg, src)
	ra.wait(t, func() bool { return len(ra.reports) == 1 })
	if ra.reports[0] {
		t.Fatal("transfer reported as failed")
	}
	rb.wait(t, func() bool { return len(rb.snaps) == 1 })
	sameDirs(t, src, rb.snaps[0])
}

func TestSnapshotResumesFromReceivedBytes(t *testing.T) {
	a, b, ra, rb := newPair(t, nil, nil)
	src := writeSnapshotFiles(t, 2*chunkSize, 5000)
	dst := IncomingDir(b.cfg.SnapshotDir, *snapMsg.Snapshot)
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "filea"), make([]byte, chunkSize), 0o644); err != nil {
		t.Fatal(err)
	}
	a.SendSnapshot(snapMsg, src)
	ra.wait(t, func() bool { return len(ra.reports) == 1 })
	if !ra.reports[0] {
		t.Fatal("wrong bytes kept from a previous attempt were not detected")
	}
	a.SendSnapshot(snapMsg, src)
	ra.wait(t, func() bool { return len(ra.reports) == 2 })
	if ra.reports[1] {
		t.Fatal("second attempt failed")
	}
	rb.wait(t, func() bool { return len(rb.snaps) == 1 })
	sameDirs(t, src, rb.snaps[0])
}

func TestSnapshotFileNames(t *testing.T) {
	for _, name := range []string{"data", "log-000/000000001.data", "a/b/c"} {
		if !validName(name) {
			t.Errorf("rejected %q", name)
		}
	}
	for _, name := range []string{"", ".", "..", "../x", "a/../../x", "/abs", "a//b", "a/./b", `a\b`, "C:x", "a/"} {
		if validName(name) {
			t.Errorf("accepted %q", name)
		}
	}
}
