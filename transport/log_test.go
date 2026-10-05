package transport

import (
	"bytes"
	"crypto/tls"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func TestLogsRejectedCertificateOnBothSides(t *testing.T) {
	pki := testPKI(t, "a", "b")
	rogue := testPKI(t, "a")
	var la, lb logBuf
	b, err := New(Config{ID: "b", Listen: "127.0.0.1:0", TLS: pki["b"], Handler: newRecorder(), SnapshotDir: t.TempDir(),
		Logger: slog.New(slog.NewTextHandler(&lb, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ca := &tls.Config{Certificates: rogue["a"].Certificates, RootCAs: pki["a"].RootCAs, MinVersion: tls.VersionTLS13}
	a, err := New(Config{ID: "a", Listen: "127.0.0.1:0", TLS: ca, Handler: newRecorder(), SnapshotDir: t.TempDir(),
		Logger: slog.New(slog.NewTextHandler(&la, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.AddPeer("b", b.Addr().String())
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(la.String(), "certificate") || !strings.Contains(lb.String(), "certificate") {
		if time.Now().After(deadline) {
			t.Fatalf("dialer logged:\n%s\nacceptor logged:\n%s", la.String(), lb.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(time.Second)
	if n := strings.Count(la.String(), "level=WARN"); n != 1 {
		t.Fatalf("dialer logged %d warnings for one failing peer:\n%s", n, la.String())
	}
	if n := strings.Count(lb.String(), "level=WARN"); n != 1 {
		t.Fatalf("acceptor logged %d warnings for one failing dialer:\n%s", n, lb.String())
	}
}
