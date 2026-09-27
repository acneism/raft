package transport

import (
	"crypto/tls"
	"encoding/binary"
	"flag"
	"io"
	mrand "math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var snapSize = flag.Int64("snap.size", 0, "snapshot bytes in TestSnapshotThroughput; 0 skips it")

func mbps(n int64, d time.Duration) float64 { return float64(n) / (1 << 20) / d.Seconds() }

func writeBigSnapshot(t *testing.T, size int64) (string, time.Duration) {
	dir := t.TempDir()
	block := make([]byte, chunkSize)
	rng := mrand.New(mrand.NewPCG(3, 4))
	for i := range block {
		block[i] = byte(rng.Uint32())
	}
	start := time.Now()
	const files = 4
	for i := range files {
		f, err := os.Create(filepath.Join(dir, "data", string(rune('a'+i))))
		if os.IsNotExist(err) {
			os.MkdirAll(filepath.Join(dir, "data"), 0o755)
			f, err = os.Create(filepath.Join(dir, "data", string(rune('a'+i))))
		}
		if err != nil {
			t.Fatal(err)
		}
		for off := int64(0); off < size/files; off += int64(len(block)) {
			binary.LittleEndian.PutUint64(block, uint64(off)+uint64(i)<<40)
			if _, err := f.Write(block[:min(int64(len(block)), size/files-off)]); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	return dir, time.Since(start)
}

func loopbackCopy(t *testing.T, size int64, tlsCfg *tls.Config) time.Duration {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan int64)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- 0
			return
		}
		if tlsCfg != nil {
			c = tls.Server(c, tlsCfg)
		}
		n, _ := io.Copy(io.Discard, c)
		c.Close()
		done <- n
	}()
	var c net.Conn
	c, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if tlsCfg != nil {
		cfg := tlsCfg.Clone()
		cfg.ServerName = "b"
		c = tls.Client(c, cfg)
	}
	buf := make([]byte, chunkSize)
	start := time.Now()
	for sent := int64(0); sent < size; sent += int64(len(buf)) {
		if _, err := c.Write(buf); err != nil {
			t.Fatal(err)
		}
	}
	c.Close()
	<-done
	return time.Since(start)
}

type cutProxy struct {
	ln     net.Listener
	target string
	cutAt  int64
	passed atomic.Int64
	cut    atomic.Bool
	wg     sync.WaitGroup
	mu     sync.Mutex
	open   []net.Conn
}

func newCutProxy(t *testing.T, target string, cutAt int64) *cutProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &cutProxy{ln: ln, target: target, cutAt: cutAt}
	go p.serve()
	t.Cleanup(func() {
		ln.Close()
		p.mu.Lock()
		for _, c := range p.open {
			c.Close()
		}
		p.mu.Unlock()
		p.wg.Wait()
	})
	return p
}

func (p *cutProxy) serve() {
	for {
		in, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			out, err := net.Dial("tcp", p.target)
			if err != nil {
				in.Close()
				return
			}
			p.mu.Lock()
			p.open = append(p.open, in, out)
			p.mu.Unlock()
			go func() {
				io.Copy(in, out)
				in.Close()
			}()
			buf := make([]byte, 64<<10)
			var n int64
			for {
				k, err := in.Read(buf)
				if k > 0 {
					if _, werr := out.Write(buf[:k]); werr != nil {
						break
					}
					n += int64(k)
					p.passed.Add(int64(k))
					if n >= p.cutAt && p.cut.CompareAndSwap(false, true) {
						break
					}
				}
				if err != nil {
					break
				}
			}
			in.Close()
			out.Close()
		}()
	}
}

func TestSnapshotThroughput(t *testing.T) {
	if *snapSize == 0 {
		t.Skip("run with -snap.size=BYTES")
	}
	size := *snapSize / 4 * 4 / chunkSize * chunkSize
	pki := testPKI(t, "a", "b")
	src, write := writeBigSnapshot(t, size)
	t.Logf("snapshot of %d MiB", size>>20)
	t.Logf("disk write + fsync      %7.0f MiB/s", mbps(size, write))
	t.Logf("loopback TCP            %7.0f MiB/s", mbps(size, loopbackCopy(t, size, nil)))
	t.Logf("loopback TLS            %7.0f MiB/s", mbps(size, loopbackCopy(t, size, pki["b"])))
	start := time.Now()
	if _, err := Manifest(src); err != nil {
		t.Fatal(err)
	}
	t.Logf("manifest (CRC of files) %7.0f MiB/s", mbps(size, time.Since(start)))

	for _, secure := range []bool{false, true} {
		var ta, tb *tls.Config
		name := "TCP"
		if secure {
			ta, tb, name = pki["a"], pki["b"], "TLS"
		}
		a, _, ra, rb := newPair(t, ta, tb)
		start := time.Now()
		a.SendSnapshot(snapMsg, src)
		ra.wait(t, func() bool { return len(ra.reports) == 1 })
		el := time.Since(start)
		if ra.reports[0] {
			t.Fatalf("%s transfer failed", name)
		}
		rb.wait(t, func() bool { return len(rb.snaps) == 1 })
		t.Logf("snapshot over %s       %7.0f MiB/s (%v)", name, mbps(size, el), el.Round(time.Millisecond))
		sameDirs(t, src, rb.snaps[0])
	}

	a, b, ra, rb := newPair(t, nil, nil)
	proxy := newCutProxy(t, b.Addr().String(), size/2)
	a.AddPeer("b", proxy.ln.Addr().String())
	a.SendSnapshot(snapMsg, src)
	ra.wait(t, func() bool { return len(ra.reports) == 1 })
	if !ra.reports[0] {
		t.Fatal("transfer cut in the middle reported success")
	}
	firstBytes := proxy.passed.Load()
	start = time.Now()
	a.SendSnapshot(snapMsg, src)
	ra.wait(t, func() bool { return len(ra.reports) == 2 })
	el := time.Since(start)
	if ra.reports[1] {
		t.Fatal("resumed transfer failed")
	}
	rb.wait(t, func() bool { return len(rb.snaps) == 1 })
	sameDirs(t, src, rb.snaps[0])
	resent := proxy.passed.Load() - firstBytes
	t.Logf("cut after %d MiB; resume sent %d MiB in %v", firstBytes>>20, resent>>20, el.Round(time.Millisecond))
	if resent > size-firstBytes+size/10 {
		t.Fatalf("resume sent %d bytes, only %d were missing", resent, size-firstBytes)
	}
}
