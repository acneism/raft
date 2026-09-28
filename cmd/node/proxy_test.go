package main

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
	"time"
)

type linkProxy struct {
	mu      sync.Mutex
	blocked map[[2]string]bool
	conns   map[[2]string][]net.Conn
	lns     []net.Listener
}

func newLinkProxy(t *testing.T) *linkProxy {
	lp := &linkProxy{blocked: map[[2]string]bool{}, conns: map[[2]string][]net.Conn{}}
	t.Cleanup(lp.close)
	return lp
}

func (lp *linkProxy) listen(t *testing.T, to, target string) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lp.mu.Lock()
	lp.lns = append(lp.lns, ln)
	lp.mu.Unlock()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go lp.serve(c, to, target)
		}
	}()
	return ln.Addr().String()
}

func readHello(r io.Reader) ([]byte, string, error) {
	frame := make([]byte, 9)
	if _, err := io.ReadFull(r, frame); err != nil {
		return nil, "", err
	}
	n := binary.LittleEndian.Uint32(frame)
	if n < 2 || n > 1<<16 {
		return nil, "", errors.New("proxy: bad hello")
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, "", err
	}
	l, k := binary.Uvarint(body[2:])
	if k <= 0 || uint64(len(body)-2-k) < l {
		return nil, "", errors.New("proxy: bad hello")
	}
	return append(frame, body...), string(body[2+k : 2+k+int(l)]), nil
}

func (lp *linkProxy) serve(in net.Conn, to, target string) {
	in.SetReadDeadline(time.Now().Add(5 * time.Second))
	hello, from, err := readHello(in)
	in.SetReadDeadline(time.Time{})
	if err != nil {
		in.Close()
		return
	}
	link := [2]string{from, to}
	out, err := net.DialTimeout("tcp", target, time.Second)
	if err != nil {
		in.Close()
		return
	}
	lp.mu.Lock()
	if lp.blocked[link] {
		lp.mu.Unlock()
		in.Close()
		out.Close()
		return
	}
	lp.conns[link] = append(lp.conns[link], in, out)
	lp.mu.Unlock()
	if _, err := out.Write(hello); err == nil {
		go func() {
			io.Copy(in, out)
			in.Close()
		}()
		io.Copy(out, in)
	}
	in.Close()
	out.Close()
	lp.mu.Lock()
	lp.conns[link] = slices.DeleteFunc(lp.conns[link], func(c net.Conn) bool { return c == in || c == out })
	lp.mu.Unlock()
}

func (lp *linkProxy) block(from, to string) {
	lp.mu.Lock()
	defer lp.mu.Unlock()
	link := [2]string{from, to}
	lp.blocked[link] = true
	for _, c := range lp.conns[link] {
		c.Close()
	}
	delete(lp.conns, link)
}

func (lp *linkProxy) heal() {
	lp.mu.Lock()
	defer lp.mu.Unlock()
	clear(lp.blocked)
}

func (lp *linkProxy) close() {
	lp.mu.Lock()
	defer lp.mu.Unlock()
	for _, ln := range lp.lns {
		ln.Close()
	}
	for _, cs := range lp.conns {
		for _, c := range cs {
			c.Close()
		}
	}
}
