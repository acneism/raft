package transport

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/gliedabrennung/raft"
)

var ErrClosed = errors.New("transport: closed")

type Handler interface {
	Receive(m raft.Message)
	ReceiveSnapshot(m raft.Message, dir string)
	ReportUnreachable(id raft.NodeID)
	ReportSnapshot(id raft.NodeID, failed bool)
}

type Config struct {
	ID            raft.NodeID
	Listen        string
	Peers         map[raft.NodeID]string
	TLS           *tls.Config
	Handler       Handler
	SnapshotDir   string
	MaxQueueMsgs  int
	MaxQueueBytes int64
	DialTimeout   time.Duration
	IOTimeout     time.Duration
}

type Transport struct {
	cfg   Config
	ln    net.Listener
	peers map[raft.NodeID]*peer
	done  chan struct{}
	wg    sync.WaitGroup
	mu    sync.Mutex
	conns map[net.Conn]struct{}
	snaps map[raft.NodeID]bool
}

func New(cfg Config) (*Transport, error) {
	if cfg.MaxQueueMsgs <= 0 {
		cfg.MaxQueueMsgs = 4096
	}
	if cfg.MaxQueueBytes <= 0 {
		cfg.MaxQueueBytes = 64 << 20
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = time.Second
	}
	if cfg.IOTimeout <= 0 {
		cfg.IOTimeout = 10 * time.Second
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, err
	}
	if cfg.TLS != nil {
		sc := cfg.TLS.Clone()
		sc.ClientAuth = tls.RequireAndVerifyClientCert
		ln = tls.NewListener(ln, sc)
	}
	t := &Transport{
		cfg:   cfg,
		ln:    ln,
		peers: map[raft.NodeID]*peer{},
		done:  make(chan struct{}),
		conns: map[net.Conn]struct{}{},
		snaps: map[raft.NodeID]bool{},
	}
	for id, addr := range cfg.Peers {
		t.AddPeer(id, addr)
	}
	t.wg.Add(1)
	go t.accept()
	return t, nil
}

func (t *Transport) Addr() net.Addr { return t.ln.Addr() }

func (t *Transport) Close() error {
	select {
	case <-t.done:
		return nil
	default:
	}
	close(t.done)
	err := t.ln.Close()
	t.mu.Lock()
	for c := range t.conns {
		c.Close()
	}
	t.mu.Unlock()
	t.wg.Wait()
	return err
}

func (t *Transport) track(c net.Conn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	select {
	case <-t.done:
		c.Close()
		return false
	default:
	}
	t.conns[c] = struct{}{}
	return true
}

func (t *Transport) untrack(c net.Conn) {
	c.Close()
	t.mu.Lock()
	delete(t.conns, c)
	t.mu.Unlock()
}

func (t *Transport) Send(msgs []raft.Message) {
	for i := range msgs {
		if p := t.peer(msgs[i].To); p != nil {
			p.enqueue(msgs[i])
		}
	}
}

func (t *Transport) dial(id raft.NodeID, addr string, kind byte) (net.Conn, *bufio.Reader, *bufio.Writer, error) {
	d := net.Dialer{Timeout: t.cfg.DialTimeout, KeepAlive: 15 * time.Second}
	var c net.Conn
	var err error
	if t.cfg.TLS != nil {
		cc := t.cfg.TLS.Clone()
		cc.ServerName = string(id)
		c, err = (&tls.Dialer{NetDialer: &d, Config: cc}).Dial("tcp", addr)
	} else {
		c, err = d.Dial("tcp", addr)
	}
	if err != nil {
		return nil, nil, nil, err
	}
	if !t.track(c) {
		return nil, nil, nil, ErrClosed
	}
	w := bufio.NewWriterSize(c, 64<<10)
	c.SetWriteDeadline(time.Now().Add(t.cfg.IOTimeout))
	if err := writeFrame(w, frameHello, hello{kind: kind, from: string(t.cfg.ID), to: string(id)}.encode()); err == nil {
		err = w.Flush()
	}
	if err != nil {
		t.untrack(c)
		return nil, nil, nil, err
	}
	return c, bufio.NewReaderSize(c, 64<<10), w, nil
}

func (t *Transport) accept() {
	defer t.wg.Done()
	for {
		c, err := t.ln.Accept()
		if err != nil {
			return
		}
		if !t.track(c) {
			return
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			defer t.untrack(c)
			t.serve(c)
		}()
	}
}

func (t *Transport) serve(c net.Conn) {
	c.SetDeadline(time.Now().Add(t.cfg.IOTimeout))
	r := bufio.NewReaderSize(c, 64<<10)
	p, err := expectFrame(r, frameHello)
	if err != nil {
		return
	}
	h, err := decodeHello(p)
	if err != nil || h.to != string(t.cfg.ID) {
		return
	}
	from := raft.NodeID(h.from)
	if tc, ok := c.(*tls.Conn); ok {
		id, err := Identity(tc.ConnectionState())
		if err != nil || id != from {
			return
		}
	}
	if t.peer(from) == nil {
		return
	}
	c.SetDeadline(time.Time{})
	switch h.kind {
	case streamMessages:
		t.receiveMessages(r, from)
	case streamSnapshot:
		t.receiveSnapshot(c, r, from)
	}
}

func (t *Transport) receiveMessages(r *bufio.Reader, from raft.NodeID) {
	for {
		p, err := expectFrame(r, frameMessages)
		if err != nil {
			return
		}
		msgs, err := decodeBatch(p)
		if err != nil {
			return
		}
		for _, m := range msgs {
			if m.From != from || m.To != t.cfg.ID || m.Type == raft.MsgSnap {
				return
			}
			t.cfg.Handler.Receive(m)
		}
	}
}

func Identity(cs tls.ConnectionState) (raft.NodeID, error) {
	if len(cs.PeerCertificates) == 0 {
		return raft.None, errors.New("transport: no peer certificate")
	}
	c := cs.PeerCertificates[0]
	if len(c.DNSNames) > 0 {
		return raft.NodeID(c.DNSNames[0]), nil
	}
	if c.Subject.CommonName != "" {
		return raft.NodeID(c.Subject.CommonName), nil
	}
	return raft.None, errors.New("transport: certificate has no node identity")
}

type peer struct {
	t      *Transport
	id     raft.NodeID
	addr   string
	mu     sync.Mutex
	queue  []raft.Message
	bytes  int64
	hb     int
	hbResp int
	spare  []raft.Message
	notify chan struct{}
}

func msgSize(m *raft.Message) int64 {
	n := int64(64)
	for i := range m.Entries {
		n += 24 + int64(len(m.Entries[i].Data))
	}
	return n
}

func (p *peer) enqueue(m raft.Message) {
	p.mu.Lock()
	switch {
	case m.Type == raft.MsgHeartbeat && p.hb >= 0:
		p.queue[p.hb] = m
	case m.Type == raft.MsgHeartbeatResp && p.hbResp >= 0:
		p.queue[p.hbResp] = m
	case len(p.queue) >= p.t.cfg.MaxQueueMsgs || p.bytes >= p.t.cfg.MaxQueueBytes:
		p.mu.Unlock()
		if m.Type == raft.MsgApp {
			p.t.cfg.Handler.ReportUnreachable(p.id)
		}
		return
	default:
		switch m.Type {
		case raft.MsgHeartbeat:
			p.hb = len(p.queue)
		case raft.MsgHeartbeatResp:
			p.hbResp = len(p.queue)
		}
		p.queue = append(p.queue, m)
		p.bytes += msgSize(&m)
	}
	p.mu.Unlock()
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

func (p *peer) take() []raft.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	q := p.queue
	p.queue, p.spare = p.spare[:0], nil
	p.bytes, p.hb, p.hbResp = 0, -1, -1
	return q
}

func (p *peer) recycle(q []raft.Message) {
	clear(q)
	p.mu.Lock()
	if p.spare == nil {
		p.spare = q[:0]
	}
	p.mu.Unlock()
}

func (p *peer) drop(q []raft.Message) {
	for i := range q {
		if q[i].Type == raft.MsgApp {
			p.t.cfg.Handler.ReportUnreachable(p.id)
			return
		}
	}
}

func (p *peer) run() {
	defer p.t.wg.Done()
	backoff := 20 * time.Millisecond
	for {
		c, _, w, err := p.t.dial(p.id, p.address(), streamMessages)
		if err != nil {
			p.drop(p.take())
			select {
			case <-p.t.done:
				return
			case <-time.After(backoff/2 + rand.N(backoff)):
			}
			backoff = min(2*backoff, time.Second)
			continue
		}
		backoff = 20 * time.Millisecond
		err = p.stream(c, w)
		p.t.untrack(c)
		if errors.Is(err, ErrClosed) {
			return
		}
		select {
		case <-p.t.done:
			return
		case <-time.After(backoff):
		}
	}
}

func (p *peer) stream(c net.Conn, w *bufio.Writer) error {
	var buf []byte
	for {
		select {
		case <-p.t.done:
			return ErrClosed
		case <-p.notify:
		}
		q := p.take()
		if len(q) == 0 {
			continue
		}
		buf = appendBatch(buf[:0], q)
		c.SetWriteDeadline(time.Now().Add(p.t.cfg.IOTimeout))
		err := writeFrame(w, frameMessages, buf)
		if err == nil {
			err = w.Flush()
		}
		if err != nil {
			p.drop(q)
			return fmt.Errorf("transport: send to %s: %w", p.id, err)
		}
		p.recycle(q)
	}
}

func (t *Transport) AddPeer(id raft.NodeID, addr string) {
	if id == t.cfg.ID {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if p := t.peers[id]; p != nil {
		p.mu.Lock()
		p.addr = addr
		p.mu.Unlock()
		return
	}
	select {
	case <-t.done:
		return
	default:
	}
	p := &peer{t: t, id: id, addr: addr, notify: make(chan struct{}, 1), hb: -1, hbResp: -1}
	t.peers[id] = p
	t.wg.Add(1)
	go p.run()
}

func (t *Transport) peer(id raft.NodeID) *peer {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.peers[id]
}

func (p *peer) address() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.addr
}
