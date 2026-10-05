package transport

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/acneism/raft"
)

const logEvery = 30 * time.Second

var (
	ErrClosed  = errors.New("transport: closed")
	errRemoved = errors.New("transport: the peer says this node was removed from the cluster")
	errHangUp  = errors.New("transport: the peer closed the connection")
)

type Handler interface {
	Receive(m raft.Message)
	ReceiveSnapshot(m raft.Message, dir string)
	ReportUnreachable(id raft.NodeID)
	ReportSnapshot(id raft.NodeID, failed bool)
}

type Config struct {
	ID            raft.NodeID
	ClusterID     string
	Listen        string
	Advertise     string
	Peers         map[raft.NodeID]string
	TLS           *tls.Config
	Handler       Handler
	SnapshotDir   string
	MaxQueueMsgs  int
	MaxQueueBytes int64
	DialTimeout   time.Duration
	IOTimeout     time.Duration
	UnknownPeer   func(id raft.NodeID, addr string)
	IsRemoved     func(id raft.NodeID) bool
	OnRemoved     func(by raft.NodeID)
	Logger        *slog.Logger
}

type Transport struct {
	cfg    Config
	ln     net.Listener
	peers  map[raft.NodeID]*peer
	done   chan struct{}
	wg     sync.WaitGroup
	mu     sync.Mutex
	conns  map[net.Conn]raft.NodeID
	snaps  map[raft.NodeID]bool
	logMu  sync.Mutex
	logged map[string]time.Time
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
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
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
		cfg:    cfg,
		ln:     ln,
		peers:  map[raft.NodeID]*peer{},
		done:   make(chan struct{}),
		conns:  map[net.Conn]raft.NodeID{},
		snaps:  map[raft.NodeID]bool{},
		logged: map[string]time.Time{},
	}
	for id, addr := range cfg.Peers {
		t.AddPeer(id, addr)
	}
	t.wg.Add(1)
	go t.accept()
	return t, nil
}

func (t *Transport) Addr() net.Addr { return t.ln.Addr() }

func (t *Transport) allow(key string) bool {
	t.logMu.Lock()
	defer t.logMu.Unlock()
	now := time.Now()
	if last, ok := t.logged[key]; ok && now.Sub(last) < logEvery {
		return false
	}
	if len(t.logged) >= 1024 {
		clear(t.logged)
	}
	t.logged[key] = now
	return true
}

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
	t.conns[c] = raft.None
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
	c, r, w, _, err := t.dialVersion(id, addr, kind)
	return c, r, w, err
}

func (t *Transport) dialVersion(id raft.NodeID, addr string, kind byte) (net.Conn, *bufio.Reader, *bufio.Writer, uint64, error) {
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
		return nil, nil, nil, 0, err
	}
	if !t.track(c) {
		return nil, nil, nil, 0, ErrClosed
	}
	w := bufio.NewWriterSize(c, 64<<10)
	c.SetWriteDeadline(time.Now().Add(t.cfg.IOTimeout))
	h := hello{kind: kind, from: string(t.cfg.ID), to: string(id), cluster: t.cfg.ClusterID}
	if kind == streamMessages {
		h.max, h.addr = protocolVersion, t.cfg.Advertise
	}
	if err := writeFrame(w, frameHello, h.encode()); err == nil {
		err = w.Flush()
	}
	if err != nil {
		t.untrack(c)
		return nil, nil, nil, 0, err
	}
	r := bufio.NewReaderSize(c, 64<<10)
	v := uint64(1)
	if kind == streamMessages {
		c.SetReadDeadline(time.Now().Add(min(t.cfg.IOTimeout, versionWait)))
		typ, p, err := readFrame(r)
		if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			t.untrack(c)
			return nil, nil, nil, 0, err
		}
		if err == nil && typ == frameRemoved {
			t.untrack(c)
			if t.cfg.OnRemoved != nil {
				t.cfg.OnRemoved(id)
			}
			return nil, nil, nil, 0, errRemoved
		}
		if err == nil && typ == frameVersion {
			d := decoder{b: p}
			if x := d.uvarint(); d.err == nil && x >= 1 {
				v = x
			}
		}
		c.SetReadDeadline(time.Time{})
	}
	return c, r, w, v, nil
}

func (t *Transport) accept() {
	defer t.wg.Done()
	for {
		c, err := t.ln.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			if t.allow("accept") {
				t.cfg.Logger.Warn("cannot accept connections", "err", err)
			}
			select {
			case <-t.done:
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
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
	remote := c.RemoteAddr().String()
	reject := func(msg string, args ...any) {
		if host, _, _ := net.SplitHostPort(remote); t.allow("accept " + host) {
			t.cfg.Logger.Warn(msg, append([]any{"remote", remote}, args...)...)
		}
	}
	r := bufio.NewReaderSize(c, 64<<10)
	p, err := expectFrame(r, frameHello)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			reject("rejected a connection", "err", err)
		}
		return
	}
	h, err := decodeHello(p)
	if err != nil || h.to != string(t.cfg.ID) {
		reject("rejected a connection with a bad hello", "to", h.to, "err", err)
		return
	}
	from := raft.NodeID(h.from)
	if t.cfg.ClusterID != "" && h.cluster != "" && h.cluster != t.cfg.ClusterID {
		reject("rejected a connection from another cluster", "from", from, "cluster", h.cluster)
		return
	}
	if t.cfg.ClusterID != "" && h.cluster == "" && t.allow("no cluster "+string(from)) {
		t.cfg.Logger.Warn("accepted a connection without a cluster ID", "from", from, "remote", remote)
	}
	if tc, ok := c.(*tls.Conn); ok {
		id, err := Identity(tc.ConnectionState())
		if err != nil || id != from {
			reject("rejected a connection whose certificate names another node", "from", from, "certificate", id, "err", err)
			return
		}
	}
	if t.peer(from) == nil && h.addr != "" && t.cfg.UnknownPeer != nil {
		t.cfg.UnknownPeer(from, h.addr)
	}
	if !t.inbound(c, from) {
		if h.kind == streamMessages && h.max >= 2 && t.cfg.IsRemoved != nil && t.cfg.IsRemoved(from) {
			w := bufio.NewWriterSize(c, 64)
			if writeFrame(w, frameRemoved, nil) == nil {
				w.Flush()
			}
			if t.allow("removed " + string(from)) {
				t.cfg.Logger.Info("told a removed node that it was removed", "from", from, "remote", remote)
			}
			return
		}
		reject("rejected a connection from an unknown node", "from", from, "addr", h.addr)
		return
	}
	if h.kind == streamMessages && h.max > 0 {
		w := bufio.NewWriterSize(c, 64)
		if writeFrame(w, frameVersion, appendUvarint(nil, min(h.max, protocolVersion))) != nil || w.Flush() != nil {
			return
		}
	}
	c.SetDeadline(time.Time{})
	switch h.kind {
	case streamMessages:
		t.receiveMessages(r, from)
	case streamSnapshot:
		t.receiveSnapshot(c, r, from)
	}
}

func (t *Transport) inbound(c net.Conn, from raft.NodeID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.peers[from] == nil {
		return false
	}
	t.conns[c] = from
	return true
}

func (t *Transport) receiveMessages(r *bufio.Reader, from raft.NodeID) {
	drop := func(err error) {
		if t.allow("receive " + string(from)) {
			t.cfg.Logger.Warn("dropped a connection from a peer", "peer", from, "err", err)
		}
	}
	for {
		p, err := expectFrame(r, frameMessages)
		if errors.Is(err, errFrame) {
			drop(err)
		}
		if err != nil {
			return
		}
		msgs, err := decodeBatch(p)
		if err != nil {
			drop(err)
			return
		}
		for _, m := range msgs {
			if m.From != from || m.To != t.cfg.ID || m.Type == raft.MsgSnap {
				drop(fmt.Errorf("transport: unexpected %v from %s to %s", m.Type, m.From, m.To))
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
	t       *Transport
	id      raft.NodeID
	addr    string
	mu      sync.Mutex
	queue   []raft.Message
	bytes   int64
	hb      int
	hbResp  int
	spare   []raft.Message
	notify  chan struct{}
	stop    chan struct{}
	version atomic.Uint64
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
	var down time.Time
	for {
		addr := p.address()
		c, r, w, v, err := p.t.dialVersion(p.id, addr, streamMessages)
		if err != nil {
			if down.IsZero() {
				down = time.Now()
			}
			if !errors.Is(err, ErrClosed) && p.t.allow("dial "+string(p.id)) {
				p.t.cfg.Logger.Warn("cannot connect to a peer", "peer", p.id, "addr", addr, "err", err)
			}
			p.drop(p.take())
			select {
			case <-p.t.done:
				return
			case <-p.stop:
				return
			case <-time.After(backoff/2 + rand.N(backoff)):
			}
			backoff = min(2*backoff, time.Second)
			continue
		}
		if !down.IsZero() && p.t.allow("connect "+string(p.id)) {
			p.t.cfg.Logger.Info("connected to a peer", "peer", p.id, "addr", addr, "after", time.Since(down).Round(time.Millisecond))
		}
		down = time.Time{}
		p.version.Store(v)
		backoff = 20 * time.Millisecond
		hungUp := make(chan struct{})
		go func() {
			io.Copy(io.Discard, r)
			close(hungUp)
		}()
		err = p.stream(c, w, hungUp)
		p.t.untrack(c)
		<-hungUp
		if errors.Is(err, ErrClosed) {
			return
		}
		down = time.Now()
		if p.t.allow("send " + string(p.id)) {
			p.t.cfg.Logger.Warn("lost the connection to a peer", "peer", p.id, "addr", addr, "err", err)
		}
		select {
		case <-p.t.done:
			return
		case <-p.stop:
			return
		case <-time.After(backoff):
		}
	}
}

func (p *peer) stream(c net.Conn, w *bufio.Writer, hungUp <-chan struct{}) error {
	var buf []byte
	for {
		select {
		case <-p.t.done:
			return ErrClosed
		case <-p.stop:
			return ErrClosed
		case <-hungUp:
			return errHangUp
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
	p := &peer{t: t, id: id, addr: addr, notify: make(chan struct{}, 1), stop: make(chan struct{}), hb: -1, hbResp: -1}
	t.peers[id] = p
	t.wg.Add(1)
	go p.run()
}

func (t *Transport) RemovePeer(id raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p := t.peers[id]; p != nil {
		delete(t.peers, id)
		close(p.stop)
	}
	for c, from := range t.conns {
		if from == id {
			c.Close()
		}
	}
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
