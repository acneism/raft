package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/acneism/raft"

	"github.com/acneism/raft/internal/kvfsm"
	"github.com/acneism/raft/node"
)

type options struct {
	dir        string
	httpAddr   string
	httpOffset int
	election   time.Duration
	noSync     bool
	join       bool
	leaseReads bool
	drift      float64
}

func main() {
	nodeID := flag.String("id", "", "ID of this node, one of --peers")
	runAll := flag.Bool("all", false, "run every node of --peers in this process")
	peerList := flag.String("peers", "", "all nodes including this one: id=host:port,id=host:port")
	var o options
	flag.StringVar(&o.dir, "dir", "data", "data directory")
	flag.StringVar(&o.httpAddr, "http", "", "HTTP API address of a single node (default: Raft port + http-offset)")
	flag.IntVar(&o.httpOffset, "http-offset", 1000, "HTTP API port offset from the Raft port")
	flag.DurationVar(&o.election, "election-timeout", time.Second, "Raft election timeout; followers wait 1-2 of it before campaigning")
	flag.BoolVar(&o.noSync, "unsafe-no-fsync", false, "do not fsync log segments")
	flag.BoolVar(&o.join, "join", false, "join an existing cluster listed in --peers instead of bootstrapping one")
	flag.BoolVar(&o.leaseReads, "lease-reads", false, "serve linearizable reads from the leader's lease without a heartbeat round")
	flag.Float64Var(&o.drift, "max-clock-drift", 0.1, "largest relative difference between node clock rates that lease reads tolerate")
	flag.Parse()

	peers, err := parsePeers(*peerList)
	if err != nil {
		fail("parse --peers", "err", err)
	}
	var ids []raft.NodeID
	switch {
	case *runAll:
		ids = slices.Sorted(maps.Keys(peers))
	case *nodeID == "":
		fail("specify --id or --all")
	default:
		if _, ok := peers[raft.NodeID(*nodeID)]; !ok {
			fail("--id is not in --peers", "id", *nodeID)
		}
		ids = []raft.NodeID{raft.NodeID(*nodeID)}
	}
	if len(ids) > 1 && o.httpAddr != "" {
		fail("--http needs a single node; use --http-offset with --all")
	}

	var servers []*server
	for _, id := range ids {
		s, err := start(id, peers, o)
		if err != nil {
			fail("start node", "id", id, "err", err)
		}
		servers = append(servers, s)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	for _, s := range servers {
		s.close()
	}
}

func fail(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

func parsePeers(s string) (map[raft.NodeID]string, error) {
	peers := map[raft.NodeID]string{}
	for part := range strings.SplitSeq(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("bad peer %q, want id=host:port", part)
		}
		if _, dup := peers[raft.NodeID(id)]; dup {
			return nil, fmt.Errorf("duplicate peer id %q", id)
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("peer %q: %w", id, err)
		}
		peers[raft.NodeID(id)] = addr
	}
	return peers, nil
}

type server struct {
	id   raft.NodeID
	n    *node.Node
	fsm  *kvfsm.FSM
	http *http.Server
}

func httpAddr(raftAddr string, offset int) (string, error) {
	host, port, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", err
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, strconv.Itoa(p+offset)), nil
}

func start(id raft.NodeID, peers map[raft.NodeID]string, o options) (*server, error) {
	dir := filepath.Join(o.dir, string(id))
	fsm, err := kvfsm.Open(filepath.Join(dir, "fsm"), 8)
	if err != nil {
		return nil, err
	}
	n, err := node.Open(node.Config{
		ID:            id,
		Dir:           filepath.Join(dir, "raft"),
		Peers:         peers,
		StateMachine:  fsm,
		TickInterval:  o.election / 100,
		PreVote:       true,
		CheckQuorum:   true,
		NoSync:        o.noSync,
		Join:          o.join,
		LeaseReads:    o.leaseReads,
		MaxClockDrift: o.drift,
		Logger:        slog.Default(),
	})
	if err != nil {
		fsm.Close()
		return nil, err
	}
	addr := o.httpAddr
	if addr == "" {
		if addr, err = httpAddr(peers[id], o.httpOffset); err != nil {
			n.Close()
			fsm.Close()
			return nil, err
		}
	}
	s := &server{id: id, n: n, fsm: fsm}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", s.status)
	mux.HandleFunc("GET /digest", s.digest)
	mux.HandleFunc("GET /kv/{key}", s.get)
	mux.HandleFunc("PUT /kv/{key}", s.put)
	mux.HandleFunc("POST /kv/{key}/append", s.append)
	mux.HandleFunc("POST /transfer/{id}", s.transfer)
	mux.HandleFunc("GET /members", s.members)
	mux.HandleFunc("POST /members/{id}", s.addLearner)
	mux.HandleFunc("POST /members/{id}/promote", s.promote)
	mux.HandleFunc("DELETE /members/{id}", s.remove)
	s.http = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		n.Close()
		fsm.Close()
		return nil, err
	}
	go s.http.Serve(ln)
	slog.Info("node started", "id", id, "raft", peers[id], "http", addr)
	return s, nil
}

func (s *server) close() {
	s.http.Close()
	s.n.Close()
	s.fsm.Close()
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func (s *server) status(w http.ResponseWriter, r *http.Request) {
	st := s.n.Status()
	writeJSON(w, map[string]any{
		"id":           s.id,
		"state":        st.State.String(),
		"term":         st.Term,
		"leader":       st.Lead,
		"commit":       st.Commit,
		"applied":      st.Applied,
		"last_index":   st.LastIndex,
		"leader_ready": st.LeaderReady,
	})
}

func (s *server) digest(w http.ResponseWriter, r *http.Request) {
	applied, digest := s.fsm.Digest()
	writeJSON(w, map[string]any{"applied": applied, "digest": fmt.Sprintf("%016x", digest), "keys": s.fsm.Len()})
}

func (s *server) replicate(w http.ResponseWriter, r *http.Request, cmd []byte) bool {
	p, err := s.n.Propose(cmd)
	if err != nil {
		w.Header().Set("X-Raft-Leader", string(s.n.Status().Lead))
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return false
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	switch err := s.n.Wait(ctx, p); {
	case err == nil:
		return true
	case errors.Is(err, node.ErrLost):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
	}
	return false
}

func (s *server) get(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Has("consistent") {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		index, err := s.n.ReadIndex(ctx)
		if err == nil {
			err = s.n.WaitApplied(ctx, index)
		}
		switch {
		case errors.Is(err, node.ErrNotLeader):
			w.Header().Set("X-Raft-Leader", string(s.n.Status().Lead))
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		case err != nil:
			http.Error(w, err.Error(), http.StatusGatewayTimeout)
			return
		}
	}
	v, ok := s.fsm.Get(r.PathValue("key"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	io.WriteString(w, v)
}

func (s *server) write(w http.ResponseWriter, r *http.Request, cmd func(key, value string) []byte) {
	value, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if s.replicate(w, r, cmd(r.PathValue("key"), string(value))) {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *server) put(w http.ResponseWriter, r *http.Request) { s.write(w, r, kvfsm.Put) }

func (s *server) append(w http.ResponseWriter, r *http.Request) { s.write(w, r, kvfsm.Append) }

func (s *server) transfer(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	switch err := s.n.TransferLeadership(ctx, raft.NodeID(r.PathValue("id"))); {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, node.ErrNotLeader):
		w.Header().Set("X-Raft-Leader", string(s.n.Status().Lead))
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, raft.ErrTransferTarget):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, node.ErrTransferFailed):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
	}
}

func (s *server) members(w http.ResponseWriter, r *http.Request) {
	cs := s.n.ConfState()
	writeJSON(w, map[string]any{"voters": cs.Voters, "learners": cs.Learners, "addrs": cs.Addrs})
}

func (s *server) membership(w http.ResponseWriter, r *http.Request, change func(context.Context) error) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	switch err := change(ctx); {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, node.ErrNotLeader):
		w.Header().Set("X-Raft-Leader", string(s.n.Status().Lead))
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, raft.ErrConfChangeInvalid):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, raft.ErrConfChangePending), errors.Is(err, node.ErrLost):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
	}
}

func (s *server) addLearner(w http.ResponseWriter, r *http.Request) {
	s.membership(w, r, func(ctx context.Context) error {
		return s.n.AddLearner(ctx, raft.NodeID(r.PathValue("id")), r.URL.Query().Get("addr"))
	})
}

func (s *server) promote(w http.ResponseWriter, r *http.Request) {
	s.membership(w, r, func(ctx context.Context) error { return s.n.Promote(ctx, raft.NodeID(r.PathValue("id"))) })
}

func (s *server) remove(w http.ResponseWriter, r *http.Request) {
	s.membership(w, r, func(ctx context.Context) error { return s.n.Remove(ctx, raft.NodeID(r.PathValue("id"))) })
}
