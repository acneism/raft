package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/acneism/raft"
	"github.com/acneism/raft/internal/config"
	"github.com/acneism/raft/internal/kvfsm"
	"github.com/acneism/raft/node"
)

type options struct {
	dir        string
	httpAddr   string
	httpOffset int
	election   time.Duration
	noSync     bool
}

func main() {
	nodeID := flag.Int("id", 0, "node ID from the Raftfile")
	runAll := flag.Bool("all", false, "run every node of the environment in this process")
	raftfile := flag.String("raftfile", "./Raftfile", "path to the Raftfile")
	env := flag.String("env", "local", "environment to load from the Raftfile")
	var o options
	flag.StringVar(&o.dir, "dir", "data", "data directory")
	flag.StringVar(&o.httpAddr, "http", "", "HTTP API address of a single node (default: Raft port + http-offset)")
	flag.IntVar(&o.httpOffset, "http-offset", 1000, "HTTP API port offset from the Raft port")
	flag.DurationVar(&o.election, "election-timeout", time.Second, "Raft election timeout; followers wait 1-2 of it before campaigning")
	flag.BoolVar(&o.noSync, "unsafe-no-fsync", false, "do not fsync log segments")
	flag.Parse()

	if !*runAll && *nodeID <= 0 {
		fail("specify --id <positive integer> or --all")
	}
	cfg, err := config.ParseRaftfile(*raftfile)
	if err != nil {
		fail("parse Raftfile", "path", *raftfile, "err", err)
	}
	members, err := cfg.GetEnv(*env)
	if err != nil {
		fail("environment not found in Raftfile", "env", *env, "err", err)
	}
	peers := map[raft.NodeID]string{}
	for _, p := range members {
		peers[raft.NodeID(strconv.Itoa(p.ID))] = p.Address
	}

	var servers []*server
	for _, p := range members {
		if !*runAll && p.ID != *nodeID {
			continue
		}
		s, err := start(raft.NodeID(strconv.Itoa(p.ID)), peers, o)
		if err != nil {
			fail("start node", "id", p.ID, "err", err)
		}
		servers = append(servers, s)
	}
	if len(servers) == 0 {
		fail("node ID not found in Raftfile", "id", *nodeID, "env", *env)
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
		ID:           id,
		Dir:          filepath.Join(dir, "raft"),
		Peers:        peers,
		StateMachine: fsm,
		TickInterval: o.election / 100,
		PreVote:      true,
		CheckQuorum:  true,
		NoSync:       o.noSync,
		Logger:       slog.Default(),
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

func (s *server) get(w http.ResponseWriter, r *http.Request) {
	v, ok := s.fsm.Get(r.PathValue("key"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	io.WriteString(w, v)
}

func (s *server) put(w http.ResponseWriter, r *http.Request) {
	value, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	p, err := s.n.Propose(kvfsm.Command(r.PathValue("key"), string(value)))
	if err != nil {
		w.Header().Set("X-Raft-Leader", string(s.n.Status().Lead))
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	switch err := s.n.Wait(ctx, p); {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, node.ErrLost):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
	}
}
