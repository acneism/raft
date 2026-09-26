package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempRaftfile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "Raftfile")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseRaftfile_Valid(t *testing.T) {
	path := writeTempRaftfile(t, `
[local]
1 127.0.0.1:8001
2 127.0.0.1:8002
3 127.0.0.1:8003
4 127.0.0.1:8004
5 127.0.0.1:8005

[prod]
1 prod-1:80
2 prod-2:80
3 prod-3:80
4 prod-4:80
5 prod-5:80
`)

	cfg, err := ParseRaftfile(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	peers, err := cfg.GetEnv("local")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(peers) != 5 {
		t.Fatalf("expected 5 peers, got %d", len(peers))
	}

	peersProd, err := cfg.GetEnv("prod")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(peersProd) != 5 {
		t.Fatalf("expected 5 peers, got %d", len(peersProd))
	}
}

func TestParseRaftfile_BackwardCompatibility(t *testing.T) {
	path := writeTempRaftfile(t, `
1 127.0.0.1:8001
2 127.0.0.1:8002
`)
	cfg, err := ParseRaftfile(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	peers, err := cfg.GetEnv("default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(peers))
	}
}

func TestParseRaftfile_DuplicateID(t *testing.T) {
	path := writeTempRaftfile(t, `
[local]
1 10.0.0.1:9000
1 10.0.0.2:9000
`)
	_, err := ParseRaftfile(path)
	if err == nil {
		t.Fatal("expected error for duplicate ID")
	}
}

func TestGetPeer(t *testing.T) {
	cfg := &ClusterConfig{
		Envs: map[string][]Peer{
			"local": {
				{ID: 1, Address: "a:1"},
				{ID: 2, Address: "b:2"},
			},
		},
	}

	p, ok := cfg.GetPeer("local", 1)
	if !ok || p.Address != "a:1" {
		t.Errorf("GetPeer(1) = %+v, %v", p, ok)
	}

	_, ok = cfg.GetPeer("local", 99)
	if ok {
		t.Error("GetPeer(99) should return false")
	}
}

func TestGetOtherPeers(t *testing.T) {
	cfg := &ClusterConfig{
		Envs: map[string][]Peer{
			"local": {
				{ID: 1, Address: "a:1"},
				{ID: 2, Address: "b:2"},
				{ID: 3, Address: "c:3"},
				{ID: 4, Address: "d:4"},
				{ID: 5, Address: "e:5"},
			},
		},
	}

	others := cfg.GetOtherPeers("local", 2)
	if len(others) != 4 {
		t.Fatalf("expected 4 other peers, got %d", len(others))
	}
	for _, p := range others {
		if p.ID == 2 {
			t.Error("GetOtherPeers should exclude selfID")
		}
	}
}

func TestParseRaftfile_SingleEnv(t *testing.T) {
	path := writeTempRaftfile(t, "[test]\n1 127.0.0.1:8001\n")
	cfg, err := ParseRaftfile(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if peers, err := cfg.GetEnv("test"); err != nil || len(peers) != 1 {
		t.Fatalf("peers %v, err %v", peers, err)
	}
}

func TestParseRaftfile_Empty(t *testing.T) {
	if _, err := ParseRaftfile(writeTempRaftfile(t, "[test]\n")); err == nil {
		t.Fatal("expected an error for a Raftfile without peers")
	}
}
