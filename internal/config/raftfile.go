package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Peer struct {
	ID      int
	Address string
}

type ClusterConfig struct {
	Envs map[string][]Peer
}

func ParseRaftfile(path string) (*ClusterConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open raftfile: %w", err)
	}
	defer file.Close()

	cfg := &ClusterConfig{
		Envs: make(map[string][]Peer),
	}
	scanner := bufio.NewScanner(file)
	lineNum := 0
	currentEnv := "default"
	seen := make(map[string]map[int]bool)
	seen[currentEnv] = make(map[int]bool)

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			currentEnv = line[1 : len(line)-1]
			if _, exists := seen[currentEnv]; !exists {
				seen[currentEnv] = make(map[int]bool)
			}
			continue
		}

		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("raftfile:%d: expected 'ID ADDRESS', got %q", lineNum, line)
		}

		id, err := strconv.Atoi(fields[0])
		if err != nil {
			return nil, fmt.Errorf("raftfile:%d: invalid node ID %q: %w", lineNum, fields[0], err)
		}

		if id <= 0 {
			return nil, fmt.Errorf("raftfile:%d: node ID must be positive, got %d", lineNum, id)
		}

		if seen[currentEnv][id] {
			return nil, fmt.Errorf("raftfile:%d: duplicate node ID %d in env %s", lineNum, id, currentEnv)
		}
		seen[currentEnv][id] = true

		cfg.Envs[currentEnv] = append(cfg.Envs[currentEnv], Peer{
			ID:      id,
			Address: fields[1],
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("raftfile: read error: %w", err)
	}

	total := 0
	for _, peers := range cfg.Envs {
		total += len(peers)
	}
	if total == 0 {
		return nil, fmt.Errorf("raftfile: no peers defined")
	}

	return cfg, nil
}

func (c *ClusterConfig) GetEnv(env string) ([]Peer, error) {
	peers, ok := c.Envs[env]
	if !ok {
		if env == "default" && len(c.Envs) > 0 {
            for _, p := range c.Envs {
                return p, nil
            }
		}
		return nil, fmt.Errorf("environment %q not found", env)
	}
	return peers, nil
}

func (c *ClusterConfig) GetPeer(env string, id int) (Peer, bool) {
	peers, err := c.GetEnv(env)
	if err != nil {
		return Peer{}, false
	}
	for _, p := range peers {
		if p.ID == id {
			return p, true
		}
	}
	return Peer{}, false
}

func (c *ClusterConfig) GetOtherPeers(env string, selfID int) []Peer {
	peers, err := c.GetEnv(env)
	if err != nil {
		return nil
	}
	var others []Peer
	for _, p := range peers {
		if p.ID != selfID {
			others = append(others, p)
		}
	}
	return others
}
