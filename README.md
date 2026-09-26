# Raft for BitKV

A Raft implementation in Go that is being built to replace hashicorp/raft in BitKV. The design and the plan are in
the specification; progress reports are in [docs/](docs).

## Features

- **Deterministic core.** The Raft state machine does no IO, starts no goroutines and never reads the clock.
  It implements elections with PreVote and CheckQuorum, fast log backtracking, pipelined replication with
  byte and message limits, and snapshots.
- **Leader writes in parallel.** AppendEntries leave before the leader's own fsync; the leader counts itself
  towards the quorum only after that fsync. A round takes max(leader fsync, network + follower fsync).
- **Segmented log.** CRC32C records chained across the log, preallocated segments, logical compaction to any
  index, HardState in two alternating slots. Works on Linux and Windows.
- **Transport.** Framed TCP with a hand-written binary codec, mutual TLS with the node ID taken from the
  certificate, per-peer queues with heartbeat coalescing, and a separate connection for snapshot transfers
  that resume after a disconnect.
- **Node.** `Propose` returns the entry's index and term without waiting for IO, `Wait` always completes,
  leadership events are never lost, and a state machine that keeps its own data on disk is replayed only from
  its durable index.
- **Deterministic simulation.** Partitions, message loss, duplication and delay, crashes with torn writes, full
  disks and clock skew, with the Raft safety invariants checked after every step.

## Project Structure

- `.` (package `raft`): the core state machine.
- `wal/`: the segmented log and HardState storage.
- `transport/`: TCP transport and snapshot transfer.
- `node/`: runs the core with the log, the transport and a state machine.
- `sim/`: the deterministic cluster simulator.
- `cmd/node/`: a demo key-value node over HTTP, and the `kill -9` test harness.
- `internal/config/`: `Raftfile` parsing.
- `internal/kvfsm/`: the demo key-value state machine.

## Quick Start

Run the whole `[local]` cluster from the `Raftfile` in one process:

```bash
go run ./cmd/node --all --env local
```

Or one node per terminal:

```bash
go run ./cmd/node --id 1 --env local
```

Each node serves its HTTP API on the Raft port plus `--http-offset` (1000 by default), so node 1 of the
`[local]` environment answers on port 9001.

## Using the API

```bash
curl http://localhost:9001/status
curl -X PUT http://localhost:9001/kv/greeting -d 'hello'
curl http://localhost:9001/kv/greeting
curl http://localhost:9001/digest
```

Writes must go to the leader; other nodes answer 503 with the leader's ID in the `X-Raft-Leader` header.
Reads are served from the local state machine and may be stale.

## Testing

```bash
go test ./...
go test ./sim -run TestSim$ -timeout 60m -args -sim.steps=10000000 -sim.seeds=8
go test ./cmd/node -run TestKillCycles -timeout 60m -args -kill.cycles=200
go test ./node -run TestRoundTime -args -round.n=2000
```

## Docker

```bash
docker build -t raft-node .
docker run -p 8001:8001 -p 9001:9001 raft-node --id 1 --env production
```

## Raftfile Example

```ini
[local]
1 127.0.0.1:8001
2 127.0.0.1:8002
3 127.0.0.1:8003

[production]
1 raft-1.internal:8001
2 raft-2.internal:8001
3 raft-3.internal:8001
```
