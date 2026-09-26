# Raft for BitKV

A Raft implementation in Go built to replace hashicorp/raft in [BitKV](https://github.com/acneism/BitKV).
It is tuned for a state machine that keeps its own data on disk, and it is checked by a deterministic
simulation with fault injection rather than by integration tests alone.

```bash
go get github.com/acneism/raft
```

## Status

| Stage | Report |
|---|---|
| 0. Audit of the learning project this repository started from | [docs/audit.md](docs/audit.md) |
| 1. Deterministic core and simulator | [docs/stage1.md](docs/stage1.md) |
| 2. Log, transport and node | [docs/stage2.md](docs/stage2.md) |
| 3. BitKV integration | [docs/stage3.md](docs/stage3.md) |
| 4. Raft index in Bitcask records, exact durable index | next |
| 5. Membership changes, leadership transfer, ReadIndex | planned |

BitKV runs on this library in its `own-raft` branch with `-raft-engine=own`; its whole test suite passes on
both engines.

## Features

- **Deterministic core.** The Raft state machine does no IO, starts no goroutines and never reads the clock.
  It implements elections with PreVote and CheckQuorum, vote retries and a pre-vote tie-break against split
  votes, fast log backtracking, pipelined replication with byte and message limits, and snapshots.
- **Leader writes in parallel.** AppendEntries leave before the leader's own fsync; the leader counts itself
  towards the quorum only after that fsync. A round takes max(leader fsync, network + follower fsync) instead of
  their sum.
- **Segmented log.** CRC32C records chained across the log, preallocated segments, logical compaction to any
  index, HardState in two slots that never overwrite the last synced state, fsync outside the log lock.
  Works on Linux and Windows.
- **Transport.** Framed TCP with a hand-written binary codec (fuzz-tested), mutual TLS with the node ID taken
  from the certificate, per-peer queues with heartbeat coalescing, and a separate connection for snapshot
  transfers that resume after a disconnect. A snapshot is a directory of files.
- **Node.** `Propose` returns the entry's index and term without waiting for IO, `Wait` always completes,
  leadership events are never lost, a state machine that keeps its own data on disk is replayed only from its
  durable index, and an interrupted snapshot restore is repeated on restart. Ticks follow the wall clock, so
  coarse OS timers do not stretch the election timeout.
- **Deterministic simulation.** Partitions, message loss, duplication and delay, crashes with torn writes, full
  and slow disks, and clock skew, with the Raft safety invariants and commit durability checked after every
  step. 6.4·10⁸ steps without violations.

## Performance

BitKV's replicated benchmarks: three nodes on one machine and one disk, 128 concurrent writers, thousands of
operations per second (median of three runs; details in [docs/stage3.md](docs/stage3.md)).

| Scenario | Windows, hashicorp → this | Linux (WSL2), hashicorp → this |
|---|---|---|
| SET, fsync | 18.3 → **36.3** | 7.5 → **16.6** |
| SET, no fsync | 30.8 → **56.0** | 45.1 → **56.7** |
| INCR of one key, fsync | 17.7 → **36.0** | 7.0 → **17.6** |
| INCR of one key, no fsync | 27.7 → **47.3** | 41.2 → **57.1** |

## Using the Library

Implement a state machine:

```go
type StateMachine interface {
	Apply(entries []raft.Entry) error
	DurableIndex() uint64
	Snapshot(dir string) (raft.SnapshotMeta, error)
	Restore(src node.SnapshotSource) error
}
```

`Apply` gets committed entries in order from one goroutine; entries of type `raft.EntryNoop` carry no data.
`DurableIndex` is the index up to which the state machine has its state on disk: after a restart only the
entries after it are replayed. `Snapshot` writes files into `dir` and returns the applied index; `Restore`
replaces the state with a snapshot directory.

Then run a node:

```go
n, err := node.Open(node.Config{
	ID:           "n1",
	Dir:          "data/n1",
	Peers:        map[raft.NodeID]string{"n1": "10.0.0.1:7000", "n2": "10.0.0.2:7000", "n3": "10.0.0.3:7000"},
	StateMachine: fsm,
	PreVote:      true,
	CheckQuorum:  true,
})
if err != nil {
	return err
}
defer n.Close()

p, err := n.Propose(command)
if err != nil {
	return err
}
switch err := n.Wait(ctx, p); {
case err == nil:
case errors.Is(err, node.ErrLost):
case errors.Is(err, node.ErrUnknown):
}
```

- **`Propose`** returns `node.ErrNotLeader` unless the node is the leader and has applied the no-op of its term.
- **`ProposeIn(term, data)`** also refuses when the term has changed since the caller looked, which makes
  speculative writes built on that term's state safe.
- **`Wait`** returns one of:
  - `nil` — the entry is applied;
  - `ErrLost` — another entry won its index;
  - `ErrUnknown` — leadership was lost first;
  - `ErrClosed` — the node was closed.
- **`Events()`** delivers every change of term, leader and readiness in order.
- **`Config.TLS`** enables mutual TLS.
- **`Config.NoSync`** disables fsync of log segments.

## Project Structure

- `.` (package `raft`): the core state machine.
- `wal/`: the segmented log and HardState storage.
- `transport/`: TCP transport and snapshot transfer.
- `node/`: runs the core with the log, the transport and a state machine.
- `sim/`: the deterministic cluster simulator.
- `cmd/node/`: a demo key-value node over HTTP, and the `kill -9` test harness.
- `internal/config/`: `Raftfile` parsing.
- `internal/kvfsm/`: the demo key-value state machine.

## Demo Node

Run the whole `[local]` cluster from the `Raftfile` in one process:

```bash
go run ./cmd/node --all --env local
```

Or one node per terminal:

```bash
go run ./cmd/node --id 1 --env local
```

Flags:

- **`--election-timeout`** — election timeout, 1 s by default; followers wait 1–2 of it before campaigning.
- **`--dir`** — data directory, `data` by default.
- **`--http` / `--http-offset`** — address of the HTTP API. By default the API listens on the Raft port plus
  1000, so node 1 of `[local]` answers on port 9001.
- **`--unsafe-no-fsync`** — do not fsync log segments.

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
go test ./sim -run TestSim$ -args -sim.seed=4 -sim.steps=3000000
go test ./cmd/node -run TestKillCycles -timeout 60m -args -kill.cycles=200
go test ./node -run TestRoundTime -args -round.n=2000
go test ./node -run TestFailoverTime -args -failover.n=60
go test ./transport -run '^$' -fuzz FuzzDecodeMessage -fuzztime 30s
```

- **Simulator.** The second command replays one seed of the simulator; a failure report always names its seed.
- **Linux without Go.** Cross-compile test binaries with `GOOS=linux go test -c` and run them in WSL.
- **`kill -9` harness.** Takes a prebuilt node binary with `-kill.bin`.

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

## License

Apache License 2.0, see [LICENSE](LICENSE).
