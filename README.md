# raft

[![CI](https://github.com/acneism/raft/actions/workflows/ci.yml/badge.svg)](https://github.com/acneism/raft/actions/workflows/ci.yml)

Raft consensus for Go, built for state machines that keep their own data on disk.

```bash
go get github.com/acneism/raft
```

## Features

- **Deterministic core.** No IO, no goroutines, no clock; elections with PreVote and CheckQuorum, pipelined
  replication, snapshots.
- **Parallel leader fsync.** AppendEntries leave before the leader's own fsync, so a round takes the longer of the
  two fsyncs, not their sum.
- **Segmented log.** CRC-chained records, logical compaction to any index, crash-safe HardState; Linux and Windows.
- **Transport.** Framed TCP with a binary codec, mutual TLS, resumable snapshot transfer; peers negotiate the protocol
  version, so a cluster can be upgraded one node at a time. Failed dials, rejected connections and TLS errors go to
  the node's `Logger`, at most once per 30 seconds for each peer.
- **Node.** `Propose` returns the index and term without waiting for IO, `Wait` always completes, leadership events
  are never lost, and replay after a restart starts at the state machine's durable index. `Status` reports the log
  bounds, the latest snapshot and, on the leader, the replication progress of every member.
- **Snapshots on demand.** The log is compacted behind the state machine's durable index without a snapshot; a
  snapshot is taken only when a follower falls behind the compacted log.
- **Linearizable reads.** `ReadIndex` confirms leadership with one heartbeat round instead of a log write, batches
  concurrent reads and works on followers. With `LeaseReads` the leader answers from its lease without a round,
  assuming node clocks run at rates that differ by at most `MaxClockDrift`.
- **Leadership transfer.** `TransferLeadership` brings the target up to date and hands leadership over without waiting
  for an election timeout. Catching up and the election get an election timeout each, and a failed transfer says
  which of them ran out or that another node won.
- **Membership changes.** A new node joins as a learner, catches up from the log or a snapshot and is promoted to
  voter; members are removed one change at a time. The configuration, with member addresses, travels in the log.
- **Simulation.** A deterministic cluster simulator with network, disk and clock faults checks the Raft safety
  invariants after every step and the client history for linearizability with
  [Porcupine](https://github.com/anishathalye/porcupine).

## Usage

Implement a state machine:

```go
type StateMachine interface {
	Apply(entries []raft.Entry) error
	DurableIndex() uint64
	Snapshot(dir string) (raft.SnapshotMeta, error)
	Restore(src node.SnapshotSource) error
}
```

`DurableIndex` is the last index the state machine has on disk: the node replays the log after it on restart and
compacts the log up to it, keeping `TrailingEntries` more. It may lag `Apply`, for example until a background fsync:
the node reads it after every applied batch and once per election timeout. `Snapshot` writes the applied state into
`dir` and returns its index, which must not be below `DurableIndex`.

Run a node:

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
err = n.Wait(ctx, p)
```

`Wait` returns `nil` once the entry is applied, `node.ErrLost` if another entry took its index, `node.ErrUnknown` if
leadership was lost first, and `node.ErrClosed` after `Close`.

Read linearizably on any node:

```go
index, err := n.ReadIndex(ctx)
if err != nil {
	return err
}
if err := n.WaitApplied(ctx, index); err != nil {
	return err
}
value := fsm.Get(key)
```

`ReadIndex` returns `node.ErrNotLeader` when no leader could confirm the read; retrying is safe.

Add a node: start it with `Join: true` and `Peers` listing at least itself, then on the leader:

```go
err := n.AddLearner(ctx, "n4", "10.0.0.4:7000")
err = n.Promote(ctx, "n4")
err = n.Remove(ctx, "n1")
```

The members dial the new node once they add it, and the node learns their addresses from the connections it accepts
before it has a configuration. `Promote` waits until the learner has caught up. Stop a removed node: running on with its old configuration it can
disturb the cluster.

## Demo

A key-value node over HTTP. Run a three-node cluster in one process:

```bash
go run ./cmd/node --all --peers 1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003
curl -X PUT http://localhost:9001/kv/greeting -d 'hello'
curl -X POST http://localhost:9001/kv/greeting/append -d ', world'
curl 'http://localhost:9001/kv/greeting?consistent=1'
```

Or one node per process: the same `--peers` everywhere and `--id 1`, `--id 2`, `--id 3`. The HTTP API listens on the
Raft port plus 1000. A plain `GET` reads the local state machine; `?consistent=1` confirms the read with
`ReadIndex` first.

`--lease-reads` serves consistent reads from the leader's lease.

`POST /members/{id}?addr=host:port`, `POST /members/{id}/promote` and `DELETE /members/{id}` change the membership
on the leader; start a new node with `--join`. `POST /transfer/{id}` hands leadership over.

## Testing

```bash
go test ./...
go test ./sim -run TestSim$ -timeout 60m -args -sim.steps=10000000 -sim.seeds=8
go test ./cmd/node -run TestKillCycles -timeout 60m -args -kill.cycles=200
go test ./sim -run TestSimLinearizable$ -timeout 60m -args -sim.lin.steps=10000000 -sim.seeds=8
go test ./cmd/node -run TestLinearizability -timeout 60m -args -lin.duration=5m
go test ./cmd/node -run TestMembership -timeout 60m -args -member.duration=5m
```

## License

Apache License 2.0, see [LICENSE](LICENSE).
