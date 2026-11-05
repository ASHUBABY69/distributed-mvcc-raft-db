# Distributed, MVCC-Backed Relational Database Engine with Raft Consensus

A distributed relational database built from scratch in Python, with no
external dependencies: a versioned key/value store with snapshot
isolation (MVCC), a relational layer of tables on top of it, atomic
multi-table writes via two-phase commit, a binary on-disk record
format, and Raft consensus for replicating writes safely across a
cluster.

## Core Architecture & Features

- **Multi-Version Concurrency Control (MVCC)** — `mvcc_store.py`.
  Every write appends a new version instead of overwriting, so any past
  snapshot stays readable without blocking other reads or writes.
- **Two-Phase Commit (2PC) Transactions** — `transaction_manager.py`.
  Coordinates writes across multiple shards: a transaction only
  commits if every shard involved agrees to prepare it; if any one
  refuses, all shards roll back.
- **Relational Storage Engine** — `relational_engine.py`. Tables keyed
  by primary key, each backed by its own private MVCC store, giving
  every table snapshot isolation for free.
- **Binary On-Disk Format** — `storage_format.py`. Length-prefixed
  binary encoding for a single record and a page of records — the
  layer that would actually touch disk.
- **Raft Consensus Protocol** — `raft_consensus.py`. Leader election,
  log replication, and the commit-safety rule that stops a new leader
  from "uncommitting" data a previous leader already called durable.

Three of these five modules were generated from natural-language
descriptions using a transpiler I built myself (Skylie), then verified
against the same test scenarios as the hand-written reference
implementations. The other two needed things outside its current
reach — raw binary packing and real networking — so they're
hand-written.

## Repository Structure

| Module | Lines | Built with | Description |
|---|---|---|---|
| `transaction_manager.py` | ~100 | Skylie | Two-phase commit coordinator and shard transaction state. |
| `mvcc_store.py` | ~88 | Skylie | Append-only versioned key/value store. |
| `relational_engine.py` | ~129 | Skylie | Relational table layer on top of MVCC storage. |
| `storage_format.py` | ~153 | hand-written | Binary encode/decode for records and disk pages. |
| `raft_consensus.py` | ~412 | hand-written | Raft leader election, replication, and commit safety. |

## Quickstart & Verification

Pure Python standard library, no dependencies.

**Run each module's own test suite:**
```
python3 transaction_manager.py
python3 mvcc_store.py
python3 relational_engine.py
python3 storage_format.py
python3 raft_consensus.py
```

**Run the full distributed integration test** — a Raft-replicated
cluster under a simulated network partition, cross-shard 2PC commits
and aborts, and a live record round-tripped through the binary format:
```
python3 integration_test.py
```

## Honest scope

This is a from-scratch learning project, not a production database —
flagged plainly rather than glossed over:

- No real networking in Raft's core logic yet — `send` is a pluggable
  function by design, ready to swap in real sockets/gRPC without
  touching the consensus logic itself.
- No query filtering beyond a direct primary-key lookup (no
  `scan`/`select`).
- No participant-failure timeout or coordinator-failure recovery log
  in the transaction manager.
- Writes aren't durability-logged to disk automatically — the binary
  format and the live store aren't wired together yet.

## License

All rights reserved — this code is for viewing only. Contact me for
any other use.
