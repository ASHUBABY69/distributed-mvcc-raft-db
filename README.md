# Distributed, MVCC-Backed Relational Database Engine with Raft Consensus

A small distributed database, built from the ground up: a versioned
key/value store with snapshot isolation (MVCC), a relational layer of
tables on top of it, atomic multi-table writes via two-phase commit, a
binary on-disk record format, and Raft consensus for replicating writes
across a cluster safely.

Three of the five core files were authored by **Skylie** — a
natural-language-to-Python transpiler built from scratch for this
project — from dictated English, not typed by hand. The other two
(`storage_format.py`, `raft_consensus.py`) are hand-written, because
they need things Skylie's grammar genuinely can't reach yet: raw binary
`struct` packing, real networking, and Python function values.

## Files

| File | Lines | Built by | What it does |
|---|---|---|---|
| `transaction_manager.py` | 100 | Skylie | Two-phase commit across shards — a multi-table write only commits if every shard involved agrees to prepare it. |
| `mvcc_store.py` | 88 | Skylie | Versioned key/value store. Every write is a new version, never an overwrite, so any past snapshot is still readable. |
| `relational_engine.py` | 129 | Skylie | Tables of rows, keyed by primary key, each backed by its own private `MVCCStore` for free snapshot isolation. |
| `storage_format.py` | 153 | hand-written | Binary encode/decode for one record and one page of records — what would actually hit disk. |
| `raft_consensus.py` | 412 | hand-written | Leader election, log replication, and the commit-safety rule that stops a new leader from "uncommitting" data a previous leader already called durable. |
| `raft_consensus_skylie_core.py` | — | Skylie (companion) | Not one of the five — a separate proof that Skylie can correctly author Raft's two subtlest rules (vote-granting, commit-safety) on its own, as pure logic with no networking. |

## How it was built

Each Skylie-authored file has a matching `.skylie` source file (the
dictated English) and its own `build_*.py` script that runs that source
through Skylie's `normalize_voice_input` → `translate` pipeline and
writes out the real `.py` file. Every one of them is checked against
real assertions mirroring the hand-written references' own test
scenarios before being treated as done.

Building these surfaced and fixed real bugs in Skylie itself — not
contrived ones, found by actually using the tool: a symbol-table
case-folding bug that corrupted a class declaration's capitalization, a
second case-folding bug that corrupted a constructor call referencing
that same class, and a parser bug where two attribute-write statements
back to back with no comma between them silently swallowed the second
statement's first word into the first statement's value. All three are
documented with root cause, fix, and a dedicated regression test in
Skylie's own test suite.

## Running it

Pure Python standard library, no dependencies. From this folder:

```
python3 transaction_manager.py
python3 mvcc_store.py
python3 relational_engine.py
python3 storage_format.py
python3 raft_consensus.py
```

Each file's own `if __name__ == "__main__":` block is its test suite —
every one prints `PASS` for each scenario and a final `ALL ... CHECKS
PASSED` line.

For the full system working together — a Raft-replicated cluster where
each node's state machine is a real `RelationalEngine`, a network
partition and recovery, cross-shard atomic commits and aborts, and a
live record round-tripped through the binary format:

```
python3 integration_test.py
```

## What's deliberately not included

Flagged honestly, not silently skipped — each one is a real limit of
Skylie's grammar at the time these files were built, not an oversight:

- No real networking in `raft_consensus.py`'s own core logic — the
  `send` function is pluggable by design; `SimulatedTransport` is the
  stand-in, swap it for real sockets/gRPC without touching anything
  above it.
- `Table.scan()` / `Table.select()` (filtering/querying beyond a single
  primary-key lookup) aren't implemented — they need a filter callable
  and a sort primitive, neither reachable in Skylie yet.
- No participant-failure timeout or coordinator-failure recovery log in
  `transaction_manager.py` — same scope limit the original design
  called out.
- `mvcc_store.py` doesn't reject an out-of-order version write (no
  `raise` in Skylie); `storage_format.py` persistence isn't wired into
  `mvcc_store.py` automatically (would need Skylie to call across
  files, which it can't yet — no `import`).

Every file's own module docstring has the full, specific list for that
file.
