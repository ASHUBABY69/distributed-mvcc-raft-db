"""
Integration test: all 5 portfolio files wired together in one realistic
scenario -- a Raft-replicated cluster where each node's local state
machine is the actual RelationalEngine (built on the actual MVCCStore),
commands travel through the actual network/consensus layer
(raft_consensus.py's RaftNode + SimulatedTransport), a cross-shard write
goes through the actual 2PC TransactionCoordinator, and one live Record
is round-tripped through the actual binary storage_format.py encoding.

This is NOT a new file in the 5-file deliverable -- it's a check that
the 5 files, built independently (3 by Skylie, 2 hand-written), actually
compose correctly, with no code changes to any of them.
"""
from raft_consensus import build_cluster, Role
from relational_engine import RelationalEngine
from mvcc_store import MVCCStore
from transaction_manager import TransactionCoordinator, Shard
from storage_format import encode_record, decode_record, Record as StorageRecord


# ---------------------------------------------------------------------
# PART 1: a Raft-replicated relational engine.
#
# Each cluster node gets its OWN RelationalEngine (its local "state
# machine"). A command proposed to the leader is an opaque string as far
# as raft_consensus.py is concerned (LogEntry.command: Any) -- here it's
# a small tuple describing one relational-engine call. Once an entry's
# index is <= a node's commit_index, that node applies it to its own
# engine and advances last_applied -- the standard Raft "replicated log
# -> deterministic state machine" pattern, applied for real.
# ---------------------------------------------------------------------
def apply_command(engine, command):
    kind = command[0]
    if kind == "create_table":
        engine.create_table(command[1])
    elif kind == "insert":
        _, table, pk, row, version = command
        engine.table(table).insert(pk, row, version)
    elif kind == "remove":
        _, table, pk, version = command
        engine.table(table).remove(pk, version)
    else:
        raise ValueError(f"unknown command {command!r}")


def commit_and_sync(leader_node):
    """Real Raft's commit_index propagates to followers with a one-round
    lag: an AppendEntries carries the leader's commit_index AS OF WHEN
    IT WAS SENT, and the leader only advances its OWN commit_index
    AFTER that round's sends go out. So a follower doesn't find out
    about a freshly-committed index until the NEXT round -- a heartbeat,
    in real deployments, sent on a timer regardless of new entries.
    Calling replicate_to_followers() twice back to back is exactly that:
    round 1 replicates the new entries and lets the LEADER cross the
    majority threshold; round 2 (empty, a heartbeat) is what actually
    carries that commit_index to every follower."""
    leader_node.replicate_to_followers()
    leader_node.replicate_to_followers()


def apply_committed(node, engine):
    """Walk this node's log from last_applied+1 up to commit_index,
    applying each entry's command to its local engine -- exactly the
    step real Raft deployments call 'applying to the state machine'."""
    while node.last_applied < node.commit_index:
        node.last_applied += 1
        entry = node.log[node.last_applied]
        if entry.command is not None:
            apply_command(engine, entry.command)


print("=== PART 1: propose/replicate/commit/apply through the real Raft + relational-engine stack ===")
ids = ["A", "B", "C"]
transport, nodes = build_cluster(ids)
by_id = {n.id: n for n in nodes}
engines = {nid: RelationalEngine({}) for nid in ids}

by_id["A"].on_election_timeout()
assert by_id["A"].role == Role.LEADER
leader = by_id["A"]
print(f"leader elected: {leader.id}, term {leader.current_term}")

leader.client_propose(("create_table", "users"))
leader.client_propose(("insert", "users", "u1", {"name": "Ada", "age": 30}, 1))
leader.client_propose(("insert", "users", "u2", {"name": "Lin", "age": 25}, 1))
commit_and_sync(leader)

for nid in ids:
    apply_committed(by_id[nid], engines[nid])

for nid in ids:
    users = engines[nid].table("users")
    row = users.get("u1", 1)
    assert row == {"name": "Ada", "age": 30}, (nid, row)
print("PASS -- all 3 nodes' independently-built RelationalEngines agree after replication:")
for nid in ids:
    print(f"  node {nid}: users.get('u1', 1) = {engines[nid].table('users').get('u1', 1)}")


print("\n=== PART 2: MVCC snapshot isolation holds ACROSS the network, not just locally ===")
leader.client_propose(("insert", "users", "u1", {"name": "Ada", "age": 31}, 5))
commit_and_sync(leader)
for nid in ids:
    apply_committed(by_id[nid], engines[nid])

for nid in ids:
    users = engines[nid].table("users")
    old = users.get("u1", 1)
    new = users.get("u1", 5)
    assert old == {"name": "Ada", "age": 30}, (nid, old)
    assert new == {"name": "Ada", "age": 31}, (nid, new)
print("PASS -- every replica sees BOTH the old snapshot (v1) and the new one (v5) correctly:")
for nid in ids:
    u = engines[nid].table("users")
    print(f"  node {nid}: u1@1={u.get('u1', 1)}, u1@5={u.get('u1', 5)}")


print("\n=== PART 3: network partition -- C is unreachable, cluster still commits, C catches up later ===")
drop_transport = transport  # reuse the same registered nodes/transport
real_send = drop_transport.send
def flaky_send(peer_id, kind, args):
    if peer_id == "C":
        return None  # C is unreachable from the leader's perspective
    return real_send(peer_id, kind, args)
leader._send = flaky_send

leader.client_propose(("insert", "users", "u3", {"name": "Omar", "age": 40}, 9))
before_c_applied = engines["C"].table("users")
commit_and_sync(leader)  # both rounds still drop C -- A/B reach full agreement, C hears nothing
for nid in ["A", "B"]:
    apply_committed(by_id[nid], engines[nid])
assert leader.commit_index >= 4, leader.commit_index
assert before_c_applied.get("u3", 9) is False, "C must NOT have u3 yet -- it never received it"
print(f"  while C is unreachable: A/B committed (commit_index={leader.commit_index}), C has not seen u3 yet")

leader._send = real_send  # network heals
commit_and_sync(leader)  # leader retries C with its still-backed-off next_index, then a heartbeat carries the commit_index
apply_committed(by_id["C"], engines["C"])
assert engines["C"].table("users").get("u3", 9) == {"name": "Omar", "age": 40}
print("PASS -- once the network heals, C automatically catches up to the same state as A and B:")
for nid in ids:
    print(f"  node {nid}: users.get('u3', 9) = {engines[nid].table('users').get('u3', 9)}")


print("\n=== PART 4: cross-shard atomicity via the real 2PC TransactionCoordinator ===")
shard_users = Shard("users_shard", MVCCStore({}), False, {})
shard_orders = Shard("orders_shard", MVCCStore({}), False, {})
shards = {"users_shard": shard_users, "orders_shard": shard_orders}
coordinator = TransactionCoordinator({})

coordinator.begin("txn-1", {
    "users_shard": [("u1", {"name": "Ada", "balance": 90})],
    "orders_shard": [("o1", {"item": "widget", "buyer": "u1"})],
})
ok = coordinator.run("txn-1", shards, 1)
assert ok is True
assert shard_users.store.get("u1", 1) == {"name": "Ada", "balance": 90}
assert shard_orders.store.get("o1", 1) == {"item": "widget", "buyer": "u1"}
print("PASS -- txn-1 committed on BOTH shards atomically:")
print(f"  users_shard: {shard_users.store.get('u1', 1)}")
print(f"  orders_shard: {shard_orders.store.get('o1', 1)}")

shard_orders.always_refuse = True  # simulate the orders shard refusing to prepare
coordinator.begin("txn-2", {
    "users_shard": [("u1", {"name": "Ada", "balance": 40})],
    "orders_shard": [("o2", {"item": "gadget", "buyer": "u1"})],
})
ok2 = coordinator.run("txn-2", shards, 2)
assert ok2 is False
assert shard_users.store.get("u1", 2) == {"name": "Ada", "balance": 90}, "users_shard must NOT have the half-applied write"
print("PASS -- txn-2: orders_shard refused, so users_shard rolled back too (no partial write):")
print(f"  users_shard after abort still reads: {shard_users.store.get('u1', 2)}")


print("\n=== PART 5: a live Record, round-tripped through the real binary storage_format.py ===")
live_record = by_id["A"].log[1]  # the committed 'create_table' LogEntry is a real raft LogEntry, not a storage Record
# Build a storage-format Record from a REAL row that actually exists in one of the live MVCCStores used above.
raw = StorageRecord(key="users:u1", value=str(engines["A"].table("users").get("u1", 5)), version=5, deleted=False)
encoded = encode_record(raw)
decoded, consumed = decode_record(encoded)
assert decoded == raw, (decoded, raw)
assert consumed == len(encoded)
print(f"PASS -- {raw} -> {len(encoded)} bytes -> {decoded} (round-trips exactly)")


print("\n=== ALL 5 FILES VERIFIED WORKING TOGETHER ===")
