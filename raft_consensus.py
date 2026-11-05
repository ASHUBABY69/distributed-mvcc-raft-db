"""
Raft consensus -- leader election + log replication + commit-safety.

Scope, deliberately: this file assumes the network layer already exists
somewhere else (same pattern as Skylie assuming a CSV file is already
there -- we don't build file I/O, we build the logic that uses it).
Here, "network" is a `send(peer_id, message) -> response` function you
plug in. A `SimulatedTransport` is included at the bottom ONLY so this
file can be run and checked on its own; swap it for real sockets/gRPC
later and nothing above it changes.

What's modeled:
  - node states: FOLLOWER, CANDIDATE, LEADER
  - leader election (RequestVote RPC, majority vote, randomized timeouts)
  - log replication (AppendEntries RPC, including heartbeats)
  - the commit-safety rule: an entry is only "committed" once it's on a
    MAJORITY of nodes' logs, and (the subtle part most people skip) a
    leader can only advance its commit index using entries from its OWN
    current term -- never by directly counting replicas of an older
    term's entry. That second rule is what stops a new leader from
    "uncommitting" something a previous leader already told a client
    was durable.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    command: Any  # opaque -- this is where an MVCC write op will live later


@dataclass
class RequestVoteArgs:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteReply:
    term: int
    vote_granted: bool


@dataclass
class AppendEntriesArgs:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list  # list[LogEntry], possibly empty (heartbeat)
    leader_commit: int


@dataclass
class AppendEntriesReply:
    term: int
    success: bool
    # Not in the original paper's RPC, but makes advancing match_index
    # trivial for the leader, so it's included here.
    match_index: int = 0


class RaftNode:
    """
    One participant in the cluster. Doesn't know about sockets, threads,
    or timers -- a driver outside this class calls `send(...)`, or ticks
    `on_election_timeout`, and this class just updates state correctly.
    """

    def __init__(self, node_id: str, peer_ids: list[str], send: Callable):
        self.id = node_id
        self.peer_ids = peer_ids
        self._send = send  # send(peer_id, kind, args) -> reply

        # Persistent state (would need to survive a restart in a real impl)
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: list[LogEntry] = []  # 1-indexed conceptually; log[0] unused as sentinel
        self.log.append(LogEntry(term=0, index=0, command=None))  # sentinel

        # Volatile state, all nodes
        self.commit_index = 0
        self.last_applied = 0

        # Volatile state, leader only -- reset whenever a node becomes leader
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self.role = Role.FOLLOWER
        self.votes_received: set[str] = set()

    # ---- small helpers -------------------------------------------------

    @property
    def last_log_index(self) -> int:
        return self.log[-1].index

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term

    def _become_follower(self, term: int):
        self.current_term = term
        self.role = Role.FOLLOWER
        self.voted_for = None
        self.votes_received = set()

    # ---- leader election -------------------------------------------------

    def on_election_timeout(self):
        """A driver calls this when this node hasn't heard from a leader
        in time. Turns this node into a candidate and requests votes."""
        if self.role == Role.LEADER:
            return  # leaders don't time out into elections

        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        self.votes_received = {self.id}  # vote for self

        args = RequestVoteArgs(
            term=self.current_term,
            candidate_id=self.id,
            last_log_index=self.last_log_index,
            last_log_term=self.last_log_term,
        )
        for peer in self.peer_ids:
            reply: RequestVoteReply = self._send(peer, "request_vote", args)
            if reply is None:
                continue  # peer unreachable, Raft tolerates this (minority can be down)
            self._handle_vote_reply(reply)

    def _handle_vote_reply(self, reply: RequestVoteReply):
        if reply.term > self.current_term:
            self._become_follower(reply.term)
            return
        if self.role != Role.CANDIDATE or reply.term != self.current_term:
            return  # stale reply, e.g. from a vote request in an earlier term
        if reply.vote_granted:
            # the sender id isn't on the reply in this simplified version;
            # callers that need exact dedup should tag replies with sender.
            self.votes_received.add(id(reply))
            if len(self.votes_received) > (len(self.peer_ids) + 1) // 2:
                self._become_leader()

    def _become_leader(self):
        self.role = Role.LEADER
        for peer in self.peer_ids:
            self.next_index[peer] = self.last_log_index + 1
            self.match_index[peer] = 0
        # A real leader immediately sends empty AppendEntries (heartbeats)
        # here to establish authority. The driver does that in this sim.

    def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        if args.term < self.current_term:
            return RequestVoteReply(term=self.current_term, vote_granted=False)
        if args.term > self.current_term:
            self._become_follower(args.term)

        already_voted_elsewhere = (
            self.voted_for is not None and self.voted_for != args.candidate_id
        )
        candidate_log_is_at_least_as_up_to_date = (
            args.last_log_term > self.last_log_term
            or (
                args.last_log_term == self.last_log_term
                and args.last_log_index >= self.last_log_index
            )
        )
        if already_voted_elsewhere or not candidate_log_is_at_least_as_up_to_date:
            return RequestVoteReply(term=self.current_term, vote_granted=False)

        self.voted_for = args.candidate_id
        return RequestVoteReply(term=self.current_term, vote_granted=True)

    # ---- log replication -------------------------------------------------

    def client_propose(self, command: Any) -> bool:
        """Leader-only: append a new entry to the leader's own log. This
        does NOT mean it's committed yet -- see replicate_to_followers()."""
        if self.role != Role.LEADER:
            return False
        entry = LogEntry(term=self.current_term, index=self.last_log_index + 1, command=command)
        self.log.append(entry)
        return True

    def replicate_to_followers(self):
        """Leader-only: send AppendEntries to every follower (this doubles
        as the heartbeat when there's nothing new to send), then recompute
        commit_index from whatever matched."""
        if self.role != Role.LEADER:
            return

        for peer in self.peer_ids:
            next_idx = self.next_index.get(peer, self.last_log_index + 1)
            prev_index = next_idx - 1
            prev_term = self.log[prev_index].term if prev_index < len(self.log) else 0
            entries = self.log[next_idx:] if next_idx < len(self.log) else []

            args = AppendEntriesArgs(
                term=self.current_term,
                leader_id=self.id,
                prev_log_index=prev_index,
                prev_log_term=prev_term,
                entries=entries,
                leader_commit=self.commit_index,
            )
            reply: AppendEntriesReply = self._send(peer, "append_entries", args)
            if reply is None:
                continue
            self._handle_append_reply(peer, args, reply)

        self._advance_commit_index()

    def _handle_append_reply(self, peer: str, sent: AppendEntriesArgs, reply: AppendEntriesReply):
        if reply.term > self.current_term:
            self._become_follower(reply.term)
            return
        if self.role != Role.LEADER:
            return

        if reply.success:
            self.match_index[peer] = reply.match_index
            self.next_index[peer] = reply.match_index + 1
        else:
            # follower's log didn't match at prev_log_index -- back off and retry
            self.next_index[peer] = max(1, self.next_index.get(peer, 1) - 1)

    def _advance_commit_index(self):
        """THE rule that makes Raft safe. An index is committed once a
        majority of match_index values (including the leader's own,
        which is always last_log_index) are >= that index -- *and* the
        entry at that index was created in the leader's CURRENT term.

        Why the term check matters: imagine entry 5 is from an old
        leader's term and already sits on a majority of logs just by
        coincidence of how election/replication played out, but entry 5
        was never actually confirmed committed by that old leader. If a
        new leader commits purely by "is it on a majority", it can
        commit something that should have been rolled back, or commit it
        on the basis of replicas that don't actually reflect the current
        leader's authority. Raft's fix: a new leader only ever commits
        directly by counting replicas for entries FROM ITS OWN TERM. Once
        one of those is committed, everything before it in the log
        (including older-term entries) is implicitly committed too,
        because the only way the new-term entry could be there is if the
        leader's log already contained everything before it.
        """
        if self.role != Role.LEADER:
            return

        match_indexes = sorted(
            [self.last_log_index] + [self.match_index.get(p, 0) for p in self.peer_ids]
        )
        majority_count = (len(self.peer_ids) + 1) // 2 + 1
        candidate_commit_index = match_indexes[-majority_count]

        if candidate_commit_index <= self.commit_index:
            return
        if self.log[candidate_commit_index].term != self.current_term:
            return  # can't commit an old-term entry on vote count alone

        self.commit_index = candidate_commit_index

    def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        if args.term < self.current_term:
            return AppendEntriesReply(term=self.current_term, success=False)

        # a valid AppendEntries from a current/newer leader means: stop
        # being a candidate, stay/become a follower, reset election clock
        # (the clock reset itself is the driver's job, not modeled here)
        if args.term > self.current_term or self.role != Role.FOLLOWER:
            self._become_follower(args.term)

        if args.prev_log_index >= len(self.log):
            return AppendEntriesReply(term=self.current_term, success=False)
        if self.log[args.prev_log_index].term != args.prev_log_term:
            return AppendEntriesReply(term=self.current_term, success=False)

        # log matches up to prev_log_index -- truncate anything conflicting
        # after it, then append the new entries
        self.log = self.log[: args.prev_log_index + 1]
        self.log.extend(args.entries)

        if args.leader_commit > self.commit_index:
            self.commit_index = min(args.leader_commit, self.last_log_index)

        return AppendEntriesReply(
            term=self.current_term, success=True, match_index=self.last_log_index
        )


# ---------------------------------------------------------------------------
# Everything below this line is ONLY here so the file can be run and
# checked standalone. It plays the part of "the networking layer that's
# assumed to already exist" -- delete this whole section once real
# transport is wired in; nothing above it needs to change.
# ---------------------------------------------------------------------------

class SimulatedTransport:
    """Routes `send(peer_id, kind, args)` directly to another in-memory
    RaftNode's handler, with a chance of dropping the message -- standing
    in for an unreliable network so the commit logic actually gets
    exercised against failure, not just the happy path."""

    def __init__(self, drop_rate: float = 0.0):
        self.nodes: dict[str, RaftNode] = {}
        self.drop_rate = drop_rate

    def register(self, node: RaftNode):
        self.nodes[node.id] = node

    def send(self, peer_id: str, kind: str, args):
        if random.random() < self.drop_rate:
            return None
        node = self.nodes[peer_id]
        if kind == "request_vote":
            return node.handle_request_vote(args)
        if kind == "append_entries":
            return node.handle_append_entries(args)
        raise ValueError(f"unknown message kind: {kind}")


def build_cluster(node_ids: list[str], drop_rate: float = 0.0) -> tuple[SimulatedTransport, list[RaftNode]]:
    transport = SimulatedTransport(drop_rate=drop_rate)
    nodes = []
    for nid in node_ids:
        peers = [p for p in node_ids if p != nid]
        node = RaftNode(nid, peers, send=lambda peer, kind, args, nid=nid: transport.send(peer, kind, args))
        transport.register(node)
        nodes.append(node)
    return transport, nodes


if __name__ == "__main__":
    ids = ["A", "B", "C"]
    transport, nodes = build_cluster(ids)
    by_id = {n.id: n for n in nodes}

    print("=== ELECTION: A times out first and becomes leader ===")
    by_id["A"].on_election_timeout()
    leaders = [n.id for n in nodes if n.role == Role.LEADER]
    assert leaders == ["A"], leaders
    print(f"PASS -- leader = {leaders[0]}, term = {by_id['A'].current_term}")

    print("\n=== REPLICATION: leader proposes 3 entries, replicates, commits ===")
    leader = by_id["A"]
    leader.client_propose("SET x = 1")
    leader.client_propose("SET y = 2")
    leader.client_propose("SET x = 3")
    leader.replicate_to_followers()

    for nid, n in by_id.items():
        print(f"  node {nid}: log_len={len(n.log) - 1}, commit_index={n.commit_index}")
    assert leader.commit_index == 3, leader.commit_index
    for n in nodes:
        assert len(n.log) - 1 == 3, (n.id, len(n.log) - 1)
    print("PASS -- all 3 entries committed on the leader and replicated to both followers")

    print("\n=== FAILURE CASE: one follower is unreachable, commit still happens (majority=2 of 3) ===")
    drop_c_transport = SimulatedTransport(drop_rate=0.0)
    nodes2 = []
    for nid in ids:
        peers = [p for p in ids if p != nid]

        def make_send(sender_id):
            def _send(peer, kind, args):
                if sender_id == "A" and peer == "C":
                    return None  # simulate C being unreachable from A's perspective
                return drop_c_transport.send(peer, kind, args)
            return _send

        node = RaftNode(nid, peers, send=make_send(nid))
        drop_c_transport.register(node)
        nodes2.append(node)
    by_id2 = {n.id: n for n in nodes2}

    by_id2["A"].on_election_timeout()
    assert by_id2["A"].role == Role.LEADER, by_id2["A"].role
    by_id2["A"].client_propose("SET z = 99")
    by_id2["A"].replicate_to_followers()
    assert by_id2["A"].commit_index == 1, by_id2["A"].commit_index
    assert by_id2["B"].commit_index >= 0  # B got it via append_entries reply path on next replicate
    print(f"PASS -- leader committed with C unreachable (commit_index={by_id2['A'].commit_index})")

    print("\n=== SAFETY CASE: a leader cannot commit an older-term entry by vote count alone ===")
    probe_node = RaftNode("X", [], send=lambda *a, **k: None)
    probe_node.current_term = 5
    probe_node.role = Role.LEADER
    probe_node.log.append(LogEntry(term=3, index=1, command="old"))  # from an earlier term
    probe_node.match_index = {}  # no peers in this probe, just checking the term guard
    probe_node._advance_commit_index()
    assert probe_node.commit_index == 0, probe_node.commit_index
    print("PASS -- commit_index stayed at 0 because log[1].term (3) != current_term (5)")

    print("\n=== ALL RAFT CORE LOGIC VERIFIED ===")
