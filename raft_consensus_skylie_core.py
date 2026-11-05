"""
raft_consensus_skylie_core.py -- AUTHORED BY SKYLIE, not hand-written.

A companion to raft_consensus.py, not a replacement for it. Generated
from dictated English (raft_consensus_skylie_core.skylie) by
translator.py's translate(), via the full normalize_voice_input
pipeline -- the same path run_engine_v2.py always uses. Confirmed
working end-to-end live in the author's own running engine: constructing
a node and calling on_election_timeout() correctly flips it to
"candidate", bumps its term to 1, and has it vote for itself.

Proves Skylie can correctly author the two genuinely subtle pieces of
Raft that raft_consensus.py's own module docstring calls out:
  - handle_request_vote's vote-granting rule (already-voted check + the
    candidate's-log-must-be-at-least-as-up-to-date check).
  - can_commit_index: a majority match is not enough on its own -- the
    matched entry must also be from the leader's own CURRENT term, or
    the commit is refused. ("the subtle part most people skip.")

Deliberately out of scope here -- real engine/vocabulary limits, not
oversights (raft_consensus.py covers all of this; nothing below
contradicts it):
  - No networking. Skylie has no function-as-value, so there's no way
    to plug in a `send` callable. Every method here is pure state
    transition / decision logic a driver outside this file would call.
  - No handle_append_entries / log truncation+replication -- needs list
    slicing and extend(), neither available in Skylie's grammar.
  - No randomized election timers -- on_election_timeout() is a pure
    state transition, called manually; timing is the driver's job here
    exactly as it is in raft_consensus.py.
  - majority_threshold is a constructor argument (work out "how many
    nodes make a majority" once, up front) rather than computed from a
    peer count -- Skylie has no confirmed division/multiplication
    primitive.
"""

class LogEntry:
    def __init__(self, term, index, command):
        self.term = term
        self.index = index
        self.command = command

class VoteReply:
    def __init__(self, term, granted):
        self.term = term
        self.granted = granted

class RaftNode:
    def __init__(self, id, current_term, voted_for, role, commit_index, last_log_index, last_log_term, log, majority_threshold):
        self.id = id
        self.current_term = current_term
        self.voted_for = voted_for
        self.role = role
        self.commit_index = commit_index
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term
        self.log = log
        self.majority_threshold = majority_threshold
    def on_election_timeout(self):
        self.role = "candidate"
        cur_term = self.current_term
        new_term = cur_term + 1
        self.current_term = new_term
        my_id = self.id
        self.voted_for = my_id
    def become_follower(self, new_term):
        self.current_term = new_term
        self.role = "follower"
        self.voted_for = False
    def become_leader(self):
        self.role = "leader"
    def handle_request_vote(self, candidate_term, candidate_id, candidate_last_log_index, candidate_last_log_term):
        my_term = self.current_term
        if candidate_term < my_term:
            reply = VoteReply(my_term, False)
            return reply
        if candidate_term > my_term:
            self.become_follower(candidate_term)
        my_voted_for = self.voted_for
        has_voted = True
        if my_voted_for == False:
            has_voted = False
        already_voted_elsewhere = False
        if has_voted == True:
            if my_voted_for == candidate_id:
                already_voted_elsewhere = False
            else:
                already_voted_elsewhere = True
        my_last_term = self.last_log_term
        my_last_index = self.last_log_index
        up_to_date = False
        if candidate_last_log_term > my_last_term:
            up_to_date = True
        if candidate_last_log_term == my_last_term:
            if candidate_last_log_index >= my_last_index:
                up_to_date = True
        grant = True
        if already_voted_elsewhere == True:
            grant = False
        if up_to_date == False:
            grant = False
        if grant == True:
            self.voted_for = candidate_id
            term_now = self.current_term
            reply = VoteReply(term_now, True)
            return reply
        term_now = self.current_term
        reply = VoteReply(term_now, False)
        return reply
    def client_propose(self, command):
        my_role = self.role
        if my_role == "leader":
            cur_last_index = self.last_log_index
            new_index = cur_last_index + 1
            my_term = self.current_term
            entry = LogEntry(my_term, new_index, command)
            log_local = self.log
            log_local.append(entry)
            self.last_log_index = new_index
            self.last_log_term = my_term
            return True
        return False
    def count_at_least(self, values, threshold):
        total = 0
        for v in values:
            if v >= threshold:
                total = total + 1
        return total
    def can_commit_index(self, candidate_index, match_values):
        votes = self.count_at_least(match_values, candidate_index)
        needed = self.majority_threshold
        has_majority = False
        if votes >= needed:
            has_majority = True
        if has_majority == False:
            return False
        log_local = self.log
        entry = log_local[candidate_index]
        entry_term = entry.term
        my_term = self.current_term
        if entry_term == my_term:
            return True
        return False
