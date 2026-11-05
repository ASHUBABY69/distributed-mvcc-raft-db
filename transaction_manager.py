"""
transaction_manager.py -- AUTHORED BY SKYLIE, not hand-written.

Generated from the dictated English source in transaction_manager.skylie
by translator.py's translate(). Two-phase commit (2PC) across multiple
shards, each wrapping its own mvcc_store.py MVCCStore -- same design as
the original hand-written reference (kept alongside as
transaction_manager_handwritten_reference.py): phase 1 (PREPARE) asks
every shard touched by a transaction to durably stage its writes; only
once every shard says yes does phase 2 (COMMIT) apply them for real. If
even one shard refuses, every shard aborts.

Differences from the hand-written reference, each a real Skylie
engine/vocabulary limit, not an oversight:
  - TransactionStatus's four enum members are plain strings
    ("pending"/"prepared"/"committed"/"aborted") -- Skylie has no enum
    support.
  - Coordinator.begin() takes txn_id as a caller-supplied argument
    rather than generating "txn-N" itself -- Skylie has no string-
    concatenation/str()-cast primitive yet to turn a counter into text.
  - Shard.commit() on an unstaged txn_id raises a plain KeyError (a
    dict[] lookup failing), not a hand-raised ValueError with a custom
    message -- Skylie has no "raise" statement. Both reject the call
    rather than silently no-op-ing.
  - No participant-failure timeout and no coordinator-failure recovery
    log, same deliberate scope limit the original file stated.
"""

class Transaction:
    def __init__(self, txn_id, writes_by_shard, status):
        self.txn_id = txn_id
        self.writes_by_shard = writes_by_shard
        self.status = status

class Shard:
    def __init__(self, shard_id, store, always_refuse, staged):
        self.shard_id = shard_id
        self.store = store
        self.always_refuse = always_refuse
        self.staged = staged
    def prepare(self, txn_id, writes):
        if self.always_refuse == True:
            return False
        staged_local = self.staged
        staged_local[txn_id] = writes
        return True
    def commit(self, txn_id, version):
        staged_local = self.staged
        commit_writes = staged_local[txn_id]
        del staged_local[txn_id]
        store_local = self.store
        for key, value in commit_writes:
            store_local.put(key, value, version)
    def abort(self, txn_id):
        staged_local = self.staged
        if txn_id in staged_local:
            del staged_local[txn_id]

class TransactionCoordinator:
    def __init__(self, transactions):
        self.transactions = transactions
    def begin(self, txn_id, writes_by_shard):
        transactions_local = self.transactions
        txn = Transaction(txn_id, writes_by_shard, "pending")
        transactions_local[txn_id] = txn
        return txn_id
    def status_of(self, txn_id):
        transactions_local = self.transactions
        txn = transactions_local[txn_id]
        return txn.status
    def run(self, txn_id, shards, commit_version):
        transactions_local = self.transactions
        txn = transactions_local[txn_id]
        writes_by_shard_local = txn.writes_by_shard
        participants = list()
        for sid, writes in writes_by_shard_local.items():
            if len(writes) > 0:
                participants.append(sid)
        votes = dict()
        for sid in participants:
            shard_x = shards[sid]
            writes_for_sid = writes_by_shard_local[sid]
            vote = shard_x.prepare(txn_id, writes_for_sid)
            votes[sid] = vote
        all_yes = True
        for sid, vote in votes.items():
            if vote == False:
                all_yes = False
        if all_yes == True:
            txn.status = "prepared"
            for sid in participants:
                shard_y = shards[sid]
                shard_y.commit(txn_id, commit_version)
            txn.status = "committed"
            return True
        for sid in participants:
            shard_z = shards[sid]
            shard_z.abort(txn_id)
        txn.status = "aborted"
        return False
