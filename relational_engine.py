"""
relational_engine.py -- AUTHORED BY SKYLIE, not hand-written.

Generated from the dictated English source in relational_engine.skylie by
translator.py's translate(), via the full normalize_voice_input pipeline
-- the same path run_engine_v2.py always uses. Confirmed working
end-to-end live in the author's own running engine (not just a test
sandbox): create_table("users") -> insert("u1", {}, 1) -> get("u1", 1)
returns {} as expected.

A thin relational layer -- tables of rows, keyed by primary key -- built
directly on top of a versioned, snapshot-isolated MVCCStore (kept
alongside as mvcc_store.py / mvcc_store_handwritten_reference.py). Record
and MVCCStore are re-declared in this file rather than imported, since
Skylie has no "import" statement yet (see the full note in
build_relational_engine.py) -- reconcile the two copies by hand before
both ship side by side.

Differences from the hand-written reference, each a real Skylie
engine/vocabulary limit, not an oversight:
  - __init__ takes `tables` as a constructor argument you pass in
    yourself (RelationalEngine({})), instead of building its own empty
    dict -- Skylie's class syntax always assigns fields straight from
    constructor arguments.
  - create_table()'s "name already exists" rejection returns the boolean
    False instead of raising ValueError -- Skylie has no "raise".
  - table(name) on a missing name raises a plain KeyError from Python's
    own dict[] lookup, not the reference's hand-raised KeyError with a
    custom message -- both reject with a KeyError, only the message
    differs.
  - scan() and select() are NOT included -- both need a `where` filter
    callable applied per-row plus sorted() for ordering; Skylie can't
    yet accept or call a function as a value, and has no sort
    primitive. Real, open gaps, not silently dropped -- the first things
    to hand-patch if querying beyond get-by-primary-key is needed.
"""

class Record:
    def __init__(self, key, value, version, deleted):
        self.key = key
        self.value = value
        self.version = version
        self.deleted = deleted

class MVCCStore:
    def __init__(self, versions):
        self.versions = versions
    def get_record(self, key, snapshot_version):
        versions_local = self.versions
        if key in versions_local:
            history = versions_local[key]
        else:
            history = list()
        for record in reversed(history):
            if record.version <= snapshot_version:
                return record
        return False
    def get(self, key, snapshot_version):
        record = self.get_record(key, snapshot_version)
        if record == False:
            return False
        if record.deleted == True:
            return False
        return record.value
    def put(self, key, value, version):
        versions_local = self.versions
        if key in versions_local:
            history = versions_local[key]
        else:
            history = list()
            versions_local[key] = history
        record = Record(key, value, version, False)
        history.append(record)
    def remove(self, key, version):
        versions_local = self.versions
        if key in versions_local:
            history = versions_local[key]
        else:
            history = list()
            versions_local[key] = history
        record = Record(key, "", version, True)
        history.append(record)
    def snapshot_read(self, keys, snapshot_version):
        result = dict()
        for key in keys:
            value_local = self.get(key, snapshot_version)
            result[key] = value_local
        return result

class Table:
    def __init__(self, name, store, known_pks):
        self.name = name
        self.store = store
        self.known_pks = known_pks
    def storage_key(self, pk):
        name_local = self.name
        return name_local + ":" + pk
    def insert(self, pk, row, version):
        store_local = self.store
        key = self.storage_key(pk)
        store_local.put(key, row, version)
        known_pks_local = self.known_pks
        known_pks_local.append(pk)
    def remove(self, pk, version):
        store_local = self.store
        key = self.storage_key(pk)
        store_local.remove(key, version)
    def get(self, pk, snapshot_version):
        store_local = self.store
        key = self.storage_key(pk)
        row = store_local.get(key, snapshot_version)
        return row

class RelationalEngine:
    def __init__(self, tables):
        self.tables = tables
    def create_table(self, name):
        tables_local = self.tables
        if name in tables_local:
            return False
        empty_versions = dict()
        store_local = MVCCStore(empty_versions)
        empty_pks = list()
        new_table = Table(name, store_local, empty_pks)
        tables_local[name] = new_table
        return new_table
    def table(self, name):
        tables_local = self.tables
        return tables_local[name]
