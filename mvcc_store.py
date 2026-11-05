"""
mvcc_store.py -- AUTHORED BY SKYLIE, not hand-written.

Generated from the dictated English source in mvcc_store.skylie by
translator.py's translate(), via the full normalize_voice_input pipeline.
A versioned key/value store with snapshot-isolated reads: every write is
a brand-new, whole version of a key, never a partial in-place mutation,
and a reader at snapshot version V sees every key exactly as it stood at
V -- the same design as the hand-written reference (kept alongside as
mvcc_store_handwritten_reference.py).

Differences from the hand-written reference, each a real Skylie
engine/vocabulary limit, not an oversight:
  - __init__ takes `versions` as a constructor argument you pass in
    yourself (MVCCStore({})), instead of building its own empty dict --
    Skylie's class syntax always assigns fields straight from
    constructor arguments; there's no "quietly initialize this field
    myself" yet.
  - The tombstone method is named remove(), not delete() -- "delete" is
    a reserved word in Skylie's own grammar (used by "delete KEY from
    DICT"), so a method can't be declared under that exact name.
  - "not found" and "deleted" both come back as the boolean False, not
    None -- Skylie has no null literal. Everywhere the reference checks
    `is None`, this version checks `is False` (or `== False`) instead;
    behaviorally equivalent, just a different sentinel.
  - put()/remove() accept an out-of-order version silently instead of
    rejecting it -- Skylie has no "raise" statement, so the reference's
    strict version-ordering check isn't reachable here. This is a real
    gap, not just cosmetic: nothing currently stops writing an older
    version after a newer one has already landed.
  - latest_version() and the dump_to_bytes()/load_from_bytes()
    persistence pair are not included -- latest_version() needs a "last
    element of a list" accessor Skylie doesn't have, and persistence
    needs storage_format.py's binary page format, which is out of reach
    of anything in Skylie's grammar. Flagged, not silently skipped.
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
