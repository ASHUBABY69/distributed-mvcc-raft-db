"""
storage_format.py -- the on-disk encoding for one versioned row, and for a
"page" (a batch of them written/read together).

Scope, deliberately: this is pure encode/decode. It knows nothing about
MVCC visibility rules, transactions, or tables -- those are
mvcc_store.py's and relational_engine.py's job. All this file promises is:
given a Record, turn it into bytes; given those bytes back, reconstruct
the exact same Record. Same "assume the layer below already exists, build
the layer that uses it" boundary raft_consensus.py drew around the network.

Format (little-endian throughout, via struct):
    one record on disk =
        4 bytes   key length   (uint32)
        N bytes   key          (utf-8)
        4 bytes   value length (uint32)
        M bytes   value        (utf-8)   -- ignored/zero-length if deleted
        8 bytes   version      (uint64)  -- the MVCC logical timestamp
        1 byte    deleted flag (0 or 1)  -- a tombstone, not an absence

    one page on disk =
        4 bytes   record count (uint32)
        then that many records, back-to-back, in the format above

A tombstone (deleted=True) is still a real record with a real version --
that's what lets a snapshot read correctly see "this key was deleted as of
version V" instead of just finding nothing and not knowing whether that
means "never written" or "written then deleted". Known, deliberate
simplification: no checksum per record/page (a real engine would want one
to detect corruption) -- flagged as future hardening, not silently
assumed solid.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

_HEADER = struct.Struct("<I")       # one uint32: a length prefix
_VERSION_AND_FLAG = struct.Struct("<QB")  # uint64 version + uint8 deleted flag
_COUNT = struct.Struct("<I")        # one uint32: how many records in a page


@dataclass
class Record:
    key: str
    value: str
    version: int
    deleted: bool = False


def encode_record(record: Record) -> bytes:
    key_bytes = record.key.encode("utf-8")
    value_bytes = record.value.encode("utf-8")
    return (
        _HEADER.pack(len(key_bytes))
        + key_bytes
        + _HEADER.pack(len(value_bytes))
        + value_bytes
        + _VERSION_AND_FLAG.pack(record.version, 1 if record.deleted else 0)
    )


def decode_record(data: bytes, offset: int = 0) -> tuple[Record, int]:
    """Decodes ONE record starting at `offset`. Returns (record,
    new_offset) so a caller can keep calling this in a loop to walk a
    buffer holding several records back-to-back, without the caller
    having to know the format's own internal byte layout."""
    (key_len,) = _HEADER.unpack_from(data, offset)
    offset += _HEADER.size
    key = data[offset:offset + key_len].decode("utf-8")
    offset += key_len

    (value_len,) = _HEADER.unpack_from(data, offset)
    offset += _HEADER.size
    value = data[offset:offset + value_len].decode("utf-8")
    offset += value_len

    version, deleted_flag = _VERSION_AND_FLAG.unpack_from(data, offset)
    offset += _VERSION_AND_FLAG.size

    return Record(key=key, value=value, version=version, deleted=bool(deleted_flag)), offset


def encode_page(records: list[Record]) -> bytes:
    body = b"".join(encode_record(r) for r in records)
    return _COUNT.pack(len(records)) + body


def decode_page(data: bytes) -> list[Record]:
    (count,) = _COUNT.unpack_from(data, 0)
    offset = _COUNT.size
    records = []
    for _ in range(count):
        record, offset = decode_record(data, offset)
        records.append(record)
    if offset != len(data):
        raise ValueError(
            f"page decoded {offset} bytes but buffer has {len(data)} -- "
            f"trailing garbage or a truncated page"
        )
    return records


if __name__ == "__main__":
    print("=== 1: round-trip a single record ===")
    r = Record(key="x", value="42", version=7, deleted=False)
    data = encode_record(r)
    back, consumed = decode_record(data)
    assert back == r, (back, r)
    assert consumed == len(data), (consumed, len(data))
    print(f"PASS -- {r} -> {len(data)} bytes -> {back}")

    print("\n=== 2: round-trip a tombstone (deleted record) ===")
    t = Record(key="y", value="", version=9, deleted=True)
    data = encode_record(t)
    back, _ = decode_record(data)
    assert back.deleted is True and back.key == "y" and back.version == 9
    print(f"PASS -- tombstone round-trips -- {back}")

    print("\n=== 3: a key/value containing multi-byte UTF-8 survives exactly ===")
    r = Record(key="city_naïve_日本", value="café ☕", version=1, deleted=False)
    back, _ = decode_record(encode_record(r))
    assert back == r, (back, r)
    print(f"PASS -- {back}")

    print("\n=== 4: a page of several records, decoded back in the same order ===")
    records = [
        Record(key="a", value="1", version=1),
        Record(key="b", value="2", version=1),
        Record(key="a", value="3", version=2),  # same key, newer version -- MVCC's job to pick, not this file's
        Record(key="c", value="", version=2, deleted=True),
    ]
    page = encode_page(records)
    back = decode_page(page)
    assert back == records, (back, records)
    print(f"PASS -- {len(records)} records round-tripped through one page, {len(page)} bytes")

    print("\n=== 5: a truncated/corrupt page is REJECTED, not silently misread ===")
    page = encode_page(records)
    try:
        decode_page(page[:-1])  # chop the last byte off
        raise AssertionError("expected a ValueError for a truncated page")
    except (ValueError, struct.error):
        pass
    print("PASS -- truncated page correctly raised instead of returning wrong data")

    print("\n=== 6: empty page (zero records) round-trips too ===")
    empty = decode_page(encode_page([]))
    assert empty == []
    print("PASS -- empty page ->", empty)

    print("\n=== ALL STORAGE-FORMAT CHECKS PASSED ===")
