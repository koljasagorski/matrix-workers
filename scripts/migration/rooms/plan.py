"""Read the immutable source and validate wire IDs with Synapse's own parser.

No source event is signed, edited, assigned a different ID, or deleted here.
Client-only event_id/unsigned fields are separated from their native wire JSON.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path

SYNAPSE_VERSION = "1.162.0"
SCHEMA_VERSION = 94


def json_text(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def source_connection(path):
    connection = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def row_event(row):
    event = {key: row[key] for key in ("room_id", "sender", "origin_server_ts", "depth")}
    event["type"] = row["event_type"]
    if row["state_key"] is not None:
        event["state_key"] = row["state_key"]
    for key in ("content", "auth_events", "prev_events", "hashes", "signatures", "unsigned"):
        if row[key] is not None:
            event[key] = json.loads(row[key])
    if row.get("redacts") is not None:
        event["redacts"] = row["redacts"]
    return event


def validate_event(event_id, event, version):
    import synapse
    from synapse.api.room_versions import KNOWN_ROOM_VERSIONS
    from synapse.events import make_event_from_dict

    if synapse.__version__ != SYNAPSE_VERSION:
        raise RuntimeError(f"Importer requires Synapse {SYNAPSE_VERSION}")
    if version not in ("10", "11", "12"):
        raise ValueError("Unsupported source room version")
    wire = dict(event)
    wire.pop("event_id", None)
    if version == "12" and wire["type"] == "m.room.create":
        wire.pop("room_id", None)
    parsed = make_event_from_dict(wire, KNOWN_ROOM_VERSIONS[version])
    if parsed.event_id != event_id:
        raise ValueError("Stored event ID differs from native reference ID")
    if not wire.get("hashes") or not wire.get("signatures"):
        raise ValueError("Event has no hashes or signatures")
    if parsed.room_id != event["room_id"]:
        raise ValueError("Event belongs to a different room")
    return wire


def build_plan(path, validator=validate_event):
    with closing(source_connection(path)) as source:
        rooms = {row["room_id"]: dict(row) for row in source.execute("SELECT * FROM rooms")}
        records = {}
        for row in source.execute("SELECT * FROM event_state_archive"):
            record = dict(row)
            payload = json.loads(record["event_json"])
            payload["room_id"] = record["room_id"]
            records[record["event_id"]] = {"event_id": record["event_id"], "room_id": record["room_id"],
                "payload": payload, "stream_ordering": None, "source": "archive"}
        raw_events = [dict(row) for row in source.execute("SELECT * FROM events")]
        for row in raw_events:
            records[row["event_id"]] = {"event_id": row["event_id"], "room_id": row["room_id"],
                "payload": row_event(row), "stream_ordering": row["stream_ordering"], "source": "events"}
        invalid = {}
        for event_id, record in records.items():
            try:
                record["wire"] = validator(event_id, record["payload"], rooms[record["room_id"]]["room_version"])
            except (ValueError, KeyError, TypeError) as error:
                invalid[event_id] = type(error).__name__ + ": " + str(error)
            except Exception as error:
                # Synapse's native parser raises SynapseError for malformed JSON.
                from synapse.api.errors import SynapseError
                if not isinstance(error, SynapseError):
                    raise
                invalid[event_id] = "Invalid native event JSON"
        states = {room_id: {} for room_id in rooms}
        for row in source.execute("SELECT * FROM room_state"):
            states[row["room_id"]][(row["event_type"], row["state_key"])] = row["event_id"]
        legacy = {room_id for room_id, state in states.items() if any(event_id in invalid for event_id in state.values())}
        for room_id, state in states.items():
            create_id = state.get(("m.room.create", ""))
            if create_id is None or create_id not in records:
                raise ValueError(f"Room has no stored create event: {room_id}")
            for key, event_id in state.items():
                record = records.get(event_id)
                if not record or record["room_id"] != room_id:
                    raise ValueError(f"Invalid current state reference: {room_id}")
                if (record["payload"]["type"], record["payload"].get("state_key")) != key:
                    raise ValueError(f"Current state key mismatch: {room_id}")
        native = {event_id: record for event_id, record in records.items()
                  if record["room_id"] not in legacy and event_id not in invalid}
        for event_id, record in native.items():
            for prev_id in record["payload"].get("prev_events", []):
                previous = records.get(prev_id)
                if previous and previous["room_id"] != record["room_id"]:
                    raise ValueError("Previous event belongs to a different room")
            for auth_id in record["payload"].get("auth_events", []):
                auth = native.get(auth_id)
                if not auth or auth["room_id"] != record["room_id"] or "state_key" not in auth["payload"]:
                    raise ValueError(f"Incomplete native auth chain for {event_id}: {auth_id}")
        snapshots = {}
        for row in source.execute("SELECT * FROM event_state_snapshots"):
            if row["event_id"] not in native:
                continue
            state = {(kind, key): event_id for kind, key, event_id in json.loads(row["state_before"])}
            for key, event_id in state.items():
                record = native.get(event_id)
                if not record or record["room_id"] != row["room_id"] or (
                    record["payload"]["type"], record["payload"].get("state_key")) != key:
                    raise ValueError(f"Invalid historical state snapshot: {row['event_id']}")
            snapshots[row["event_id"]] = state
        memberships = [dict(row) for row in source.execute("SELECT * FROM room_memberships")]
        for membership in memberships:
            if membership["room_id"] in legacy:
                continue
            record = native.get(membership["event_id"])
            if not record or record["payload"].get("state_key") != membership["user_id"] or (
                record["payload"]["content"].get("membership") != membership["membership"]):
                raise ValueError("Current membership differs from its stored event")
        aliases = [dict(row) for row in source.execute("SELECT * FROM room_aliases")]
        receipts = [dict(row) for row in source.execute("SELECT * FROM receipts")]
    return {"source_sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest(), "rooms": rooms,
        "events": records, "native_events": native, "raw_events": raw_events, "legacy_rooms": legacy,
        "invalid_events": invalid, "states": states, "snapshots": snapshots, "memberships": memberships,
        "aliases": aliases, "receipts": receipts}


def summary(plan):
    return {"source_sha256": plan["source_sha256"], "synapse_version": SYNAPSE_VERSION,
        "schema_version": SCHEMA_VERSION, "rooms": len(plan["rooms"]),
        "native_rooms": len(plan["rooms"]) - len(plan["legacy_rooms"]),
        "legacy_rooms": sorted(plan["legacy_rooms"]), "source_events": len(plan["raw_events"]),
        "native_events": len(plan["native_events"]), "invalid_event_count": len(plan["invalid_events"]),
        "invalid_native_history": [event_id for event_id, record in plan["events"].items()
            if record["room_id"] not in plan["legacy_rooms"] and event_id in plan["invalid_events"]]}
