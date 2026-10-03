"""Transactional offline import for Synapse 1.162/schema94 only.

Every native PDU was parsed and its reference ID checked by plan.py. Cached
auth/state events are outliers; source timeline positions remain unchanged.
No unsigned legacy event is put in Synapse's native event store.
"""
import hashlib
import json
import os
from contextlib import closing

from .plan import SCHEMA_VERSION, json_text
from .state import build_states


def connect():
    import psycopg2
    if os.environ.get("MATRIX_IMPORT_DSN"):
        return psycopg2.connect(os.environ["MATRIX_IMPORT_DSN"])
    return psycopg2.connect(host=os.environ.get("PGHOST", "matrix-postgres"),
        dbname=os.environ.get("PGDATABASE", "synapse"), user=os.environ.get("POSTGRES_USER", "matrix"),
        password=os.environ["POSTGRES_PASSWORD"])


def insert(cursor, table, row):
    from psycopg2 import sql
    keys = list(row)
    cursor.execute(sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(sql.Identifier(table),
        sql.SQL(",").join(map(sql.Identifier, keys)), sql.SQL(",").join(sql.Placeholder() for _ in keys)),
        [row[key] for key in keys])


def event_format(version):
    from synapse.api.room_versions import KNOWN_ROOM_VERSIONS
    return int(KNOWN_ROOM_VERSIONS[version].event_format)


def import_plan(plan, server_name):
    native = plan["native_events"]
    after, reconstructed = build_states(plan)
    positive = [record["stream_ordering"] for record in native.values() if record["stream_ordering"] is not None]
    if len(positive) != len(set(positive)) or any(position <= 0 for position in positive):
        raise ValueError("Source timeline positions are not unique positive integers")
    ordering = {eid: record["stream_ordering"] for eid, record in native.items()}
    negative = 0
    for eid in sorted(native):
        if ordering[eid] is None:
            negative -= 1
            ordering[eid] = negative
    max_stream = max(positive, default=1)
    timeline = {eid for eid, record in native.items() if record["stream_ordering"] is not None}
    referenced = {parent for eid in timeline for parent in native[eid]["payload"].get("prev_events", [])}
    heads = {room_id: sorted(eid for eid in timeline - referenced if native[eid]["room_id"] == room_id)
             for room_id in plan["rooms"] if room_id not in plan["legacy_rooms"]}
    if any(not room_heads for room_heads in heads.values()):
        raise ValueError("Native room has no forward extremity")
    # Future native events must start from the source's complete current state,
    # even where the source never retained older full-state snapshots.
    for room_id, room_heads in heads.items():
        for eid in room_heads:
            after[eid] = dict(plan["states"][room_id])
    result = {"imported": True, "native_timeline_events": len(timeline),
        "native_outliers": len(native) - len(timeline), "reconstructed_historic_groups": len(reconstructed),
        "reconstructed_historic_events": sorted(reconstructed),
        "native_pdu_sha256": hashlib.sha256(json_text([(eid, native[eid]["wire"]) for eid in sorted(native)]).encode()).hexdigest(),
        "authoritative_historic_snapshots": len(plan["snapshots"]), "max_event_stream": max_stream,
        "source_receipts": len(plan["receipts"]),
        "archive_receipts": sum(receipt["room_id"] in plan["legacy_rooms"] for receipt in plan["receipts"])}
    with closing(connect()) as connection, connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(7366421094)")
            cursor.execute("SELECT version FROM schema_version")
            if cursor.fetchone() != (SCHEMA_VERSION,):
                raise ValueError("Target schema version differs from pinned import schema")
            cursor.execute("CREATE TABLE IF NOT EXISTS worker_room_migration (source_sha256 text PRIMARY KEY, report text NOT NULL)")
            cursor.execute("SELECT report FROM worker_room_migration WHERE source_sha256=%s", (plan["source_sha256"],))
            previous = cursor.fetchone()
            if previous:
                return dict(json.loads(previous[0]), resumed=True)
            cursor.execute("SELECT (SELECT COUNT(*) FROM rooms), (SELECT COUNT(*) FROM events)")
            if cursor.fetchone() != (0, 0):
                raise ValueError("Room import requires an empty target room/event store")
            cursor.execute("SELECT name FROM users WHERE deactivated=0")
            users = {row[0] for row in cursor.fetchall()}
            if not users:
                raise ValueError("Import local users first")
            pending = []
            def persist(table, row):
                # Schema94 enforces event foreign keys immediately for these
                # materialized views. Events must exist before their metadata.
                if table in {"current_state_events", "local_current_membership",
                             "sliding_sync_joined_rooms", "sliding_sync_membership_snapshots"}:
                    pending.append((table, row))
                else:
                    insert(cursor, table, row)
            group_cache = {}
            for room_id, room in plan["rooms"].items():
                if room_id in plan["legacy_rooms"]:
                    continue
                persist("rooms", dict(room_id=room_id, is_public=bool(room["is_public"]),
                    creator=room["creator_id"], room_version=room["room_version"], has_auth_chain_index=False))
                current = plan["states"][room_id]
                content = lambda kind: native[current[(kind, "")]]["payload"]["content"] if (kind, "") in current else {}
                create = content("m.room.create")
                name, room_type = content("m.room.name").get("name"), create.get("type")
                encrypted = ("m.room.encryption", "") in current
                tombstone = content("m.room.tombstone").get("replacement_room")
                members = [native[eid]["payload"] for (kind, key), eid in current.items() if kind == "m.room.member"]
                member_counts = {membership: sum(event["content"].get("membership") == membership for event in members)
                    for membership in ("join", "invite", "leave", "ban", "knock")}
                local_joined = sum(event["state_key"] in users and event["content"].get("membership") == "join" for event in members)
                room_events = [eid for eid in timeline if native[eid]["room_id"] == room_id]
                last = max(ordering[eid] for eid in room_events)
                bump = max((ordering[eid] for eid in room_events if native[eid]["payload"]["type"] in
                    ("m.room.message", "m.room.encrypted", "m.sticker", "m.call.invite", "m.poll.start")), default=last)
                persist("room_depth", dict(room_id=room_id, min_depth=max(native[eid]["payload"]["depth"] for eid in room_events)))
                persist("room_stats_current", dict(room_id=room_id, current_state_events=len(current),
                    joined_members=member_counts["join"], invited_members=member_counts["invite"],
                    left_members=member_counts["leave"], banned_members=member_counts["ban"], knocked_members=member_counts["knock"],
                    local_users_in_room=local_joined, completed_delta_stream_id=max_stream))
                persist("room_stats_state", dict(room_id=room_id, name=name, room_type=room_type,
                    canonical_alias=content("m.room.canonical_alias").get("alias"), join_rules=content("m.room.join_rules").get("join_rule"),
                    history_visibility=content("m.room.history_visibility").get("history_visibility"),
                    encryption=content("m.room.encryption").get("algorithm"), avatar=content("m.room.avatar").get("url"),
                    guest_access=content("m.room.guest_access").get("guest_access"), topic=content("m.room.topic").get("topic"),
                    is_federatable=create.get("m.federate", True)))
                if local_joined:
                    persist("sliding_sync_joined_rooms", dict(room_id=room_id, event_stream_ordering=last,
                        bump_stamp=bump, room_type=room_type, room_name=name, is_encrypted=encrypted,
                        tombstone_successor_room_id=tombstone))
                for (kind, key), eid in current.items():
                    membership = native[eid]["payload"]["content"].get("membership") if kind == "m.room.member" else None
                    persist("current_state_events", dict(event_id=eid, room_id=room_id, type=kind, state_key=key,
                        membership=membership, event_stream_ordering=ordering[eid]))
                    persist("current_state_delta_stream", dict(stream_id=max_stream, instance_name="master",
                        room_id=room_id, type=kind, state_key=key, event_id=eid, prev_event_id=None))
                    if kind == "m.room.member" and key in users:
                        persist("local_current_membership", dict(room_id=room_id, user_id=key,
                            event_id=eid, membership=membership, event_stream_ordering=ordering[eid]))
                        persist("sliding_sync_membership_snapshots", dict(room_id=room_id, user_id=key,
                            sender=native[eid]["payload"]["sender"], membership_event_id=eid, membership=membership,
                            forgotten=0, event_stream_ordering=ordering[eid], event_instance_name="master", has_known_state=True,
                            room_type=room_type, room_name=name, is_encrypted=encrypted, tombstone_successor_room_id=tombstone))
                for eid in heads[room_id]:
                    persist("event_forward_extremities", dict(room_id=room_id, event_id=eid))
                missing = {parent for eid in room_events for parent in native[eid]["payload"].get("prev_events", []) if parent not in native}
                for eid in missing:
                    persist("event_backward_extremities", dict(room_id=room_id, event_id=eid))
            for eid, record in sorted(native.items()):
                event = record["payload"]
                outlier = eid not in timeline
                persist("events", dict(event_id=eid, room_id=record["room_id"], sender=event["sender"], type=event["type"],
                    state_key=event.get("state_key"), topological_ordering=event["depth"], depth=event["depth"],
                    processed=True, outlier=outlier, stream_ordering=ordering[eid], instance_name="master",
                    origin_server_ts=event["origin_server_ts"], received_ts=event["origin_server_ts"],
                    contains_url=isinstance(event["content"].get("url"), str), rejection_reason=None))
                persist("event_json", dict(event_id=eid, room_id=record["room_id"],
                    internal_metadata=json_text({"outlier": outlier}), json=json_text(record["wire"]),
                    format_version=event_format(plan["rooms"][record["room_id"]]["room_version"])))
                for parent in event.get("prev_events", []):
                    persist("event_edges", dict(event_id=eid, prev_event_id=parent, room_id=record["room_id"], is_state=False))
                if "state_key" in event:
                    persist("state_events", dict(event_id=eid, room_id=record["room_id"], type=event["type"], state_key=event["state_key"]))
                    for auth in event.get("auth_events", []):
                        persist("event_auth", dict(event_id=eid, room_id=record["room_id"], auth_id=auth))
                if event["type"] == "m.room.member":
                    persist("room_memberships", dict(event_id=eid, user_id=event["state_key"], sender=event["sender"],
                        room_id=record["room_id"], membership=event["content"]["membership"], forgotten=0,
                        display_name=event["content"].get("displayname"), avatar_url=event["content"].get("avatar_url"),
                        event_stream_ordering=ordering[eid], participant=False))
                if not outlier:
                    key = (record["room_id"], tuple(sorted(after[eid].items())))
                    group = group_cache.get(key)
                    if group is None:
                        cursor.execute("SELECT nextval('state_group_id_seq')")
                        group = cursor.fetchone()[0]
                        group_cache[key] = group
                        persist("state_groups", dict(id=group, room_id=record["room_id"], event_id=eid))
                        for (kind, state_key), state_id in after[eid].items():
                            persist("state_groups_state", dict(state_group=group, room_id=record["room_id"],
                                type=kind, state_key=state_key, event_id=state_id))
                    persist("event_to_state_groups", dict(event_id=eid, state_group=group))
                relation = event["content"].get("m.relates_to")
                if isinstance(relation, dict) and isinstance(relation.get("event_id"), str) and isinstance(relation.get("rel_type"), str):
                    persist("event_relations", dict(event_id=eid, relates_to_id=relation["event_id"],
                        relation_type=relation["rel_type"], aggregation_key=relation.get("key")))
                if event["type"] == "m.room.redaction":
                    target = event.get("redacts") if plan["rooms"][record["room_id"]]["room_version"] == "10" else event["content"].get("redacts")
                    if isinstance(target, str):
                        persist("redactions", dict(event_id=eid, redacts=target, have_censored=False,
                            received_ts=event["origin_server_ts"], recheck=True))
            for table, row in pending:
                insert(cursor, table, row)
            for alias in plan["aliases"]:
                if alias["room_id"] not in heads:
                    continue
                persist("room_aliases", dict(room_alias=alias["alias"], room_id=alias["room_id"], creator=alias["creator_id"]))
                persist("room_alias_servers", dict(room_alias=alias["alias"], server=server_name))
            receipt_count = 0
            for receipt in plan["receipts"]:
                eid = receipt["event_id"]
                if eid not in native or native[eid]["room_id"] != receipt["room_id"]:
                    continue
                receipt_count += 1
                data = {"ts": receipt["ts"]}
                thread = receipt["thread_id"] or None
                if thread is not None:
                    data["thread_id"] = thread
                row = dict(room_id=receipt["room_id"], receipt_type=receipt["receipt_type"], user_id=receipt["user_id"],
                    data=json_text(data), thread_id=thread)
                persist("receipts_graph", dict(row, event_ids=json_text([eid])))
                persist("receipts_linearized", dict(row, stream_id=receipt_count, event_id=eid,
                    instance_name="master", event_stream_ordering=ordering[eid]))
            cursor.execute("SELECT setval('events_stream_seq', %s, true)", (max_stream,))
            cursor.execute("SELECT setval('events_backfill_stream_seq', %s, true)", (max(1, -negative),))
            cursor.execute("SELECT setval('receipts_sequence', %s, true)", (max(1, receipt_count),))
            for stream, position in (("events", max_stream), ("backfill", negative), ("receipts", receipt_count)):
                cursor.execute("INSERT INTO stream_positions(stream_name,instance_name,stream_id) VALUES (%s,'master',%s) ON CONFLICT(stream_name,instance_name) DO UPDATE SET stream_id=EXCLUDED.stream_id", (stream, position))
            result.update(native_receipts=receipt_count, state_groups=len(group_cache))
            persist("worker_room_migration", dict(source_sha256=plan["source_sha256"], report=json_text(result)))
    return result
