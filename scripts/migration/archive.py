"""Authenticated, immutable history for PDUs Synapse cannot represent natively.

The five legacy rooms retain their original IDs, event IDs and ciphertext. Their
snapshot is merged only into initial/full-state classic syncs. The native sync
token is never replaced. Native authentication is checked by the gateway before
every call. No event, membership, encryption key or receipt is invented here.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from pathlib import Path
from urllib.parse import quote, unquote

from aiohttp import web

ROOM_PATH = re.compile(r"^/_matrix/client/(?:v3|r0)/rooms/([^/]+)(?:/(.*))?$")
TOKEN = re.compile(r"^wa_(-?\d+)$")
LEGACY_TOKEN = re.compile(r"^s?(-?\d+)(?:_td\d+)?(?:_dk\d+)?(?:_rr\d+)?(?:_ad\d+)?$")
TOPO_TOKEN = re.compile(r"^t(\d+)-(-?\d+)$")
STORE = None


def error(code, message, status):
    return web.json_response({"errcode": code, "error": message}, status=status,
                             headers={"Cache-Control": "no-store"})


def event(row):
    result = {"event_id": row["event_id"], "room_id": row["room_id"], "sender": row["sender"],
              "type": row["event_type"], "origin_server_ts": row["origin_server_ts"],
              "content": json.loads(row["content"])}
    if row["state_key"] is not None:
        result["state_key"] = row["state_key"]
    if row.get("unsigned"):
        result["unsigned"] = json.loads(row["unsigned"])
    if row.get("redacts"):
        result["redacts"] = row["redacts"]
    return result


def allowed(value, section):
    if value in section.get("not_rooms", []):
        return False
    rooms = section.get("rooms")
    return rooms is None or value in rooms


def filtered(events, section, user_id):
    if not isinstance(section, dict):
        raise ValueError("Event filter must be an object")
    result = []
    for item in events:
        kind = item["type"]
        matches = lambda pattern: kind.startswith(pattern[:-1]) if pattern.endswith("*") else kind == pattern
        if section.get("types") is not None and not any(matches(pattern) for pattern in section["types"]):
            continue
        if any(matches(pattern) for pattern in section.get("not_types", [])):
            continue
        if section.get("senders") is not None and item["sender"] not in section["senders"]:
            continue
        if item["sender"] in section.get("not_senders", []):
            continue
        result.append(item)
    return result


class ArchiveStore:
    def __init__(self, source_path, manifest_path):
        manifest = json.loads(Path(manifest_path).read_text())
        if hashlib.sha256(Path(source_path).read_bytes()).hexdigest() != manifest.get("archive_sha256", manifest["source_sha256"]):
            raise ValueError("Archive source differs from the validated migration snapshot")
        self.legacy = set(manifest["legacy_rooms"])
        self.invalid_history = set(manifest["invalid_native_history"])
        self.source = sqlite3.connect(Path(source_path).resolve().as_uri() + "?mode=ro&immutable=1", uri=True)
        self.source.row_factory = sqlite3.Row
        if "archive_sha256" in manifest:
            provenance = self.source.execute("SELECT source_sha256 FROM archive_provenance").fetchone()
            if not provenance or provenance[0] != manifest["source_sha256"]:
                raise ValueError("Archive provenance differs from the validated source snapshot")
        self.events = {row["event_id"]: dict(row) for row in self.source.execute("SELECT * FROM events")}
        self.memberships = {(row["room_id"], row["user_id"]): row["membership"]
                            for row in self.source.execute("SELECT * FROM room_memberships")}
        self.states = {}
        for row in self.source.execute("SELECT * FROM room_state"):
            self.states.setdefault(row["room_id"], {})[(row["event_type"], row["state_key"])] = row["event_id"]
        self.account_data = {}
        for row in self.source.execute("SELECT * FROM account_data WHERE room_id <> ''"):
            self.account_data.setdefault((row["user_id"], row["room_id"]), []).append(
                {"type": row["event_type"], "content": json.loads(row["content"])})
        self.receipts = [dict(row) for row in self.source.execute("SELECT * FROM receipts")]

    def can_read(self, user_id, room_id):
        # Only users who could read these exact historical events in the source
        # can access the immutable copy. Native whoami additionally checks session
        # revocation/deactivation on every request.
        return self.memberships.get((room_id, user_id)) == "join"

    def timeline(self, room_id):
        return sorted((row for row in self.events.values() if row["room_id"] == room_id and row["stream_ordering"] is not None),
                      key=lambda row: row["stream_ordering"])

    def state(self, room_id):
        return [event(self.events[eid]) for key, eid in sorted(self.states[room_id].items())]

    def room_snapshot(self, room_id, user_id, room_filter):
        section = room_filter.get("timeline", {})
        limit = min(max(int(section.get("limit", 20)), 0), 100)
        source_rows = self.timeline(room_id)
        timeline = filtered([event(row) for row in source_rows], section, user_id)
        selected = timeline[-limit:] if limit else []
        oldest = self.events[selected[0]["event_id"]]["stream_ordering"] if selected else (source_rows[-1]["stream_ordering"] + 1 if source_rows else 1)
        state = self.state(room_id)
        members = [item for item in state if item["type"] == "m.room.member"]
        receipt_content = {}
        for row in self.receipts:
            if row["room_id"] != room_id or (row["receipt_type"] == "m.read.private" and row["user_id"] != user_id):
                continue
            data = {"ts": row["ts"]}
            if row["thread_id"]:
                data["thread_id"] = row["thread_id"]
            receipt_content.setdefault(row["event_id"], {}).setdefault(row["receipt_type"], {})[row["user_id"]] = data
        return {"timeline": {"events": selected, "limited": len(timeline) > len(selected), "prev_batch": "wa_" + str(oldest)},
                "state": {"events": filtered(state, room_filter.get("state", {}), user_id)},
                "ephemeral": {"events": [{"type": "m.receipt", "content": receipt_content}] if receipt_content else []},
                "account_data": {"events": self.account_data.get((user_id, room_id), [])},
                "summary": {"m.joined_member_count": sum(item["content"].get("membership") == "join" for item in members),
                            "m.invited_member_count": sum(item["content"].get("membership") == "invite" for item in members)},
                "unread_notifications": {"notification_count": 0, "highlight_count": 0}}

    def messages(self, room_id, query, user_id):
        direction = query.get("dir", "b")
        if direction not in ("b", "f"):
            return error("M_INVALID_PARAM", "Invalid pagination direction", 400)
        try:
            limit = min(max(int(query.get("limit", 10)), 0), 100)
            rows = self.timeline(room_id)
            original = query.get("from")
            token = TOKEN.fullmatch(original or "") or LEGACY_TOKEN.fullmatch(original or "")
            if original is not None and token is None:
                return error("M_INVALID_PARAM", "Invalid archive pagination position", 400)
            position = int(token[1]) if token else (rows[-1]["stream_ordering"] + 1 if direction == "b" and rows else 0)
            to_token = TOKEN.fullmatch(query.get("to", "")) or LEGACY_TOKEN.fullmatch(query.get("to", ""))
            if query.get("to") is not None and to_token is None:
                return error("M_INVALID_PARAM", "Invalid archive pagination position", 400)
            until = int(to_token[1]) if to_token else None
            selected = [row for row in rows if (row["stream_ordering"] < position if direction == "b" else row["stream_ordering"] >= position)
                        and (until is None or (row["stream_ordering"] >= until if direction == "b" else row["stream_ordering"] < until))]
            if direction == "b":
                selected.reverse()
            section = json.loads(query.get("filter", "{}"))
            if not isinstance(section, dict):
                return error("M_INVALID_PARAM", "Event filter must be an object", 400)
            candidates = filtered([event(row) for row in selected], section, user_id)
            chunk = candidates[:limit]
            end = self.events[chunk[-1]["event_id"]]["stream_ordering"] + (1 if direction == "f" else 0) if chunk else position
            return web.json_response({"start": original or "wa_" + str(position), "end": "wa_" + str(end),
                "chunk": chunk, "state": self.state(room_id)}, headers={"Cache-Control": "no-store"})
        except (ValueError, TypeError, KeyError):
            return error("M_INVALID_PARAM", "Invalid archive pagination parameters", 400)


def store():
    global STORE
    if STORE is None:
        STORE = ArchiveStore(os.environ["MATRIX_ARCHIVE_SOURCE"], os.environ["MATRIX_ARCHIVE_MANIFEST"])
    return STORE


async def upstream_json(request, session, upstream_url, headers):
    headers = dict(headers)
    headers["Accept-Encoding"] = "identity"
    async with session.request(request.method, upstream_url + request["compat_path"],
                               params=request["compat_query"], data=request["compat_body"], headers=headers) as response:
        if response.status != 200:
            return web.Response(status=response.status, body=await response.read(), content_type="application/json")
        return await response.json()


async def sync_filter(value, identity, session, upstream_url, headers):
    """Resolve owner-scoped native filter IDs before requesting any sync data.

    Element creates numeric saved filters after a fresh login. Legacy filters
    were already expanded by the gateway; inline JSON remains unchanged.
    """
    if value is None:
        return {}
    try:
        parsed = json.loads(value)
    except ValueError:
        parsed = None
        if value.startswith("{"):
            return error("M_INVALID_PARAM", "Invalid sync filter", 400)
    if isinstance(parsed, dict):
        return parsed
    headers = dict(headers)
    headers["Accept-Encoding"] = "identity"
    path = "/_matrix/client/v3/user/" + quote(identity["user_id"], safe="") + "/filter/" + quote(value, safe="")
    async with session.get(upstream_url + path, headers=headers) as response:
        if response.status != 200:
            return web.Response(status=response.status, body=await response.read(), content_type="application/json")
        parsed = await response.json()
        if not isinstance(parsed, dict):
            return error("M_INVALID_PARAM", "Invalid saved sync filter", 400)
        return parsed


async def handle(request, identity, upstream_session, upstream_url, forwarded_headers):
    archive = store()
    path = request["compat_path"]
    match = ROOM_PATH.fullmatch(path)
    if match:
        room_id, suffix = unquote(match[1]), match[2] or ""
        legacy = room_id in archive.legacy
        target_id = unquote(suffix[6:]) if suffix.startswith("event/") else unquote(suffix[8:]) if suffix.startswith("context/") else None
        historical = target_id in archive.invalid_history and archive.events[target_id]["room_id"] == room_id
        query = dict(request["compat_query"])
        old_from = query.get("from", "")
        old_cursor = LEGACY_TOKEN.fullmatch(old_from) if "_" in old_from or old_from.isdecimal() else None
        historical_room = any(archive.events[eid]["room_id"] == room_id for eid in archive.invalid_history)
        if not legacy and suffix == "messages" and request.method == "GET" and (old_cursor or historical_room):
            if old_cursor:
                request["compat_query"] = [(key, "s" + old_cursor[1] if key == "from" else value)
                                           for key, value in request["compat_query"]]
            # Native authorization/history visibility remains authoritative.
            response = await upstream_json(request, upstream_session, upstream_url, forwarded_headers)
            if isinstance(response, web.StreamResponse):
                return response
            if identity and archive.can_read(identity["user_id"], room_id):
                overlay_history(archive, response, query, room_id, identity["user_id"])
            return web.json_response(response, headers={"Cache-Control": "no-store"})
        if not legacy and not historical:
            return None
        if identity is None:
            return error("M_UNKNOWN_TOKEN", "Authentication required", 401)
        if not archive.can_read(identity["user_id"], room_id):
            return error("M_FORBIDDEN", "You cannot read this archive", 403)
        if request.method != "GET":
            return error("M_FORBIDDEN", "This historical room is read only", 403)
        if suffix == "messages":
            return archive.messages(room_id, dict(request["compat_query"]), identity["user_id"])
        if suffix.startswith("event/"):
            row = archive.events.get(target_id)
            if not row or row["room_id"] != room_id:
                return error("M_NOT_FOUND", "Event not found", 404)
            return web.json_response(event(row), headers={"Cache-Control": "no-store"})
        if suffix in ("state", "state/"):
            return web.json_response(archive.state(room_id))
        if suffix.startswith("state/"):
            segments = suffix[6:].split("/", 1)
            key = (unquote(segments[0]), unquote(segments[1]) if len(segments) > 1 else "")
            eid = archive.states[room_id].get(key)
            return web.json_response(event(archive.events[eid])["content"]) if eid else error("M_NOT_FOUND", "State not found", 404)
        if suffix == "members":
            query = dict(request["compat_query"])
            chunk = [item for item in archive.state(room_id) if item["type"] == "m.room.member"
                     and ("membership" not in query or item["content"].get("membership") == query["membership"])
                     and ("not_membership" not in query or item["content"].get("membership") != query["not_membership"])]
            return web.json_response({"chunk": chunk})
        if suffix == "joined_members":
            joined = {item["state_key"]: {"display_name": item["content"].get("displayname"), "avatar_url": item["content"].get("avatar_url")}
                      for item in archive.state(room_id) if item["type"] == "m.room.member" and item["content"].get("membership") == "join"}
            return web.json_response({"joined": joined})
        if suffix.startswith("context/"):
            eid = unquote(suffix[8:])
            rows = archive.timeline(room_id)
            index = next((index for index, row in enumerate(rows) if row["event_id"] == eid), None)
            if index is None:
                return error("M_NOT_FOUND", "Event not found", 404)
            try:
                limit = min(max(int(dict(request["compat_query"]).get("limit", 10)), 0), 100)
            except ValueError:
                return error("M_INVALID_PARAM", "Invalid context limit", 400)
            before, after = rows[max(0, index - limit):index], rows[index + 1:index + 1 + limit]
            return web.json_response({"event": event(rows[index]), "events_before": [event(row) for row in reversed(before)],
                "events_after": [event(row) for row in after], "state": archive.state(room_id),
                "start": "wa_" + str(before[0]["stream_ordering"] if before else rows[index]["stream_ordering"]),
                "end": "wa_" + str(after[-1]["stream_ordering"] + 1 if after else rows[index]["stream_ordering"] + 1)})
        if suffix.startswith("relations/"):
            segments = [unquote(segment) for segment in suffix[10:].split("/")]
            chunk = []
            for row in archive.timeline(room_id):
                item = event(row)
                relation = item["content"].get("m.relates_to", {})
                if relation.get("event_id") == segments[0] and (len(segments) < 2 or relation.get("rel_type") == segments[1]) and (len(segments) < 3 or item["type"] == segments[2]):
                    chunk.append(item)
            return web.json_response({"chunk": chunk})
        return error("M_NOT_FOUND", "Archive endpoint not found", 404)
    if path not in ("/_matrix/client/v3/sync", "/_matrix/client/r0/sync", "/_matrix/client/v3/joined_rooms", "/_matrix/client/r0/joined_rooms") or request.method != "GET" or identity is None:
        return None
    query = dict(request["compat_query"])
    sync = path.endswith("/sync")
    if sync and "since" in query and query.get("full_state") != "true":
        return None
    room_filter = {}
    if sync:
        definition = await sync_filter(query.get("filter"), identity, upstream_session, upstream_url, forwarded_headers)
        if isinstance(definition, web.StreamResponse):
            return definition
        room_filter = definition.get("room", {})
        if not isinstance(room_filter, dict):
            return error("M_INVALID_PARAM", "Invalid room sync filter", 400)
    response = await upstream_json(request, upstream_session, upstream_url, forwarded_headers)
    if isinstance(response, web.StreamResponse):
        return response
    user_id = identity["user_id"]
    readable = [room_id for room_id in sorted(archive.legacy) if archive.can_read(user_id, room_id)]
    if not sync:
        response["joined_rooms"] = sorted(set(response.get("joined_rooms", [])) | set(readable))
    else:
        joined = response.setdefault("rooms", {}).setdefault("join", {})
        for room_id in readable:
            if allowed(room_id, room_filter):
                joined[room_id] = archive.room_snapshot(room_id, user_id, room_filter)
    return web.json_response(response, headers={"Cache-Control": "no-store"})


def overlay_history(archive, response, query, room_id, user_id):
    """Insert the one malformed historical leave only inside its native page.

    It remains a client history object, never native auth/current state. Bounds
    and the continuation follow Synapse's (depth, stream) ordering. No timestamp
    guess, membership replacement, or repeated event on subsequent pages.
    """
    direction = query.get("dir", "b")
    chunk = response.get("chunk", [])
    source_key = lambda row: (row["depth"], row["stream_ordering"])
    start = TOPO_TOKEN.fullmatch(response.get("start", ""))
    end = TOPO_TOKEN.fullmatch(response.get("end", ""))
    start_key = (int(start[1]), int(start[2])) if start else None
    end_key = (int(end[1]), int(end[2])) if end else None
    if start_key is None:
        stream = LEGACY_TOKEN.fullmatch(query.get("from", ""))
        if stream:
            candidates = [row for row in archive.timeline(room_id) if row["stream_ordering"] <= int(stream[1])]
            start_key = max(map(source_key, candidates), default=(0, 0))
        else:
            start_key = (2**63 - 1, 2**63 - 1) if direction == "b" else (0, 0)
    section = json.loads(query.get("filter", "{}"))
    for eid in archive.invalid_history:
        row = archive.events[eid]
        if row["room_id"] != room_id or eid in {item["event_id"] for item in chunk}:
            continue
        position = source_key(row)
        inside = (position <= start_key and (end_key is None or position > end_key)) if direction == "b" else (position > start_key and (end_key is None or position <= end_key))
        if not inside or not filtered([event(row)], section, user_id):
            continue
        insertion = next((index for index, item in enumerate(chunk) if item["event_id"] in archive.events and
                          ((source_key(archive.events[item["event_id"]]) < position) if direction == "b" else
                           (source_key(archive.events[item["event_id"]]) > position))), len(chunk))
        chunk.insert(insertion, event(row))
    limit = min(max(int(query.get("limit", 10)), 0), 100)
    if len(chunk) > limit:
        del chunk[limit:]
        if chunk and chunk[-1]["event_id"] in archive.events:
            last = archive.events[chunk[-1]["event_id"]]
            response["end"] = f"t{last['depth']}-{last['stream_ordering'] - (1 if direction == 'b' else 0)}"
