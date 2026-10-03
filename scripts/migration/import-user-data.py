#!/usr/bin/env python3
"""Offline user/device/E2EE import into an EMPTY, STOPPED Synapse 1.162 database.

No encrypted key material is interpreted or re-encrypted. The optional incomplete
mode is exclusively for staging rehearsal; the final manifest must be complete.
Run with MATRIX_IMPORT_DSN in the environment, never a credential on the command line.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time

sys.path.insert(0, str(Path(__file__).parent / "auth"))
from migration_auth.common import native_token, TOKEN_HASH

PRIORITIES = {"underride": 1, "sender": 2, "room": 3, "content": 4, "override": 5, "postcontent": 6}
OWNED_TABLES = ["migration_legacy_passwords", "access_tokens", "refresh_tokens", "devices", "profiles",
    "user_threepids", "e2e_device_keys_json", "e2e_one_time_keys_json", "e2e_fallback_keys_json",
    "e2e_cross_signing_keys", "e2e_cross_signing_signatures", "e2e_room_keys", "e2e_room_keys_versions",
    "account_data", "room_account_data", "room_tags", "room_tags_revisions", "ignored_users",
    "device_inbox", "device_lists_stream", "push_rules", "push_rules_enable", "push_rules_stream", "pushers", "users"]
INSERT_ORDER = ["users", "profiles", "devices", "refresh_tokens", "access_tokens"] + [
    table for table in OWNED_TABLES if table not in ("users", "profiles", "devices", "refresh_tokens", "access_tokens")]
STREAM_SEQUENCES = {
    "account_data_sequence": ("account_data", [("account_data", "stream_id"), ("room_account_data", "stream_id"), ("room_tags_revisions", "stream_id")]),
    "device_inbox_sequence": ("to_device", [("device_inbox", "stream_id"), ("device_federation_outbox", "stream_id")]),
    "device_lists_sequence": ("device_lists_stream", [(table, "stream_id") for table in (
        "device_lists_stream", "user_signature_stream", "device_lists_outbound_pokes", "device_lists_changes_in_room",
        "device_lists_remote_pending", "device_lists_changes_converted_stream_position")]),
    "e2e_cross_signing_keys_sequence": ("e2e_cross_signing_keys", [("e2e_cross_signing_keys", "stream_id")]),
    "pushers_sequence": ("pushers", [("pushers", "id"), ("deleted_pushers", "stream_id")]),
    "push_rules_stream_sequence": ("push_rules_stream", [("push_rules_stream", "stream_id")]),
}


def json_text(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def rows(source, table):
    if not source.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
        return []
    return [dict(row) for row in source.execute(f'SELECT * FROM "{table}"')]


def load_overlay(path, allow_incomplete):
    if path and Path(path).is_file():
        payload = Path(path).read_bytes()
        return json.loads(payload), True, hashlib.sha256(payload).hexdigest()
    if allow_incomplete:
        return {}, False, None
    raise ValueError("Final import requires the authoritative UserKeysDO, refresh-token and filter exports")


def validate_overlay(source, overlay):
    exported = overlay.get("users")
    if not isinstance(exported, dict):
        raise ValueError("UserKeysDO export is missing the users map")
    for row in rows(source, "users"):
        data = exported.get(row["user_id"])
        if not isinstance(data, dict) or any(not isinstance(data.get(key), expected) for key, expected in
            (("account_data", dict), ("device_keys", dict), ("cross_signing", dict), ("signatures", list))):
            raise ValueError("UserKeysDO export is incomplete for a local user")


def build_import(source, overlay, refresh, secret, now_ms):
    """Pure import plan, exercised without production data by the regression tests."""
    result = {table: [] for table in OWNED_TABLES}
    users = rows(source, "users")
    local_users = {user["user_id"] for user in users}
    active_users = {user["user_id"] for user in users if not user["is_deactivated"]}
    devices = {(device["user_id"], device["device_id"]): device for device in rows(source, "devices")}
    for user in users:
        result["users"].append(dict(name=user["user_id"], password_hash=None, creation_ts=user["created_at"] // 1000,
            admin=int(user["admin"]), is_guest=int(user["is_guest"]), deactivated=int(user["is_deactivated"]), approved=True))
        result["profiles"].append(dict(user_id=user["localpart"], full_user_id=user["user_id"],
            displayname=user["display_name"], avatar_url=user["avatar_url"]))
        if user["password_hash"]:
            result["migration_legacy_passwords"].append(dict(user_id=user["user_id"], password_hash=user["password_hash"]))
    for device in devices.values():
        result["devices"].append(dict(user_id=device["user_id"], device_id=device["device_id"], display_name=device["display_name"],
            last_seen=device["last_seen_ts"], ip=device["last_seen_ip"], hidden=False))
    for row in rows(source, "user_threepids"):
        result["user_threepids"].append({key: row[key] for key in ("user_id", "medium", "address", "validated_at", "added_at")})

    access_rows = rows(source, "access_tokens")
    access_by_id = {row["token_id"]: row for row in access_rows}
    refresh_by_access = {}
    bridge_refresh = []
    for row in refresh.get("tokens", []):
        access = access_by_id.get(row.get("access_token_id"))
        # The legacy server checks the backing session even while a stale KV
        # refresh entry remains. Do not resurrect a logged-out session merely
        # because another session still uses that same device.
        if not access or row["user_id"] not in active_users or (row["user_id"], row["device_id"]) not in devices:
            continue
        if (access["user_id"], access["device_id"]) != (row["user_id"], row["device_id"]):
            raise ValueError("Refresh-token session owner mismatch")
        if row["access_token_id"] in refresh_by_access:
            raise ValueError("Multiple refresh tokens reference one legacy session")
        if not TOKEN_HASH.fullmatch(row["token_hash"]):
            raise ValueError("Invalid refresh-token hash")
        expiry = row.get("expires_at_ms")
        if expiry is None or expiry <= now_ms:
            continue
        native_id = len(result["refresh_tokens"]) + 1
        result["refresh_tokens"].append(dict(id=native_id, user_id=row["user_id"], device_id=row["device_id"],
            token=native_token(secret, row["token_hash"], "refresh"), expiry_ts=expiry, ultimate_session_expiry_ts=None))
        refresh_by_access[row.get("access_token_id")] = native_id
        bridge_refresh.append({key: row[key] for key in ("token_hash", "user_id", "device_id")})
    bridge_access = []
    access_native_ids = {}
    for row in access_rows:
        if row["user_id"] not in local_users or (row["device_id"] and (row["user_id"], row["device_id"]) not in devices):
            raise ValueError("Access token references an unknown user/device")
        native_id = len(result["access_tokens"]) + 1
        result["access_tokens"].append(dict(id=native_id, user_id=row["user_id"], device_id=row["device_id"],
            token=native_token(secret, row["token_hash"]), valid_until_ms=row["expires_at"], last_validated=now_ms,
            refresh_token_id=refresh_by_access.get(row["token_id"]), used=False))
        access_native_ids[row["token_id"]] = native_id
        bridge_access.append({key: row[key] for key in ("token_hash", "user_id", "device_id")})

    account_data = {(row["user_id"], row["room_id"], row["event_type"]): json.loads(row["content"]) for row in rows(source, "account_data")}
    device_keys = {}
    cross_signing = {(row["user_id"], row["key_type"]): json.loads(row["key_data"]) for row in rows(source, "cross_signing_keys")}
    signatures = {(row["signer_user_id"], row["signer_key_id"], row["user_id"], row["key_id"]): row["signature"] for row in rows(source, "cross_signing_signatures")}
    for user_id, data in overlay.get("users", {}).items():
        if user_id not in local_users:
            raise ValueError("UserKeysDO overlay references a nonlocal user")
        for kind, value in data.get("account_data", {}).items():
            account_data[(user_id, "", kind)] = value
        for device_id, value in data.get("device_keys", {}).items():
            if (user_id, device_id) not in devices:
                # Deleted-device orphan keys are retained in the raw export,
                # not resurrected as active devices.
                continue
            if value.get("user_id") != user_id or value.get("device_id") != device_id:
                raise ValueError("Device-key owner mismatch")
            device_keys[(user_id, device_id)] = value
        for kind, value in data.get("cross_signing", {}).items():
            if kind not in ("master", "self_signing", "user_signing") or value.get("user_id") != user_id:
                raise ValueError("Cross-signing key owner/type mismatch")
            cross_signing[(user_id, kind)] = value
        for signature in data.get("signatures", []):
            signatures[(signature["signer_user_id"], signature["signer_key_id"], signature["target_user_id"], signature["target_key_id"])] = signature["signature"]

    def embedded_signatures(user_id, target_id, key):
        for signer, signed in key.get("signatures", {}).items():
            for signing_id, signature in signed.items():
                signatures[(signer, signing_id, user_id, target_id)] = signature

    for (user_id, device_id), key in device_keys.items():
        result["e2e_device_keys_json"].append(dict(user_id=user_id, device_id=device_id, ts_added_ms=now_ms, key_json=json_text(key)))
        embedded_signatures(user_id, device_id, key)
    for index, ((user_id, kind), key) in enumerate(sorted(cross_signing.items()), 1):
        result["e2e_cross_signing_keys"].append(dict(user_id=user_id, keytype=kind, keydata=json_text(key), stream_id=index, instance_name="master"))
        public_keys = key.get("keys", {})
        if len(public_keys) != 1:
            raise ValueError("Cross-signing keys must contain one public key")
        embedded_signatures(user_id, next(iter(public_keys.values())), key)
    for (signer, signing_id, user_id, target_id), signature in signatures.items():
        result["e2e_cross_signing_signatures"].append(dict(user_id=signer, key_id=signing_id,
            target_user_id=user_id, target_device_id=target_id, signature=signature))
    for row in rows(source, "one_time_keys"):
        if row["claimed"] or (row["user_id"], row["device_id"]) not in devices:
            continue
        result["e2e_one_time_keys_json"].append(dict(user_id=row["user_id"], device_id=row["device_id"], algorithm=row["algorithm"],
            key_id=row["key_id"], ts_added_ms=row["created_at"], key_json=json_text(json.loads(row["key_data"]))))
    for row in rows(source, "fallback_keys"):
        if (row["user_id"], row["device_id"]) in devices:
            result["e2e_fallback_keys_json"].append(dict(user_id=row["user_id"], device_id=row["device_id"], algorithm=row["algorithm"],
                key_id=row["key_id"], key_json=json_text(json.loads(row["key_data"])), used=bool(row["used"])))
    for row in rows(source, "key_backup_versions"):
        etag = int(row["etag"]) if str(row["etag"]).isdigit() and int(row["etag"]) < 2**63 else 0
        result["e2e_room_keys_versions"].append(dict(user_id=row["user_id"], version=row["version"], algorithm=row["algorithm"],
            auth_data=json_text(json.loads(row["auth_data"])), deleted=int(row["deleted"]), etag=etag))
    for row in rows(source, "key_backup_keys"):
        result["e2e_room_keys"].append(dict(user_id=row["user_id"], version=row["version"], room_id=row["room_id"], session_id=row["session_id"],
            first_message_index=row["first_message_index"], forwarded_count=row["forwarded_count"], is_verified=bool(row["is_verified"]),
            session_data=json_text(json.loads(row["session_data"]))))

    for index, ((user_id, room_id, kind), content) in enumerate(sorted(account_data.items()), 1):
        if kind == "m.push_rules":
            continue  # Synapse produces this from its authoritative push-rule tables.
        if kind == "m.tag" and room_id:
            for tag, value in content.get("tags", {}).items():
                result["room_tags"].append(dict(user_id=user_id, room_id=room_id, tag=tag, content=json_text(value)))
            result["room_tags_revisions"].append(dict(user_id=user_id, room_id=room_id, stream_id=index, instance_name="master"))
        else:
            row = dict(user_id=user_id, account_data_type=kind, stream_id=index, content=json_text(content), instance_name="master")
            if room_id:
                row["room_id"] = room_id
            result["room_account_data" if room_id else "account_data"].append(row)
        if kind == "m.ignored_user_list" and not room_id:
            for ignored in content.get("ignored_users", {}):
                result["ignored_users"].append(dict(ignorer_user_id=user_id, ignored_user_id=ignored))
    for index, row in enumerate(rows(source, "push_rules"), 1):
        if row["kind"] not in PRIORITIES:
            raise ValueError("Unknown push-rule kind")
        rule_id = f"global/{row['kind']}/{row['rule_id']}"
        result["push_rules"].append(dict(id=index, user_name=row["user_id"], rule_id=rule_id, priority_class=PRIORITIES[row["kind"]],
            priority=row["priority"], conditions=row["conditions"] or "[]", actions=row["actions"]))
        result["push_rules_enable"].append(dict(id=index, user_name=row["user_id"], rule_id=rule_id, enabled=int(row["enabled"])))
    for row in rows(source, "pushers"):
        legacy_access = access_by_id.get(row["access_token_id"])
        result["pushers"].append(dict(id=len(result["pushers"]) + 1, user_name=row["user_id"], access_token=access_native_ids.get(row["access_token_id"]),
            profile_tag=row["profile_tag"] or "", kind=row["kind"], app_id=row["app_id"], app_display_name=row["app_display_name"],
            device_display_name=row["device_display_name"], pushkey=row["pushkey"], ts=row["created_at"], lang=row["lang"], data=row["data"],
            last_success=row["last_success"], failing_since=row["last_failure"] if row["failure_count"] else None,
            enabled=bool(row["enabled"]), device_id=legacy_access["device_id"] if legacy_access else None, instance_name="master"))
    for index, device in enumerate(devices.values(), 1):
        result["device_lists_stream"].append(dict(stream_id=index, user_id=device["user_id"], device_id=device["device_id"], instance_name="master"))
    for row in rows(source, "to_device_messages"):
        if row["delivered"] or row["recipient_user_id"] not in local_users:
            continue
        targets = [device_id for (user_id, device_id) in devices if user_id == row["recipient_user_id"] and (row["recipient_device_id"] == "*" or device_id == row["recipient_device_id"])]
        for device_id in targets:
            result["device_inbox"].append(dict(user_id=row["recipient_user_id"], device_id=device_id, stream_id=len(result["device_inbox"]) + 1,
                message_json=json_text({"sender": row["sender_user_id"], "type": row["event_type"], "content": json.loads(row["content"])}), instance_name="master"))
    return result, bridge_access, bridge_refresh


def write_private(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as file:
        file.write(data)
    os.chmod(path, 0o600)


def insert_plan(cursor, plan):
    # Native access_tokens.refresh_token_id is a real FK. Populate its parent
    # before access tokens; deletion uses the reverse dependency above.
    for table in INSERT_ORDER:
        for entry in plan[table]:
            columns = list(entry)
            cursor.execute(f'INSERT INTO "{table}" ({",".join(columns)}) VALUES ({",".join(["%s"] * len(columns))})', tuple(entry.values()))


def advance_sequences(cursor, sequences=STREAM_SEQUENCES):
    for sequence, (stream_name, tables) in sequences.items():
        # A staging reimport must not move a native counter backwards. Synapse
        # validates its sequence against every table sharing that stream and
        # stream_positions, even when imported rows have smaller IDs.
        cursor.execute(f'SELECT last_value FROM "{sequence}"')
        high = max(cursor.fetchone()[0] or 0, 1)
        cursor.execute("SELECT COALESCE(MAX(stream_id),0) FROM stream_positions WHERE stream_name=%s", (stream_name,))
        high = max(high, cursor.fetchone()[0])
        for table, column in tables:
            cursor.execute(f'SELECT COALESCE(MAX("{column}"),0) FROM "{table}"')
            high = max(high, cursor.fetchone()[0])
        cursor.execute("SELECT setval(%s,%s,true)", (sequence, high))


def run(args):
    import psycopg
    source_path = Path(args.source)
    source = sqlite3.connect(f"file:{source_path.resolve()}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    overlay, keys_complete, keys_sha256 = load_overlay(args.user_keys, args.allow_incomplete_export)
    refresh, refresh_complete, refresh_sha256 = load_overlay(args.refresh_tokens, args.allow_incomplete_export)
    filters, filters_complete, filters_sha256 = load_overlay(args.client_filters, args.allow_incomplete_export)
    if keys_complete:
        validate_overlay(source, overlay)
    compat = Path(args.compat_dir)
    secret_path = compat / "token-secret.bin"
    secret = secret_path.read_bytes() if secret_path.exists() else os.urandom(32)
    write_private(secret_path, secret)
    plan, bridge_access, bridge_refresh = build_import(source, overlay, refresh, secret, int(time.time() * 1000))
    if any(row["name"].split(":", 1)[-1] != args.server_name for row in plan["users"]):
        raise ValueError("Local user domain must match Synapse server_name exactly")
    digest = hashlib.sha256(source_path.read_bytes()).hexdigest()
    with psycopg.connect(os.environ[args.dsn_env]) as target:
        with target.cursor() as cursor:
            cursor.execute("SELECT version FROM schema_version")
            if cursor.fetchone()[0] != 94:
                raise ValueError("Expected initialized Synapse 1.162 schema94")
            cursor.execute("CREATE TABLE IF NOT EXISTS migration_user_import (source_sha256 TEXT NOT NULL, completed BOOLEAN NOT NULL)")
            cursor.execute("SELECT count(*) FROM users")
            occupied = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM migration_user_import")
            previous = cursor.fetchone()[0]
            if occupied and not (args.replace_staging and previous):
                raise ValueError("Refusing to modify an occupied Synapse database; explicit --replace-staging requires an earlier import")
            cursor.execute("CREATE TABLE IF NOT EXISTS migration_legacy_passwords (user_id TEXT PRIMARY KEY,password_hash TEXT NOT NULL)")
            if occupied:
                for table in OWNED_TABLES:
                    cursor.execute(f'DELETE FROM "{table}"')
                cursor.execute("DELETE FROM migration_user_import")
            insert_plan(cursor, plan)
            advance_sequences(cursor)
            cursor.execute("INSERT INTO migration_user_import VALUES (%s,%s)", (digest, keys_complete and refresh_complete and filters_complete))
    bridge_path = compat / "tokens.sqlite"
    if bridge_path.exists():
        bridge_path.unlink()
    bridge = sqlite3.connect(bridge_path)
    for table, entries in (("access_tokens", bridge_access), ("refresh_tokens", bridge_refresh)):
        bridge.execute(f"CREATE TABLE {table} (token_hash TEXT PRIMARY KEY,user_id TEXT NOT NULL,device_id TEXT)")
        bridge.executemany(f"INSERT INTO {table} VALUES (?,?,?)", [(row["token_hash"], row["user_id"], row["device_id"]) for row in entries])
    bridge.commit()
    bridge.execute("CREATE TABLE client_filters (user_id TEXT NOT NULL,filter_id TEXT NOT NULL,filter_json TEXT NOT NULL,PRIMARY KEY(user_id,filter_id))")
    bridge.executemany("INSERT INTO client_filters VALUES (?,?,?)", [(row["user_id"], row["filter_id"], json_text(row["filter"])) for row in filters.get("filters", [])])
    bridge.commit()
    bridge.close()
    os.chmod(bridge_path, 0o600)
    manifest = {"synapse_version": "1.162.0", "source_sha256": digest, "complete": keys_complete and refresh_complete and filters_complete,
        "overlay_sha256": {"user_keys": keys_sha256, "refresh_tokens": refresh_sha256, "client_filters": filters_sha256},
        "counts": {table: len(entries) for table, entries in plan.items()}, "opaque_backup_etags_rebased": sum(not str(row["etag"]).isdigit() for row in rows(source, "key_backup_versions")),
        "ciphertext_sha256": {table: hashlib.sha256(json_text(plan[table]).encode()).hexdigest() for table in ("e2e_device_keys_json", "e2e_cross_signing_keys", "e2e_room_keys")}}
    write_private(compat / "user-import.json", (json_text(manifest) + "\n").encode())
    if not (compat / "gateway.json").exists():
        write_private(compat / "gateway.json", (json_text({"host": "127.0.0.1", "port": 8009, "upstream": "http://127.0.0.1:8008",
            "server_name": args.server_name,
            "token_database": str(bridge_path.resolve()), "secret_file": str(secret_path.resolve())}) + "\n").encode())
    print(json_text({"complete": manifest["complete"], "counts": manifest["counts"], "manifest": str(compat / "user-import.json")}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--server-name", required=True)
    parser.add_argument("--user-keys")
    parser.add_argument("--refresh-tokens")
    parser.add_argument("--client-filters")
    parser.add_argument("--compat-dir", required=True)
    parser.add_argument("--dsn-env", default="MATRIX_IMPORT_DSN")
    parser.add_argument("--allow-incomplete-export", action="store_true")
    parser.add_argument("--replace-staging", action="store_true")
    try:
        run(parser.parse_args())
    except Exception as error:
        # DB exceptions may contain interpolated tokens/key material: never echo
        # their query/detail. Operators can inspect the private database offline.
        print(f"User import failed ({type(error).__name__}); no credentials or key material were logged", file=sys.stderr)
        sys.exit(1)
