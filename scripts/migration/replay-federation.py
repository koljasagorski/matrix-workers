#!/usr/bin/env python3
"""Persist and resume original federation transactions; never edit/resign PDUs.

Run only after the final source freeze and transport gate are verified. Network
request signatures use the preserved original server key. All logs are counts,
HTTP statuses or exception type names, never payloads/credentials.
"""
import argparse
import asyncio
import base64
import contextlib
import fcntl
import hashlib
import ipaddress
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from urllib.parse import urljoin, urlsplit


def compact(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


class Journal:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o777 not in (0o600, 0o640):
                raise ValueError("Unsafe existing journal file or permissions")
        else:
            os.close(descriptor)
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        # DELETE mode allows the native UID to read the journal through a read-only
        # directory mount without receiving write access to WAL/SHM sidecars.
        self.connection.execute("PRAGMA journal_mode=DELETE")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript("""
          CREATE TABLE IF NOT EXISTS replay_transactions (
            destination TEXT NOT NULL, transaction_id TEXT NOT NULL, sequence INTEGER NOT NULL,
            body_json TEXT NOT NULL, body_sha256 TEXT NOT NULL, event_ids TEXT NOT NULL,
            source_keys TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','complete')),
            attempts INTEGER NOT NULL DEFAULT 0, next_retry_ms INTEGER NOT NULL DEFAULT 0,
            last_http_status INTEGER, last_error_type TEXT, response_json TEXT,
            PRIMARY KEY(destination,transaction_id), UNIQUE(destination,sequence));
          CREATE TABLE IF NOT EXISTS replay_sources (source_sha256 TEXT NOT NULL, do_state_sha256 TEXT NOT NULL, plan_sha256 TEXT NOT NULL);
        """)

    def import_plan(self, plan):
        if plan.get("format") != "matrix-workers-federation-replay-v1" or plan.get("origin") != "m.sgr.ski":
            raise ValueError("Invalid original queue plan")
        for field in ("source_sha256", "do_state_sha256"):
            if not isinstance(plan.get(field), str) or not re.fullmatch(r"[0-9a-f]{64}", plan[field]):
                raise ValueError("Invalid source snapshot identity")
        if not isinstance(plan.get("transactions"), list):
            raise ValueError("Missing original transactions")
        plan_hash = hashlib.sha256(compact(plan).encode()).hexdigest()
        identities = set()
        sequences = {}
        queue_keys = set()
        with self.connection:
            sources = self.connection.execute("SELECT * FROM replay_sources").fetchall()
            if sources and any(row["source_sha256"] != plan["source_sha256"] or row["do_state_sha256"] != plan["do_state_sha256"] or row["plan_sha256"] != plan_hash for row in sources):
                raise ValueError("Refusing another source snapshot in an existing replay journal")
            if not sources:
                self.connection.execute("INSERT INTO replay_sources VALUES (?,?,?)", (plan["source_sha256"], plan["do_state_sha256"], plan_hash))
            for row in plan["transactions"]:
                key = (row["destination"], row["transaction_id"])
                if key in identities:
                    raise ValueError("Duplicate transaction identity")
                identities.add(key)
                validate_server(row["destination"])
                if row["destination"] == plan["origin"] or type(row.get("sequence")) is not int or row["sequence"] < 0:
                    raise ValueError("Invalid transaction sequence/destination")
                sequences.setdefault(row["destination"], []).append(row["sequence"])
                if not re.fullmatch(r"[A-Za-z0-9_-]+", row["transaction_id"]):
                    raise ValueError("Invalid original transaction ID")
                actual = hashlib.sha256(row["body_json"].encode()).hexdigest()
                if actual != row["body_sha256"]:
                    raise ValueError("Transaction payload checksum mismatch")
                body = json.loads(row["body_json"])
                if not isinstance(body, dict) or set(body) != {"origin", "origin_server_ts", "pdus", "edus"} or body.get("origin") != plan["origin"] or type(body.get("origin_server_ts")) is not int or body["origin_server_ts"] < 0 or not isinstance(body.get("pdus"), list) or not isinstance(body.get("edus"), list) or len(body["pdus"]) > 50 or len(body["edus"]) > 100 or not body["pdus"] and not body["edus"]:
                    raise ValueError("Invalid original transaction body")
                if not isinstance(row.get("event_ids"), list) or not isinstance(row.get("source_keys"), list) or len(row["event_ids"]) != len(body["pdus"]) or len(row["source_keys"]) != len(body["pdus"]) + len(body["edus"]):
                    raise ValueError("Original queue references do not match transaction body")
                for source_key in row["source_keys"]:
                    if not isinstance(source_key, str) or not source_key.startswith(("queue:", "edu:")) or source_key in queue_keys:
                        raise ValueError("Duplicate or invalid original queue reference")
                    queue_keys.add(source_key)
                previous = self.connection.execute("SELECT body_json,body_sha256 FROM replay_transactions WHERE destination=? AND transaction_id=?", key).fetchone()
                if previous and (previous["body_json"] != row["body_json"] or previous["body_sha256"] != actual):
                    raise ValueError("Transaction ID cannot be reused with another payload")
                self.connection.execute("""INSERT OR IGNORE INTO replay_transactions
                    (destination,transaction_id,sequence,body_json,body_sha256,event_ids,source_keys) VALUES (?,?,?,?,?,?,?)""",
                    (*key, row["sequence"], row["body_json"], actual, compact(row["event_ids"]), compact(row["source_keys"])))
            if any(sorted(values) != list(range(len(values))) for values in sequences.values()):
                raise ValueError("Original transaction sequence has gaps or duplicates")
            if self.connection.execute("SELECT count(*) FROM replay_transactions").fetchone()[0] != len(identities):
                raise ValueError("Replay journal contains transactions outside the final snapshot")

    def due(self, now_ms):
        return self.connection.execute("""SELECT * FROM replay_transactions t WHERE status='pending' AND next_retry_ms<=?
            AND NOT EXISTS (SELECT 1 FROM replay_transactions earlier WHERE earlier.destination=t.destination
              AND earlier.status='pending' AND earlier.sequence<t.sequence) ORDER BY destination""", (now_ms,)).fetchall()

    def fail(self, row, now_ms, error_type, status=None):
        attempts = row["attempts"] + 1
        delay = min(60000 * 2 ** min(attempts - 1, 6), 3600000)
        with self.connection:
            self.connection.execute("""UPDATE replay_transactions SET attempts=?,next_retry_ms=?,last_http_status=?,last_error_type=?
                WHERE destination=? AND transaction_id=? AND status='pending'""",
                (attempts, now_ms + delay, status, error_type, row["destination"], row["transaction_id"]))

    def acknowledge(self, row, response):
        if not isinstance(response, dict) or not isinstance(response.get("pdus"), dict):
            raise ValueError("Invalid federation acknowledgement")
        with self.connection:
            self.connection.execute("""UPDATE replay_transactions SET status='complete',last_http_status=200,
                last_error_type=NULL,response_json=? WHERE destination=? AND transaction_id=?""",
                (compact(response), row["destination"], row["transaction_id"]))

    def counts(self):
        return dict(self.connection.execute("SELECT status,count(*) FROM replay_transactions GROUP BY status").fetchall())

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.connection.close()


def validate_server(server):
    if not isinstance(server, str) or not server or re.search(r'[\s"/@?#\\]', server):
        raise ValueError("Invalid federation server")
    validate_url("https://" + server + "/")


def validate_url(url):
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower().rstrip(".")
    if parsed.scheme != "https" or not host or parsed.username or parsed.password:
        raise ValueError("Unsafe federation URL")
    if host in ("localhost", "internal", "metadata") or any(host.endswith("." + suffix) for suffix in ("localhost", "internal", "local", "metadata")):
        raise ValueError("Private federation host")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address and (not address.is_global or (address.ipv4_mapped and not address.ipv4_mapped.is_global if address.version == 6 else False)):
        raise ValueError("Private federation address")
    if parsed.port in (22, 23, 25, 53, 135, 139, 445, 1433, 1521, 3306, 3389, 5432, 5900, 6379, 9200, 27017):
        raise ValueError("Unsafe federation port")
    return parsed


async def bounded_json(response, limit=2097152):
    data = bytearray()
    async for chunk in response.content.iter_chunked(65536):
        data.extend(chunk)
        if len(data) > limit:
            raise ValueError("Oversized federation response")
    return json.loads(data)


async def discover(session, destination):
    validate_server(destination)
    parsed = urlsplit("https://" + destination)
    if parsed.port:
        return "https://" + destination
    current = "https://" + destination + "/.well-known/matrix/server"
    visited = set()
    try:
        async with asyncio.timeout(10):
            for redirect in range(6):
                validate_url(current)
                if current in visited:
                    break
                visited.add(current)
                async with session.get(current, allow_redirects=False) as response:
                    if response.status in (301, 302, 303, 307, 308):
                        location = response.headers.get("Location")
                        if not location or redirect == 5:
                            break
                        current = urljoin(current, location)
                        continue
                    if response.status == 200:
                        delegated = (await bounded_json(response, 65536)).get("m.server")
                        validate_server(delegated)
                        resolved = urlsplit("https://" + delegated)
                        # No explicit delegated port requires SRV resolution. Use
                        # Synapse's full transport for such destinations instead
                        # of guessing an endpoint and leaking a signed payload.
                        if resolved.port:
                            return "https://" + delegated
                    break
    except (TimeoutError, ValueError, OSError):
        pass
    # Fallback8448 is safe only when no SRV exists; the operator's pinned
    # endpoint for the previously-discovered offline peer makes this explicit.
    raise ValueError("Discovery needs an explicitly validated endpoint")


def read_source_key(source):
    with contextlib.closing(sqlite3.connect(Path(source).resolve().as_uri() + "?mode=ro&immutable=1", uri=True)) as connection:
        row = connection.execute("SELECT key_id,private_key_jwk,public_key FROM server_keys WHERE is_current=1 AND key_version=2").fetchone()
    if not row:
        raise ValueError("Original signing key unavailable")
    return {"key_id": row[0], "private_key_jwk": json.loads(row[1]), "public_key": row[2], "source_sha256": hashlib.sha256(Path(source).read_bytes()).hexdigest()}


def signing_key_document(document):
    from nacl.signing import SigningKey
    jwk = document["private_key_jwk"]
    if jwk.get("kty") != "OKP" or jwk.get("crv") != "Ed25519":
        raise ValueError("Invalid original signing key")
    decode = lambda value: base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    key = SigningKey(decode(jwk["d"]))
    if bytes(key.verify_key) != decode(jwk["x"]) or bytes(key.verify_key) != decode(document["public_key"]):
        raise ValueError("Original signing identity mismatch")
    if not re.fullmatch(r"ed25519:[A-Za-z0-9_]+", document["key_id"]):
        raise ValueError("Invalid original signing key ID")
    return document["key_id"], key


def export_signing_key(source, output):
    document = read_source_key(source)
    signing_key_document(document)
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with os.fdopen(os.open(output, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600), "w") as handle:
        os.fchmod(handle.fileno(), 0o600)
        handle.write(compact(document) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return {"exported_signing_identity": True}


def authorization(row, origin, key_id, key):
    from canonicaljson import encode_canonical_json
    uri = "/_matrix/federation/v1/send/" + row["transaction_id"]
    document = {"method": "PUT", "uri": uri, "origin": origin, "destination": row["destination"], "content": json.loads(row["body_json"])}
    canonical = encode_canonical_json(document)
    signature = base64.b64encode(key.sign(canonical).signature).decode().rstrip("=")
    return uri, f'X-Matrix origin="{origin}",destination="{row["destination"]}",key="{key_id}",sig="{signature}"'


def replay_enabled(proof_path, source_sha256):
    try:
        proof = json.loads(Path(proof_path).read_text())
        return proof.get("source_sha256") == source_sha256 and proof.get("server_name") == "m.sgr.ski" and all(proof.get(field) is True for field in (
            "consistent_frozen_snapshot", "native_federation_replay_gate_verified", "federation_replay_enabled"))
    except (ValueError, OSError, AttributeError):
        return False


async def replay_once(journal, session, origin, key_id, key, endpoints, now_ms=None, should_send=None):
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    for row in journal.due(now_ms):
        if should_send is not None and not should_send():
            break
        try:
            endpoint = endpoints.get(row["destination"]) or await discover(session, row["destination"])
            parsed = validate_url(endpoint)
            if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
                raise ValueError("Federation endpoint must be an HTTPS origin")
            if should_send is not None and not should_send():
                break
            uri, header = authorization(row, origin, key_id, key)
            async with session.put(endpoint.rstrip("/") + uri, data=row["body_json"].encode(), headers={"Authorization": header, "Content-Type": "application/json"}, allow_redirects=False) as response:
                if response.status != 200:
                    journal.fail(row, now_ms, "HTTP", response.status)
                    continue
                journal.acknowledge(row, await bounded_json(response))
        except Exception as error:
            journal.fail(row, now_ms, type(error).__name__)
    return journal.counts()


async def run(config, once=False, import_only=False):
    import aiohttp
    proof = json.loads(Path(config["proof"]).read_text())
    plan = json.loads(Path(config["plan"]).read_text())
    document = json.loads(Path(config["signing_key"]).read_text()) if config.get("signing_key") else read_source_key(config["source"])
    if proof.get("consistent_frozen_snapshot") is not True or proof.get("server_name") != "m.sgr.ski" or proof.get("source_sha256") != plan["source_sha256"] or document.get("source_sha256") != plan["source_sha256"]:
        raise ValueError("Final source freeze/source checksum proof missing")
    if not import_only and proof.get("native_federation_replay_gate_verified") is not True:
        raise ValueError("Native per-destination transaction ordering gate missing")
    key_id, key = signing_key_document(document)
    lock_path = config["journal"] + ".lock"
    Path(lock_path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with open(lock_path, "a") as lock:
        os.fchmod(lock.fileno(), 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with Journal(config["journal"]) as journal:
            journal.import_plan(plan)
            if import_only:
                return journal.counts()

            class PublicResolver(aiohttp.abc.AbstractResolver):
                def __init__(self):
                    self.parent = aiohttp.resolver.DefaultResolver()

                async def resolve(self, host, port=0, family=0):
                    rows = await self.parent.resolve(host, port, family)
                    if not rows or any(not ipaddress.ip_address(row["host"]).is_global for row in rows):
                        raise ValueError("Resolved federation address is not public")
                    return rows

                async def close(self):
                    await self.parent.close()

            connector = aiohttp.TCPConnector(resolver=PublicResolver(), use_dns_cache=False)
            async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=45, sock_connect=10), auto_decompress=True) as session:
                while True:
                    enabled = lambda: replay_enabled(config["proof"], plan["source_sha256"])
                    counts = await replay_once(journal, session, "m.sgr.ski", key_id, key, config.get("endpoints", {}), should_send=enabled) if enabled() else journal.counts()
                    if once:
                        return counts
                    await asyncio.sleep(15)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config")
    parser.add_argument("--export-signing-key")
    parser.add_argument("--signing-key-output")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--import-only", action="store_true")
    options = parser.parse_args()
    try:
        if options.export_signing_key:
            if not options.signing_key_output or options.config:
                raise ValueError("Expected separate signing-key output and no replay configuration")
            result = export_signing_key(options.export_signing_key, options.signing_key_output)
        else:
            if not options.config:
                raise ValueError("Replay configuration required")
            result = asyncio.run(run(json.loads(Path(options.config).read_text()), options.once, options.import_only))
        print(compact(result))
    except Exception as error:
        print("Federation replay failed: " + type(error).__name__)
        raise SystemExit(1)
