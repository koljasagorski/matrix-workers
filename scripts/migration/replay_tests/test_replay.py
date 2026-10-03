import base64
import copy
import hashlib
import importlib.util
import json
import re
import sqlite3
import tempfile
import unittest
from pathlib import Path
from nacl.signing import SigningKey
from canonicaljson import encode_canonical_json

spec = importlib.util.spec_from_file_location("replay", Path(__file__).parents[1] / "replay-federation.py")
replay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(replay)


def transaction(destination, sequence=0):
    event = {"type": "m.room.encrypted", "content": {"ciphertext": "preserved 🔒"}, "signatures": {"m.sgr.ski": {"ed25519:original": "unchanged"}}}
    body = {"origin": "m.sgr.ski", "origin_server_ts": 42, "pdus": [event], "edus": [{"edu_type": "m.device_list_update", "content": {"stream_id": sequence}}]}
    text = replay.compact(body)
    return {"destination": destination, "transaction_id": f"original_{destination.replace('.', '_')}_{sequence}", "sequence": sequence,
            "body_json": text, "body_sha256": hashlib.sha256(text.encode()).hexdigest(), "event_ids": [f"$event-{sequence}"],
            "source_keys": [f"queue:{destination}:$event-{sequence}", f"edu:{destination}:{sequence}"]}


def plan(*rows):
    return {"format": "matrix-workers-federation-replay-v1", "origin": "m.sgr.ski", "source_sha256": "a" * 64, "do_state_sha256": "b" * 64, "transactions": list(rows)}


class Response:
    def __init__(self, status=200, body=None, error=None, headers=None):
        self.status, self.error, self.headers = status, error, headers or {}
        self.data = replay.compact(body if body is not None else {"pdus": {"$event-0": {}}}).encode()
        self.content = self

    async def __aenter__(self):
        if self.error:
            raise self.error
        return self

    async def __aexit__(self, *args):
        return False

    async def iter_chunked(self, _size):
        yield self.data


class Session:
    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.calls = []

    def put(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0) if self.responses else Response()

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


class ReplayTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "journal.sqlite"
        self.journal = replay.Journal(self.path)
        self.key = SigningKey(b"k" * 32)

    def tearDown(self):
        self.journal.connection.close()
        self.temp.cleanup()

    async def test_network_retry_survives_reopen_with_identical_original_body_and_id(self):
        original = transaction("offline.example")
        self.journal.import_plan(plan(original))
        endpoints = {"offline.example": "https://offline.example:8448"}
        session = Session([Response(error=ConnectionResetError()), Response()])
        await replay.replay_once(self.journal, session, "m.sgr.ski", "ed25519:original", self.key, endpoints, 1000)
        self.journal.connection.close()
        self.journal = replay.Journal(self.path)
        self.journal.import_plan(plan(original))
        await replay.replay_once(self.journal, session, "m.sgr.ski", "ed25519:original", self.key, endpoints, 60999)
        self.assertEqual(len(session.calls), 1)
        await replay.replay_once(self.journal, session, "m.sgr.ski", "ed25519:original", self.key, endpoints, 61000)
        self.assertEqual(session.calls[0], session.calls[1])
        url, options = session.calls[1]
        self.assertTrue(url.endswith("/send/" + original["transaction_id"]))
        self.assertEqual(options["data"], original["body_json"].encode())
        self.assertFalse(options["allow_redirects"])
        signature = re.search('sig="([^"]+)"', options["headers"]["Authorization"])[1]
        document = {"method": "PUT", "uri": "/_matrix/federation/v1/send/" + original["transaction_id"], "origin": "m.sgr.ski", "destination": "offline.example", "content": json.loads(original["body_json"])}
        self.key.verify_key.verify(encode_canonical_json(document), base64.b64decode(signature + "=" * (-len(signature) % 4)))
        self.assertEqual(self.journal.counts(), {"complete": 1})
        self.assertEqual(self.journal.connection.execute("PRAGMA journal_mode").fetchone()[0], "delete")

    async def test_failed_destination_keeps_order_without_blocking_other_destinations(self):
        self.journal.import_plan(plan(transaction("a.example"), transaction("a.example", 1), transaction("b.example")))
        session = Session([Response(status=523), Response()])
        endpoints = {host: "https://" + host + ":8448" for host in ("a.example", "b.example")}
        await replay.replay_once(self.journal, session, "m.sgr.ski", "ed25519:original", self.key, endpoints, 0)
        self.assertEqual(self.journal.counts(), {"pending": 2, "complete": 1})
        self.assertEqual(len(session.calls), 2)
        await replay.replay_once(self.journal, session, "m.sgr.ski", "ed25519:original", self.key, endpoints, 60000)
        await replay.replay_once(self.journal, session, "m.sgr.ski", "ed25519:original", self.key, endpoints, 60000)
        self.assertTrue(session.calls[2][0].endswith("original_a_example_0"))
        self.assertTrue(session.calls[3][0].endswith("original_a_example_1"))
        self.assertEqual(self.journal.counts(), {"complete": 3})

    async def test_http200_pdu_rejection_is_retained_and_releases_next_transaction(self):
        self.journal.import_plan(plan(transaction("peer.example"), transaction("peer.example", 1)))
        session = Session([Response(body={"pdus": {"$event-0": {"error": "rejected original event"}}}), Response()])
        endpoints = {"peer.example": "https://peer.example:8448"}
        await replay.replay_once(self.journal, session, "m.sgr.ski", "ed25519:original", self.key, endpoints, 0)
        row = self.journal.connection.execute("SELECT * FROM replay_transactions WHERE sequence=0").fetchone()
        self.assertEqual(json.loads(row["response_json"])["pdus"]["$event-0"]["error"], "rejected original event")
        await replay.replay_once(self.journal, session, "m.sgr.ski", "ed25519:original", self.key, endpoints, 0)
        self.assertEqual(self.journal.counts(), {"complete": 2})

    def test_unicode_custom_keys_use_official_matrix_canonical_json(self):
        original = transaction("peer.example")
        body = json.loads(original["body_json"])
        body["pdus"][0]["content"]["\ue000"] = "BMP key"
        body["pdus"][0]["content"]["😀"] = "non-BMP key"
        original["body_json"] = replay.compact(body)
        uri, header = replay.authorization(original, "m.sgr.ski", "ed25519:original", self.key)
        signature = re.search('sig="([^"]+)"', header)[1]
        document = {"method": "PUT", "uri": uri, "origin": "m.sgr.ski", "destination": "peer.example", "content": body}
        canonical = encode_canonical_json(document)
        self.assertLess(canonical.index("\ue000".encode()), canonical.index("😀".encode()))
        self.key.verify_key.verify(canonical, base64.b64decode(signature + "=" * (-len(signature) % 4)))

    async def test_missing_acknowledgement_or_oversized_response_stays_pending(self):
        self.journal.import_plan(plan(transaction("peer.example")))
        endpoints = {"peer.example": "https://peer.example:8448"}
        await replay.replay_once(self.journal, Session([Response(body={"error": "not an ACK"})]), "m.sgr.ski", "ed25519:original", self.key, endpoints, 0)
        self.assertEqual(self.journal.counts(), {"pending": 1})
        response = Response()
        response.data = b"x" * 2097153
        await replay.replay_once(self.journal, Session([response]), "m.sgr.ski", "ed25519:original", self.key, endpoints, 60000)
        self.assertEqual(self.journal.counts(), {"pending": 1})

    def test_immutable_plan_and_import_rollback(self):
        original = plan(transaction("peer.example"))
        self.journal.import_plan(original)
        changed = copy.deepcopy(original)
        changed["transactions"][0]["sequence"] = 1
        with self.assertRaises(ValueError):
            self.journal.import_plan(changed)
        self.assertEqual(self.journal.counts(), {"pending": 1})
        self.journal.connection.close()
        self.path.unlink()
        self.journal = replay.Journal(self.path)
        invalid = plan(transaction("peer.example"), transaction("peer.example", 2))
        with self.assertRaises(ValueError):
            self.journal.import_plan(invalid)
        self.assertEqual(self.journal.counts(), {})
        self.assertEqual(self.journal.connection.execute("SELECT count(*) FROM replay_sources").fetchone()[0], 0)

    def test_restart_preserves_native_readonly_group_access(self):
        self.journal.connection.close()
        self.path.chmod(0o640)
        self.journal = replay.Journal(self.path)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o640)

    async def test_freeze_and_native_gate_are_required_before_any_network(self):
        source = Path(self.temp.name) / "source.sqlite"
        key = self.key
        encode = lambda value: base64.urlsafe_b64encode(value).decode().rstrip("=")
        jwk = {"kty": "OKP", "crv": "Ed25519", "d": encode(bytes(key)), "x": encode(bytes(key.verify_key))}
        with sqlite3.connect(source) as database:
            database.execute("CREATE TABLE server_keys(key_id,private_key_jwk,public_key,is_current,key_version)")
            database.execute("INSERT INTO server_keys VALUES(?,?,?,1,2)", ("ed25519:original", json.dumps(jwk), encode(bytes(key.verify_key))))
        signing_file = Path(self.temp.name) / "private/key.json"
        replay.export_signing_key(source, signing_file)
        source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
        value = plan(transaction("peer.example"))
        value["source_sha256"] = source_hash
        plan_file, proof_file = Path(self.temp.name) / "plan.json", Path(self.temp.name) / "proof.json"
        plan_file.write_text(json.dumps(value))
        proof_file.write_text(json.dumps({"source_sha256": source_hash, "server_name": "m.sgr.ski"}))
        config = {"signing_key": str(signing_file), "plan": str(plan_file), "proof": str(proof_file), "journal": str(Path(self.temp.name) / "final/journal.sqlite")}
        with self.assertRaisesRegex(ValueError, "freeze"):
            await replay.run(config, import_only=True)
        proof_file.write_text(json.dumps({"source_sha256": source_hash, "server_name": "m.sgr.ski", "consistent_frozen_snapshot": True}))
        self.assertEqual(await replay.run(config, import_only=True), {"pending": 1})
        with self.assertRaisesRegex(ValueError, "gate"):
            await replay.run(config, once=True)
        self.assertEqual(signing_file.stat().st_mode & 0o777, 0o600)

    async def test_redirects_are_bounded_and_block_private_metadata_destinations(self):
        for target in ("http://peer.example", "https://127.0.0.1", "https://[::ffff:127.0.0.1]", "https://169.254.169.254", "https://sub.localhost.", "https://a.metadata", "https://user:pass@peer.example", "https://peer.example:5432"):
            with self.subTest(target=target), self.assertRaises(ValueError):
                replay.validate_url(target)
        session = Session([Response(status=302, headers={"Location": "https://127.0.0.1/"})])
        with self.assertRaises(ValueError):
            await replay.discover(session, "peer.example")
        self.assertEqual(len(session.calls), 1)
        session = Session([Response(status=302, headers={"Location": "/delegate"}), Response(body={"m.server": "federation.peer.example:443"})])
        self.assertEqual(await replay.discover(session, "peer.example"), "https://federation.peer.example:443")
        self.assertEqual(len(session.calls), 2)

    async def test_dynamic_enable_flag_defaults_paused_and_reload_stops_later_destinations(self):
        proof_file = Path(self.temp.name) / "live-proof.json"
        proof = {"source_sha256": "a" * 64, "server_name": "m.sgr.ski", "consistent_frozen_snapshot": True, "native_federation_replay_gate_verified": True}
        proof_file.write_text(json.dumps(proof))
        self.journal.import_plan(plan(transaction("a.example"), transaction("b.example")))
        enabled = lambda: replay.replay_enabled(proof_file, "a" * 64)
        session = Session()
        endpoints = {host: "https://" + host + ":8448" for host in ("a.example", "b.example")}
        await replay.replay_once(self.journal, session, "m.sgr.ski", "ed25519:original", self.key, endpoints, 0, should_send=enabled)
        self.assertEqual(session.calls, [])
        proof["federation_replay_enabled"] = True
        proof_file.write_text(json.dumps(proof))
        self.assertTrue(enabled())
        original_put = session.put
        def pause_after_first_request(*args, **kwargs):
            response = original_put(*args, **kwargs)
            proof["federation_replay_enabled"] = False
            proof_file.write_text(json.dumps(proof))
            return response
        session.put = pause_after_first_request
        await replay.replay_once(self.journal, session, "m.sgr.ski", "ed25519:original", self.key, endpoints, 0, should_send=enabled)
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(self.journal.counts(), {"complete": 1, "pending": 1})
        proof_file.write_text("{not complete JSON")
        self.assertFalse(enabled())
        proof_file.unlink()
        self.assertFalse(enabled())


if __name__ == "__main__":
    unittest.main()
