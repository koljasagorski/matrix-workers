import copy
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parents[2]))
from rooms.plan import build_plan, json_text, validate_event
from rooms.state import build_states
from rooms.postgres import event_format
from rooms.archive_export import export_archive
from synapse.api.room_versions import KNOWN_ROOM_VERSIONS
from synapse.crypto.event_signing import add_hashes_and_signatures
from synapse.events import make_event_from_dict
from signedjson.key import generate_signing_key


class NativePlanTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "source.sqlite"
        self.source = sqlite3.connect(self.path)
        self.source.executescript('''
        CREATE TABLE rooms(room_id TEXT,room_version TEXT,is_public INTEGER,creator_id TEXT,created_at INTEGER);
        CREATE TABLE events(event_id TEXT,room_id TEXT,sender TEXT,event_type TEXT,state_key TEXT,content TEXT,origin_server_ts INTEGER,unsigned TEXT,depth INTEGER,auth_events TEXT,prev_events TEXT,hashes TEXT,signatures TEXT,stream_ordering INTEGER,redacts TEXT);
        CREATE TABLE event_state_archive(event_id TEXT,room_id TEXT,event_json TEXT);
        CREATE TABLE event_state_snapshots(event_id TEXT,room_id TEXT,state_before TEXT);
        CREATE TABLE room_state(room_id TEXT,event_type TEXT,state_key TEXT,event_id TEXT);
        CREATE TABLE room_memberships(room_id TEXT,user_id TEXT,membership TEXT,event_id TEXT);
        CREATE TABLE room_aliases(alias TEXT,room_id TEXT,creator_id TEXT);
        CREATE TABLE receipts(room_id TEXT,user_id TEXT,receipt_type TEXT,event_id TEXT,thread_id TEXT,ts INTEGER);
        CREATE TABLE account_data(user_id TEXT,room_id TEXT,event_type TEXT,content TEXT);
        CREATE TABLE users(user_id TEXT,password_hash TEXT,is_deactivated INTEGER DEFAULT 0);
        ''')
        self.version = KNOWN_ROOM_VERSIONS["12"]
        self.key = generate_signing_key("fixture")
        self.user = "@alice:example.org"
        create = self.signed("m.room.create", {"room_version": "12"}, 1, [], [], state_key="")
        self.room_id = make_event_from_dict(create, self.version).room_id
        self.create_id = self.store(create, 1)
        self.member_id = self.store(self.signed("m.room.member", {"membership": "join"}, 2,
            [self.create_id], [self.create_id], state_key=self.user), 2)
        self.ciphertext = {"algorithm": "m.megolm.v1.aes-sha2", "ciphertext": "UNMODIFIED+/=", "session_id": "session"}
        self.message_id = self.store(self.signed("m.room.encrypted", self.ciphertext, 3,
            [self.create_id, self.member_id], [self.member_id]), 3)
        self.source.execute("INSERT INTO rooms VALUES (?,?,?,?,?)", (self.room_id, "12", 0, self.user, 1))
        for kind, state_key, eid in [("m.room.create", "", self.create_id), ("m.room.member", self.user, self.member_id)]:
            self.source.execute("INSERT INTO room_state VALUES (?,?,?,?)", (self.room_id, kind, state_key, eid))
        self.source.execute("INSERT INTO room_memberships VALUES (?,?,?,?)", (self.room_id, self.user, "join", self.member_id))
        self.source.commit()

    def tearDown(self):
        self.source.close()
        self.directory.cleanup()

    def signed(self, kind, content, depth, auth, prev, **extra):
        payload = dict(sender=self.user, type=kind, content=content, depth=depth,
            origin_server_ts=1000 + depth, auth_events=auth, prev_events=prev, **extra)
        if kind != "m.room.create":
            payload["room_id"] = self.room_id
        add_hashes_and_signatures(self.version, payload, "example.org", self.key)
        return payload

    def store(self, payload, stream):
        parsed = make_event_from_dict(payload, self.version)
        self.source.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            parsed.event_id, parsed.room_id, payload["sender"], payload["type"], payload.get("state_key"), json_text(payload["content"]),
            payload["origin_server_ts"], None, payload["depth"], json_text(payload["auth_events"]), json_text(payload["prev_events"]),
            json_text(payload["hashes"]), json_text(payload["signatures"]), stream, None))
        return parsed.event_id

    def test_native_id_and_ciphertext_unchanged_and_create_wire_omits_room_id(self):
        plan = build_plan(self.path)
        self.assertEqual(len(plan["native_events"]), 3)
        self.assertEqual(plan["native_events"][self.message_id]["wire"]["content"], self.ciphertext)
        self.assertNotIn("room_id", plan["native_events"][self.create_id]["wire"])
        after, reconstructed = build_states(plan)
        self.assertEqual(after[self.message_id][("m.room.member", self.user)], self.member_id)
        self.assertIn(self.message_id, reconstructed)

    def test_database_codec_matches_pinned_native_room_version(self):
        self.assertEqual(event_format("10"), 3)
        self.assertEqual(event_format("11"), 3)
        self.assertEqual(event_format("12"), 4)
        plan = build_plan(self.path)
        for record in plan["native_events"].values():
            parsed = make_event_from_dict(record["wire"], self.version)
            self.assertEqual(event_format("12"), parsed.format_version)

    def test_malformed_history_excluded_without_changing_current_membership(self):
        payload = self.signed("m.room.member", {"membership": "leave"}, 4,
            [self.create_id, self.member_id], [self.message_id], state_key=self.user)
        bad_id = self.store(payload, 4)
        self.source.execute("UPDATE events SET event_id='$unsigned-random',hashes='{}',signatures='{}' WHERE event_id=?", (bad_id,))
        self.source.commit()
        plan = build_plan(self.path)
        self.assertIn("$unsigned-random", plan["invalid_events"])
        self.assertNotIn("$unsigned-random", plan["native_events"])
        self.assertEqual(plan["states"][self.room_id][("m.room.member", self.user)], self.member_id)
        self.assertEqual(plan["legacy_rooms"], set())

    def test_invalid_current_create_archives_entire_room_instead_of_rewriting_ids(self):
        self.source.execute("UPDATE events SET hashes='{}',signatures='{}' WHERE event_id=?", (self.create_id,))
        self.source.commit()
        plan = build_plan(self.path)
        self.assertEqual(plan["legacy_rooms"], {self.room_id})
        self.assertEqual(plan["native_events"], {})
        self.assertEqual(plan["events"][self.message_id]["payload"]["content"], self.ciphertext)

    def test_sanitized_archive_preserves_ciphertext_without_credentials_or_global_keys(self):
        self.source.execute("UPDATE events SET hashes='{}',signatures='{}' WHERE event_id=?", (self.create_id,))
        self.source.execute("INSERT INTO users(user_id,password_hash) VALUES (?,?)", (self.user, "PRIVATE-PASSWORD-HASH"))
        self.source.execute("INSERT INTO account_data VALUES (?,?,?,?)", (self.user, "", "m.secret_storage.key.fixture", '{"secret":"private-global"}'))
        self.source.execute("INSERT INTO account_data VALUES (?,?,?,?)", (self.user, self.room_id, "m.fully_read", json_text({"event_id": self.message_id})))
        self.source.commit()
        plan = build_plan(self.path)
        target = Path(self.directory.name) / "archive.sqlite"
        result = export_archive(self.path, target, plan)
        self.assertEqual(result["archive_rooms"], 1)
        with sqlite3.connect(target) as archive:
            self.assertFalse(archive.execute("SELECT 1 FROM sqlite_master WHERE name='users'").fetchone())
            self.assertEqual(archive.execute("SELECT COUNT(*) FROM account_data").fetchone()[0], 1)
            self.assertEqual(json.loads(archive.execute("SELECT content FROM events WHERE event_id=?", (self.message_id,)).fetchone()[0]), self.ciphertext)
            self.assertEqual(archive.execute("SELECT source_sha256 FROM archive_provenance").fetchone()[0], plan["source_sha256"])

    def test_inconsistent_current_membership_and_cross_room_state_fail_closed(self):
        self.source.execute("UPDATE room_memberships SET membership='leave'")
        self.source.commit()
        with self.assertRaisesRegex(ValueError, "Current membership"):
            build_plan(self.path)
        self.source.execute("UPDATE room_memberships SET membership='join'")
        self.source.execute("UPDATE room_state SET event_type='m.room.name' WHERE event_type='m.room.create'")
        self.source.commit()
        with self.assertRaises(ValueError):
            build_plan(self.path)


if __name__ == "__main__":
    unittest.main()
