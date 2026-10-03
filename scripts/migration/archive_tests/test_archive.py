import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

sys.path.insert(0, str(Path(__file__).parents[1]))
sys.path.insert(0, str(Path(__file__).parents[1] / "auth"))
import archive
from migration_auth.gateway import create_app


class ArchiveTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        source = sqlite3.connect(self.root / "source.sqlite")
        source.executescript('''
        CREATE TABLE events(event_id TEXT,room_id TEXT,sender TEXT,event_type TEXT,state_key TEXT,content TEXT,origin_server_ts INTEGER,unsigned TEXT,depth INTEGER,stream_ordering INTEGER,redacts TEXT);
        CREATE TABLE room_state(room_id TEXT,event_type TEXT,state_key TEXT,event_id TEXT);
        CREATE TABLE room_memberships(room_id TEXT,user_id TEXT,membership TEXT);
        CREATE TABLE account_data(user_id TEXT,room_id TEXT,event_type TEXT,content TEXT);
        CREATE TABLE receipts(room_id TEXT,user_id TEXT,receipt_type TEXT,event_id TEXT,thread_id TEXT,ts INTEGER);
        CREATE TABLE access_tokens(token_hash TEXT,user_id TEXT,device_id TEXT);
        CREATE TABLE refresh_tokens(token_hash TEXT);
        CREATE TABLE client_filters(user_id TEXT,filter_id TEXT,filter_json TEXT);
        ''')
        self.user, self.room, self.native = "@alice:example.org", "!legacy:example.org", "!native:example.org"
        for room in (self.room, self.native):
            source.execute("INSERT INTO room_memberships VALUES (?,?,?)", (room, self.user, "join"))
        for eid, kind, key, content, pos in [("$create", "m.room.create", "", {"room_version": "10"}, 1),
            ("$member", "m.room.member", self.user, {"membership": "join", "displayname": "Alice"}, 2),
            ("$cipher", "m.room.encrypted", None, {"ciphertext": "UNCHANGED+/=", "session_id": "original"}, 3)]:
            source.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?)", (eid, self.room, self.user, kind, key, json.dumps(content), pos * 1000, None, pos, pos, None))
            if key is not None:
                source.execute("INSERT INTO room_state VALUES (?,?,?,?)", (self.room, kind, key, eid))
        source.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?)", ("$badleave", self.native, self.user,
            "m.room.member", self.user, '{"membership":"leave"}', 13000, None, 13, 92, None))
        for eid, depth, pos in [("$older", 12, 80), ("$newer", 13, 141)]:
            source.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?)", (eid, self.native, self.user,
                "m.room.encrypted", None, '{"ciphertext":"native"}', pos * 1000, None, depth, pos, None))
        source.commit()
        source.close()
        manifest = {"source_sha256": hashlib.sha256((self.root / "source.sqlite").read_bytes()).hexdigest(),
                    "legacy_rooms": [self.room], "invalid_native_history": ["$badleave"]}
        (self.root / "manifest.json").write_text(json.dumps(manifest))
        (self.root / "secret").write_bytes(b"s" * 32)
        archive.STORE = archive.ArchiveStore(self.root / "source.sqlite", self.root / "manifest.json")
        self.valid = {"native-access": self.user, "other-access": "@other:example.org"}
        self.calls = []
        async def upstream(request):
            self.calls.append((request.path, dict(request.query), request.headers.get("Accept-Encoding")))
            user = self.valid.get(request.headers.get("Authorization", "")[7:])
            if not user:
                return web.json_response({"errcode": "M_UNKNOWN_TOKEN"}, status=401)
            if request.path.endswith("whoami"):
                return web.json_response({"user_id": user, "device_id": "KEPT"})
            if request.path.endswith("sync"):
                return web.json_response({"next_batch": "s200_1_2_3_4_5_6_7_8_9", "rooms": {"join": {self.native: {"timeline": {"events": []}}}}, "to_device": {"events": [{"content": {"ciphertext": "device-original"}}]}})
            if request.path.endswith("joined_rooms"):
                return web.json_response({"joined_rooms": [self.native]})
            return web.json_response({"native": True})
        self.upstream = TestServer(web.Application())
        self.upstream.app.router.add_route("*", "/{tail:.*}", upstream)
        await self.upstream.start_server()
        app = await create_app({"token_database": str(self.root / "source.sqlite"), "secret_file": str(self.root / "secret"),
            "archive_module": "archive", "upstream": str(self.upstream.make_url(""))})
        self.client = TestClient(TestServer(app))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()
        await self.upstream.close()
        archive.STORE.source.close()
        archive.STORE = None
        self.directory.cleanup()

    async def get(self, suffix, token="native-access", **params):
        return await self.client.get(suffix, params=params, headers={"Authorization": "Bearer " + token})

    async def test_initial_sync_preserves_native_cursor_ciphertext_and_filters(self):
        response = await self.get("/_matrix/client/v3/sync", since="s123_td1_dk2_rr3_ad4")
        body = await response.json()
        self.assertEqual(body["next_batch"], "s200_1_2_3_4_5_6_7_8_9")
        self.assertEqual(body["rooms"]["join"][self.room]["timeline"]["events"][-1]["content"]["ciphertext"], "UNCHANGED+/=")
        self.assertEqual(body["to_device"]["events"][0]["content"]["ciphertext"], "device-original")
        self.assertEqual(self.calls[-1][2], "identity")
        response = await self.get("/_matrix/client/v3/sync", since=body["next_batch"])
        self.assertNotIn(self.room, (await response.json())["rooms"]["join"])
        response = await self.get("/_matrix/client/v3/sync", filter=json.dumps({"room": {"not_rooms": [self.room]}}))
        self.assertNotIn(self.room, (await response.json())["rooms"]["join"])

    async def test_authentication_revocation_cross_user_and_read_only(self):
        path = "/_matrix/client/v3/rooms/" + self.room + "/messages"
        self.assertEqual((await self.get(path, token="other-access")).status, 403)
        self.assertEqual((await self.get(path, token="missing")).status, 401)
        del self.valid["native-access"]
        self.assertEqual((await self.get(path)).status, 401)
        self.valid["native-access"] = self.user
        response = await self.client.put("/_matrix/client/v3/rooms/" + self.room + "/state/m.room.name", json={"name": "cannot change"}, headers={"Authorization": "Bearer native-access"})
        self.assertEqual(response.status, 403)

    async def test_pagination_state_empty_key_and_original_event_id(self):
        path = "/_matrix/client/v3/rooms/" + self.room
        first = await (await self.get(path + "/messages", limit="1")).json()
        second = await (await self.get(path + "/messages", limit="1", **{"from": first["end"]})).json()
        self.assertEqual(first["chunk"][0]["event_id"], "$cipher")
        self.assertEqual(second["chunk"][0]["event_id"], "$member")
        legacy = await (await self.get(path + "/messages", **{"from": "s3_td1_dk2_rr3_ad4"})).json()
        self.assertNotIn("$cipher", [item["event_id"] for item in legacy["chunk"]])
        self.assertEqual((await (await self.get(path + "/state/m.room.create/")).json())["room_version"], "10")
        self.assertEqual((await (await self.get(path + "/event/$cipher")).json())["content"]["ciphertext"], "UNCHANGED+/=")
        self.assertEqual((await self.get(path + "/messages", **{"from": "arbitrary"})).status, 400)

    async def test_malformed_leave_is_history_only_and_page_bound(self):
        body = {"start": "t13-141", "end": "t12-79", "chunk": [archive.event(archive.STORE.events["$newer"]), archive.event(archive.STORE.events["$older"])]}
        archive.overlay_history(archive.STORE, body, {"dir": "b", "limit": "2"}, self.native, self.user)
        self.assertEqual([item["event_id"] for item in body["chunk"]], ["$newer", "$badleave"])
        self.assertEqual(body["end"], "t13-91")
        following = {"start": "t13-91", "end": "t12-79", "chunk": [archive.event(archive.STORE.events["$older"])]}
        archive.overlay_history(archive.STORE, following, {"dir": "b"}, self.native, self.user)
        self.assertEqual([item["event_id"] for item in following["chunk"]], ["$older"])
        response = await self.get("/_matrix/client/v3/rooms/" + self.native + "/event/$badleave")
        self.assertEqual((await response.json())["content"]["membership"], "leave")
        self.assertEqual(archive.STORE.memberships[(self.native, self.user)], "join")

    async def test_manifest_snapshot_mismatch_fails_closed(self):
        manifest = json.loads((self.root / "manifest.json").read_text())
        manifest["source_sha256"] = "0" * 64
        (self.root / "wrong.json").write_text(json.dumps(manifest))
        with self.assertRaises(ValueError):
            archive.ArchiveStore(self.root / "source.sqlite", self.root / "wrong.json")


if __name__ == "__main__":
    unittest.main()
