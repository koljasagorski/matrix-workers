import asyncio
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import types
import unittest
import zlib

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

sys.path.insert(0, str(Path(__file__).parents[1]))
from migration_auth.common import native_token, token_hash, verify_legacy_password
from migration_auth.gateway import create_app, normalize_request, NATIVE_SLIDING_PATH, private_health_probe
from migration_auth.password import LegacyPasswordProvider

spec = importlib.util.spec_from_file_location("user_import", os.environ.get("MATRIX_USER_IMPORTER") or Path(__file__).parents[2] / "import-user-data.py")
user_import = importlib.util.module_from_spec(spec)
spec.loader.exec_module(user_import)


def password_hash(password):
    salt = b"0123456789abcdef"
    return "$pbkdf2-sha256$100000$" + base64.b64encode(salt).decode() + "$" + base64.b64encode(hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)).decode()


class PasswordTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_unicode_password_and_reset_precedence(self):
        class API:
            row = (None, 0, False, False, password_hash("päss✓"))
            def register_password_auth_provider_callbacks(self, **kwargs):
                self.callbacks = kwargs
            def get_qualified_user_id(self, user):
                return user if user.startswith("@") else "@" + user + ":example.org"
            async def run_db_interaction(self, name, function):
                api = self
                class Txn:
                    def execute(self, sql, params):
                        self.params = params
                    def fetchone(self):
                        return api.row
                return function(Txn())
        api = API()
        provider = LegacyPasswordProvider({}, api)
        self.assertEqual(await provider.check_auth("alice", "m.login.password", {"password": "päss✓"}), ("@alice:example.org", None))
        self.assertIsNone(await provider.check_auth("alice", "m.login.password", {"password": "wrong"}))
        for native_hash, deactivated, locked, suspended in (("$2b$new", 0, False, False), (None, 1, False, False), (None, 0, True, False), (None, 0, False, True)):
            api.row = (native_hash, deactivated, locked, suspended, password_hash("päss✓"))
            self.assertIsNone(await provider.check_auth("alice", "m.login.password", {"password": "päss✓"}))

    async def test_invalid_hashes_fail_without_unbounded_work(self):
        for encoded in ("", "$pbkdf2-sha256$999999999$AA==$AA==", "$pbkdf2-sha256$100000$!!!!$!!!!", password_hash("ok") + "$extra"):
            self.assertFalse(verify_legacy_password("ok", encoded))
        self.assertFalse(verify_legacy_password("no", password_hash("ok")))


class PositionTests(unittest.TestCase):
    def test_native_refresh_envelope_and_crc_are_valid(self):
        token = native_token(b"s" * 32, token_hash("old-refresh"), "refresh")
        fields = token.split("_")
        self.assertEqual(len(fields), 4)
        self.assertEqual(fields[0], "syr")
        self.assertEqual(len(fields[2]), 64)
        alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        decoded = 0
        for char in fields[3]:
            decoded = decoded * 62 + alphabet.index(char)
        self.assertEqual(decoded, zlib.crc32("_".join(fields[:3]).encode("ascii")))

    def test_only_legacy_positions_reset(self):
        for old in ("s123", "123", "s123_td4_dk5_rr6_ad7"):
            _, query, _, legacy = normalize_request("/_matrix/client/v3/sync", [("since", old), ("timeout", "1")], b"")
            self.assertTrue(legacy)
            self.assertEqual(query, [("timeout", "1")])
        native = "s7_0_0_0_0_0_0_0_0_0"
        self.assertEqual(normalize_request("/_matrix/client/v3/sync", [("since", native)], b"")[1], [("since", native)])
        self.assertFalse(normalize_request("/_matrix/client/v3/sync", [("since", native)], b"")[3])
        self.assertEqual(normalize_request("/_matrix/client/v3/messages", [("from", "s123")], b"")[1], [("from", "s123")])

    def test_sliding_alias_and_extension_cursor(self):
        path, query, body, legacy = normalize_request("/_matrix/client/v4/sync", [], json.dumps({"pos": "123_dk1_rr2_ad3", "conn_id": "phone", "extensions": {"to_device": {"enabled": True, "since": "23"}}}).encode())
        self.assertEqual(path, NATIVE_SLIDING_PATH)
        self.assertTrue(legacy)
        self.assertEqual(query, [])
        self.assertNotIn("since", json.loads(body)["extensions"]["to_device"])
        self.assertEqual(json.loads(body)["conn_id"], "phone")


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.secret = b"s" * 32
        (self.root / "secret").write_bytes(self.secret)
        database = sqlite3.connect(self.root / "tokens.sqlite")
        database.execute("CREATE TABLE access_tokens(token_hash TEXT,user_id TEXT,device_id TEXT)")
        database.execute("CREATE TABLE refresh_tokens(token_hash TEXT,user_id TEXT,device_id TEXT)")
        database.execute("CREATE TABLE client_filters(user_id TEXT,filter_id TEXT,filter_json TEXT)")
        database.execute("INSERT INTO client_filters VALUES (?,?,?)", ("@alice:example.org", "0fabc123", '{"room":{"timeline":{"limit":20}}}'))
        database.execute("INSERT INTO access_tokens VALUES (?,?,?)", (token_hash("old-access"), "@alice:example.org", "KEPT_DEVICE"))
        database.execute("INSERT INTO refresh_tokens VALUES (?,?,?)", (token_hash("old-refresh"), "@alice:example.org", "KEPT_DEVICE"))
        database.commit()
        database.close()
        self.synthetic = native_token(self.secret, token_hash("old-access"))
        self.valid_tokens = {self.synthetic, "new-native-access"}
        self.calls = []
        self.health_available = True

        async def upstream(request):
            body = await request.read()
            self.calls.append((request.path, list(request.query.items()), request.headers.get("Authorization"), body))
            if request.path == "/_matrix/client/versions":
                return web.json_response({"versions": ["v1.15"]}, status=200 if self.health_available else 503)
            token = request.headers.get("Authorization", "")[7:]
            if request.path.endswith("/refresh"):
                supplied = json.loads(body)["refresh_token"]
                if supplied != native_token(self.secret, token_hash("old-refresh"), "refresh"):
                    return web.json_response({"errcode": "M_UNKNOWN_TOKEN"}, status=401)
                return web.json_response({"access_token": "new-native-access", "refresh_token": "new-native-refresh"})
            if token not in self.valid_tokens:
                return web.json_response({"errcode": "M_UNKNOWN_TOKEN"}, status=401)
            if request.path.endswith("/logout"):
                self.valid_tokens.remove(token)
            if request.path.endswith("/whoami"):
                return web.json_response({"user_id": "@alice:example.org", "device_id": "KEPT_DEVICE"})
            return web.json_response({"next_batch": "s7_0_0_0_0_0_0_0_0_0", "device_id": "KEPT_DEVICE", "keys": {"ciphertext": "unchanged"}})

        self.upstream = TestServer(web.Application())
        self.upstream.app.router.add_route("*", "/{tail:.*}", upstream)
        await self.upstream.start_server()
        app = await create_app({"token_database": str(self.root / "tokens.sqlite"), "secret_file": str(self.root / "secret"), "upstream": str(self.upstream.make_url("")), "maintenance_file": str(self.root / "maintenance.flag")})
        self.client = TestClient(TestServer(app))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()
        await self.upstream.close()
        self.directory.cleanup()

    async def test_existing_device_survives_reset_and_logout_is_permanent(self):
        response = await self.client.get("/_matrix/client/v3/sync?since=s999_td2_dk4_rr1_ad10", headers={"Authorization": "Bearer old-access"})
        self.assertEqual(response.status, 200)
        result = await response.json()
        self.assertEqual(result["device_id"], "KEPT_DEVICE")
        self.assertEqual(result["keys"], {"ciphertext": "unchanged"})
        self.assertEqual(self.calls[-1][1], [])
        self.assertEqual(self.calls[-1][2], "Bearer " + self.synthetic)
        self.assertEqual((await self.client.post("/_matrix/client/v3/logout", headers={"Authorization": "Bearer old-access"})).status, 200)
        self.assertEqual((await self.client.get("/_matrix/client/v3/sync", headers={"Authorization": "Bearer old-access"})).status, 401)
        self.assertEqual((await self.client.get("/_matrix/client/v3/sync", headers={"Authorization": "Bearer old-access"})).status, 401)

    async def test_query_token_removed_and_native_position_passes(self):
        native = "s7_0_0_0_0_0_0_0_0_0"
        self.assertEqual((await self.client.get("/_matrix/client/v3/sync", params={"access_token": "old-access", "since": native})).status, 200)
        self.assertEqual(self.calls[-1][1], [("since", native)])
        self.assertEqual((await self.client.get("/_matrix/client/v3/sync", headers={"Authorization": "Bearer new-native-access"})).status, 200)
        self.assertEqual(self.calls[-1][2], "Bearer new-native-access")

    async def test_refresh_returns_native_tokens(self):
        response = await self.client.post("/_matrix/client/v3/refresh", json={"refresh_token": "old-refresh"})
        self.assertEqual(response.status, 200)
        self.assertEqual((await response.json())["access_token"], "new-native-access")
        self.assertNotEqual(json.loads(self.calls[-1][3])["refresh_token"], "old-refresh")

    async def test_saved_nonnumeric_filter_id_and_ownership(self):
        auth = {"Authorization": "Bearer old-access"}
        self.assertEqual((await self.client.get("/_matrix/client/v3/sync?filter=0fabc123", headers=auth)).status, 200)
        self.assertEqual(json.loads(dict(self.calls[-1][1])["filter"]), {"room": {"timeline": {"limit": 20}}})
        response = await self.client.get("/_matrix/client/v3/user/@alice:example.org/filter/0fabc123", headers=auth)
        self.assertEqual(response.status, 200)
        self.assertEqual(await response.json(), {"room": {"timeline": {"limit": 20}}})
        self.assertEqual((await self.client.get("/_matrix/client/v3/user/@bob:example.org/filter/0fabc123", headers=auth)).status, 403)
        await self.client.post("/_matrix/client/v3/logout", headers=auth)
        self.assertEqual((await self.client.get("/_matrix/client/v3/user/@alice:example.org/filter/0fabc123", headers=auth)).status, 401)

    async def test_truthful_health_well_known_cors_and_private_admin(self):
        response = await self.client.get("/health")
        self.assertEqual(response.status, 200)
        self.assertEqual(await response.json(), {"status": "ok"})
        self.health_available = False
        response = await self.client.get("/health")
        self.assertEqual(response.status, 503)
        self.assertEqual(await response.json(), {"status": "unavailable"})
        response = await self.client.get("/.well-known/matrix/server")
        self.assertEqual(await response.json(), {"m.server": "m.sgr.ski:443"})
        response = await self.client.get("/.well-known/matrix/client")
        self.assertEqual(await response.json(), {"m.homeserver": {"base_url": "https://m.sgr.ski"}})
        self.assertEqual(response.headers["Access-Control-Allow-Origin"], "*")
        response = await self.client.options("/.well-known/matrix/client")
        self.assertEqual(response.status, 204)
        self.assertEqual(response.headers["Access-Control-Allow-Origin"], "*")
        count = len(self.calls)
        for path in ("/admin", "/admin/config", "/_synapse/admin/v2/users", "/_matrix/client/v3/admin/whois/user"):
            self.assertEqual((await self.client.get(path)).status, 404)
        self.assertEqual(len(self.calls), count)

    async def test_maintenance_blocks_proxy_and_health_with_loopback_only_probe(self):
        (self.root / "maintenance.flag").write_text("maintenance")
        count = len(self.calls)
        for path in ("/_matrix/client/v3/sync", "/_matrix/federation/v1/version", "/_matrix/media/v3/download/server/id", "/health"):
            response = await self.client.get(path)
            self.assertEqual(response.status, 503)
            self.assertEqual(response.headers["Retry-After"], "30")
        self.assertEqual(len(self.calls), count)
        self.assertEqual((await self.client.get("/.well-known/matrix/client")).status, 200)
        response = await self.client.get("/health?maintenance_probe=true")
        self.assertEqual(response.status, 200)
        self.assertEqual(await response.json(), {"status": "ok"})
        for remote in ("172.20.0.1", "198.51.100.1", None):
            request = types.SimpleNamespace(query={"maintenance_probe": "true"}, remote=remote, headers={"X-Forwarded-For": "127.0.0.1"})
            self.assertFalse(private_health_probe(request))
        (self.root / "maintenance.flag").unlink()
        self.assertEqual((await self.client.get("/health")).status, 200)


class ImportTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE users(user_id,localpart,password_hash,display_name,avatar_url,is_guest,is_deactivated,admin,created_at);
            INSERT INTO users VALUES('@alice:example.org','alice',NULL,'Alice',NULL,0,0,0,1000);
            CREATE TABLE devices(user_id,device_id,display_name,last_seen_ts,last_seen_ip);
            INSERT INTO devices VALUES('@alice:example.org','KEPT_DEVICE','Phone',1000,NULL);
            CREATE TABLE access_tokens(token_id,token_hash,user_id,device_id,expires_at);
            CREATE TABLE account_data(user_id,room_id,event_type,content);
        """)
        self.db.execute("INSERT INTO access_tokens VALUES (?,?,?,?,?)", ("id", token_hash("old"), "@alice:example.org", "KEPT_DEVICE", 3000))

    def tearDown(self):
        self.db.close()

    def test_authoritative_crypto_overlay_and_private_settings_preserved(self):
        keys = {"user_id": "@alice:example.org", "device_id": "KEPT_DEVICE", "keys": {"ed25519:KEPT_DEVICE": "public"}, "signatures": {"@alice:example.org": {"ed25519:master": "sig"}}}
        secret_storage = {"encrypted": {"key": {"ciphertext": "opaque", "iv": "iv", "mac": "mac"}}}
        self.db.execute("INSERT INTO account_data VALUES(?,?,?,?)", ("@alice:example.org", "", "m.direct", json.dumps({"@bob:remote": ["!old:example.org", "!new:example.org"]})))
        overlay = {"users": {"@alice:example.org": {"device_keys": {"KEPT_DEVICE": keys}, "account_data": {"m.cross_signing.master": secret_storage}}}}
        plan, access, _ = user_import.build_import(self.db, overlay, {}, b"s" * 32, 1000)
        self.assertEqual(json.loads(plan["e2e_device_keys_json"][0]["key_json"]), keys)
        stored = {row["account_data_type"]: json.loads(row["content"]) for row in plan["account_data"]}
        self.assertEqual(stored["m.cross_signing.master"], secret_storage)
        self.assertEqual(stored["m.direct"]["@bob:remote"], ["!old:example.org", "!new:example.org"])
        self.assertEqual(plan["access_tokens"][0]["device_id"], "KEPT_DEVICE")
        self.assertEqual(plan["access_tokens"][0]["valid_until_ms"], 3000)
        self.assertEqual(plan["e2e_cross_signing_signatures"][0]["target_device_id"], "KEPT_DEVICE")

    def test_claimed_keys_are_not_resurrected_and_refresh_keeps_expiry(self):
        self.db.executescript("CREATE TABLE one_time_keys(user_id,device_id,algorithm,key_id,key_data,created_at,claimed); INSERT INTO one_time_keys VALUES('@alice:example.org','KEPT_DEVICE','signed_curve25519','used','{}',1,1); INSERT INTO one_time_keys VALUES('@alice:example.org','KEPT_DEVICE','signed_curve25519','unused','{}',1,0);")
        refresh = {"tokens": [{"token_hash": token_hash("refresh"), "user_id": "@alice:example.org", "device_id": "KEPT_DEVICE", "access_token_id": "id", "expires_at_ms": 2000}]}
        plan, _, bridge = user_import.build_import(self.db, {}, refresh, b"s" * 32, 1000)
        self.assertEqual([row["key_id"] for row in plan["e2e_one_time_keys_json"]], ["unused"])
        self.assertEqual(plan["refresh_tokens"][0]["expiry_ts"], 2000)
        self.assertEqual(plan["access_tokens"][0]["refresh_token_id"], 1)
        self.assertEqual(len(bridge), 1)
        self.assertEqual(user_import.build_import(self.db, {}, refresh, b"s" * 32, 3000)[0]["refresh_tokens"], [])

    def test_missing_overlay_is_a_final_import_blocker(self):
        with self.assertRaises(ValueError):
            user_import.load_overlay(None, False)
        with self.assertRaises(ValueError):
            user_import.validate_overlay(self.db, {"users": {}})
        user_import.validate_overlay(self.db, {"users": {"@alice:example.org": {"account_data": {}, "device_keys": {}, "cross_signing": {}, "signatures": []}}})

    def test_stale_kv_refresh_does_not_resurrect_logged_out_session(self):
        refresh = {"tokens": [{"token_hash": token_hash("revoked-refresh"), "user_id": "@alice:example.org", "device_id": "KEPT_DEVICE", "access_token_id": "deleted-session", "expires_at_ms": 9000}]}
        plan, _, bridge = user_import.build_import(self.db, {}, refresh, b"s" * 32, 1000)
        self.assertEqual(plan["refresh_tokens"], [])
        self.assertEqual(bridge, [])

    def test_native_refresh_foreign_key_parent_is_inserted_first(self):
        target = sqlite3.connect(":memory:")
        target.executescript("PRAGMA foreign_keys=ON; CREATE TABLE users(name TEXT PRIMARY KEY); CREATE TABLE devices(user_id TEXT,device_id TEXT); CREATE TABLE refresh_tokens(id INTEGER PRIMARY KEY,user_id TEXT REFERENCES users(name)); CREATE TABLE access_tokens(id INTEGER PRIMARY KEY,user_id TEXT REFERENCES users(name),refresh_token_id INTEGER REFERENCES refresh_tokens(id));")
        plan = {table: [] for table in user_import.OWNED_TABLES}
        plan["users"] = [{"name": "@alice:example.org"}]
        plan["refresh_tokens"] = [{"id": 1, "user_id": "@alice:example.org"}]
        plan["access_tokens"] = [{"id": 1, "user_id": "@alice:example.org", "refresh_token_id": 1}]
        class Cursor:
            def execute(self, sql, params):
                return target.execute(sql.replace("%s", "?"), params)
        user_import.insert_plan(Cursor(), plan)
        self.assertEqual(target.execute("SELECT refresh_token_id FROM access_tokens").fetchone(), (1,))
        target.close()

    def test_stream_counter_keeps_retained_positions_tables_and_sequence(self):
        target = sqlite3.connect(":memory:")
        target.executescript("CREATE TABLE stream_positions(stream_name TEXT,stream_id INTEGER); INSERT INTO stream_positions VALUES('device_lists_stream',80); CREATE TABLE device_lists_stream(stream_id INTEGER); INSERT INTO device_lists_stream VALUES(11); CREATE TABLE user_signature_stream(stream_id INTEGER); INSERT INTO user_signature_stream VALUES(99);")
        class Cursor:
            high = 4
            result = None
            def execute(self, sql, params=()):
                if sql.startswith("SELECT last_value"):
                    self.result = (self.high,)
                elif sql.startswith("SELECT setval"):
                    self.high = params[1]
                    self.result = (self.high,)
                else:
                    self.result = target.execute(sql.replace("%s", "?"), params).fetchone()
            def fetchone(self):
                return self.result
        cursor = Cursor()
        sequences = {"device_lists_sequence": ("device_lists_stream", [("device_lists_stream", "stream_id"), ("user_signature_stream", "stream_id")])}
        user_import.advance_sequences(cursor, sequences)
        self.assertEqual(cursor.high, 99)
        target.execute("UPDATE stream_positions SET stream_id=150")
        user_import.advance_sequences(cursor, sequences)
        self.assertEqual(cursor.high, 150)
        target.execute("DELETE FROM stream_positions")
        target.execute("DELETE FROM user_signature_stream")
        user_import.advance_sequences(cursor, sequences)
        self.assertEqual(cursor.high, 150)
        target.close()


if __name__ == "__main__":
    unittest.main()
