import importlib
from pathlib import Path
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1]))
from migration_auth.federation import LegacyFederationGate


class GateTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.journal = Path(self.directory.name) / "journal.sqlite"
        db = sqlite3.connect(self.journal)
        db.executescript("CREATE TABLE replay_transactions(destination TEXT,status TEXT); INSERT INTO replay_transactions VALUES('pending.example','pending'); INSERT INTO replay_transactions VALUES('pending.example','complete'); INSERT INTO replay_transactions VALUES('finished.example','complete');")
        db.close()
        self.calls = []
        owner = self
        class HTTP:
            async def put_json(self, destination, path, *args, **kwargs):
                owner.calls.append((destination, path, args, kwargs))
                return {"ok": True}
        self.http = HTTP()
        class HS:
            def get_federation_http_client(self):
                return owner.http
            def get_clock(self):
                return types.SimpleNamespace(time_msec=lambda: 123)
        self.api = types.SimpleNamespace(_hs=HS())
        class NotRetryingDestination(Exception):
            def __init__(self, retry_last_ts, retry_interval, destination):
                self.retry_last_ts = retry_last_ts
                self.retry_interval = retry_interval
                self.destination = destination
        self.not_retrying = NotRetryingDestination
        synapse = types.ModuleType("synapse")
        synapse.__version__ = "1.162.0"
        retryutils = types.ModuleType("synapse.util.retryutils")
        retryutils.NotRetryingDestination = self.not_retrying
        self.patch = patch.dict(sys.modules, {"synapse": synapse, "synapse.util.retryutils": retryutils})
        self.patch.start()

    async def asyncTearDown(self):
        self.patch.stop()
        self.directory.cleanup()

    def install(self):
        config = LegacyFederationGate.parse_config({"journal": str(self.journal), "destinations": ["pending.example", "finished.example"]})
        return LegacyFederationGate(config, self.api)

    async def test_only_pending_send_is_delayed_then_ack_releases(self):
        self.install()
        with self.assertRaises(self.not_retrying) as result:
            await self.http.put_json("pending.example", "/_matrix/federation/v1/send/new", data={"pdus": []})
        self.assertEqual(result.exception.retry_interval, 30000)
        self.assertEqual(self.calls, [])
        for destination, path in (("other.example", "/_matrix/federation/v1/send/new"), ("pending.example", "/_matrix/federation/v1/make_join/room/user"), ("pending.example", "/_matrix/key/v2/query"), ("finished.example", "/_matrix/federation/v1/send/new")):
            self.assertEqual(await self.http.put_json(destination, path, data={"kept": True}), {"ok": True})
            self.assertEqual(self.calls[-1][3], {"data": {"kept": True}})
        db = sqlite3.connect(self.journal)
        db.execute("UPDATE replay_transactions SET status='complete' WHERE destination='pending.example'")
        db.commit()
        db.close()
        self.assertEqual(await self.http.put_json("pending.example", "/_matrix/federation/v1/send/new"), {"ok": True})

    async def test_missing_and_corrupt_journal_fail_closed_for_watched_send(self):
        self.install()
        self.journal.unlink()
        for data in (None, b"not a SQLite database"):
            if data is not None:
                self.journal.write_bytes(data)
            with self.assertRaises(self.not_retrying):
                await self.http.put_json("pending.example", "/_matrix/federation/v1/send/new")
            self.assertEqual(await self.http.put_json("other.example", "/_matrix/federation/v1/send/new"), {"ok": True})

    async def test_version_and_missing_scope_fail_at_startup(self):
        sys.modules["synapse"].__version__ = "1.163.0"
        with self.assertRaises(RuntimeError):
            self.install()
        with self.assertRaises(ValueError):
            LegacyFederationGate.parse_config({"journal": str(self.journal)})

    async def test_explicit_empty_watchlist_is_disabled_for_rehearsal(self):
        self.journal.unlink()
        LegacyFederationGate({"journal": str(self.journal), "destinations": []}, self.api)
        self.assertEqual(await self.http.put_json("pending.example", "/_matrix/federation/v1/send/new"), {"ok": True})

    async def test_missing_destination_and_unknown_status_fail_closed(self):
        self.install()
        db = sqlite3.connect(self.journal)
        db.execute("DELETE FROM replay_transactions WHERE destination='pending.example'")
        db.commit()
        with self.assertRaises(self.not_retrying):
            await self.http.put_json("pending.example", "/_matrix/federation/v1/send/new")
        db.execute("INSERT INTO replay_transactions VALUES('pending.example','unknown')")
        db.commit()
        db.close()
        with self.assertRaises(self.not_retrying):
            await self.http.put_json("pending.example", "/_matrix/federation/v1/send/new")


if __name__ == "__main__":
    unittest.main()
