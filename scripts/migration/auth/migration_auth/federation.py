"""Pinned, temporary transport gate while legacy federation transactions replay.

Synapse has no public outgoing transport callback. This deliberately narrow
private API wrapper refuses to initialize with a different Synapse version.
"""
from contextlib import closing
from pathlib import Path
import sqlite3
from urllib.parse import quote


class LegacyFederationGate:
    @staticmethod
    def parse_config(config):
        journal = config.get("journal")
        destinations = config.get("destinations")
        retry_ms = config.get("retry_ms", 30000)
        if not isinstance(journal, str) or not journal.startswith("/"):
            raise ValueError("An absolute replay journal path is required")
        if not isinstance(destinations, (list, frozenset)) or any(not isinstance(value, str) or not value for value in destinations):
            raise ValueError("Explicit replay destinations are required")
        if not isinstance(retry_ms, int) or not 1000 <= retry_ms <= 30000:
            raise ValueError("Replay gate retry must be between 1 and 30 seconds")
        return {"journal": journal, "destinations": frozenset(destinations), "retry_ms": retry_ms}

    def __init__(self, config, api):
        import synapse
        from synapse.util.retryutils import NotRetryingDestination
        if synapse.__version__ != "1.162.0":
            raise RuntimeError("LegacyFederationGate requires the tested Synapse 1.162.0")
        self.config = self.parse_config(config)
        self.uri = "file:" + quote(str(Path(self.config["journal"]).resolve())) + "?mode=ro"
        http = api._hs.get_federation_http_client()
        clock = api._hs.get_clock()
        original = http.put_json

        async def guarded(destination, path, *args, **kwargs):
            if destination in self.config["destinations"] and path.startswith("/_matrix/federation/v1/send/") and self.pending(destination):
                raise NotRetryingDestination(clock.time_msec(), self.config["retry_ms"], destination)
            return await original(destination, path, *args, **kwargs)

        http.put_json = guarded

    def pending(self, destination):
        try:
            with closing(sqlite3.connect(self.uri, uri=True, timeout=0.05)) as journal:
                total, incomplete = journal.execute(
                    "SELECT COUNT(*),SUM(CASE WHEN status='complete' THEN 0 ELSE 1 END) FROM replay_transactions WHERE destination=?",
                    (destination,),
                ).fetchone()
                return total == 0 or incomplete != 0
        except (sqlite3.Error, OSError):
            # A missing/corrupt journal cannot establish that the prior native
            # transaction was acknowledged. Only configured send destinations
            # are delayed; other Matrix federation requests remain available.
            return True
