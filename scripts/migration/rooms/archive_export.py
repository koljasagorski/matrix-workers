"""Write a credential-free, immutable client-history subset of the source."""
import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
from contextlib import closing

from .plan import source_connection


def export_archive(source_path, target_path, plan):
    target = Path(target_path)
    if target.resolve() == Path(source_path).resolve():
        raise ValueError("Archive output must not replace the source backup")
    rooms = set(plan["legacy_rooms"]) | {plan["events"][eid]["room_id"] for eid in plan["invalid_events"]
                                       if plan["events"][eid]["room_id"] not in plan["legacy_rooms"]}
    descriptor, temporary = tempfile.mkstemp(prefix=".archive-", suffix=".sqlite", dir=target.parent)
    os.close(descriptor)
    try:
        with closing(source_connection(source_path)) as source, closing(sqlite3.connect(temporary)) as destination, destination:
            for table in ("events", "room_state", "room_memberships", "account_data", "receipts"):
                columns = source.execute(f'PRAGMA table_info("{table}")').fetchall()
                # A standalone immutable subset must not retain foreign keys to
                # omitted users/token/room tables. Values and column types remain
                # exact; source schema/constraints remain in the full backup.
                definitions = []
                for column in columns:
                    if column[2].upper() not in {"TEXT", "INTEGER", "REAL", "BLOB", "NUMERIC"}:
                        raise ValueError("Unexpected archive source column type")
                    name = column[1].replace('"', '""')
                    definitions.append(f'"{name}" {column[2]}')
                destination.execute(f'CREATE TABLE "{table}" ({",".join(definitions)})')
                placeholders = ",".join("?" for _ in rooms)
                rows = source.execute(f'SELECT * FROM "{table}" WHERE room_id IN ({placeholders})', sorted(rooms)).fetchall()
                if rows:
                    values = ",".join("?" for _ in rows[0])
                    destination.executemany(f'INSERT INTO "{table}" VALUES ({values})', [tuple(row) for row in rows])
            destination.execute("CREATE TABLE archive_provenance(source_sha256 TEXT NOT NULL)")
            destination.execute("INSERT INTO archive_provenance VALUES (?)", (plan["source_sha256"],))
        os.chmod(temporary, 0o600)
        os.replace(temporary, target)
        return {"archive_sha256": hashlib.sha256(target.read_bytes()).hexdigest(), "archive_rooms": len(rooms)}
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
