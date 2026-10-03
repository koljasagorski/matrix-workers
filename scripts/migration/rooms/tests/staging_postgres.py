"""Real schema94 rollback/idempotence/preservation regression on a disposable DB.

Never imports into synapse. Creates one uniquely named isolated test database,
loads the provided schema-only dump, and drops only that created DB in finally.
Run inside the pinned Synapse image with private POSTGRES_* environment values.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import sys
import uuid

import psycopg2
from psycopg2 import sql

sys.path.insert(0, str(Path(__file__).parents[2]))
from rooms.plan import build_plan, source_connection
from rooms.postgres import connect, import_plan


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--server-name", required=True)
    options = parser.parse_args()
    if os.environ.get("MATRIX_IMPORT_DSN"):
        raise ValueError("Disposable test requires POSTGRES_* variables, not a fixed DSN")
    plan = build_plan(options.source)
    database = "matrix_room_import_regression_" + uuid.uuid4().hex
    admin = connect()
    admin.autocommit = True
    created = False
    original_database = os.environ.get("PGDATABASE")
    try:
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(database)))
        created = True
        os.environ["PGDATABASE"] = database
        connection = connect()
        try:
            with connection, connection.cursor() as cursor:
                # psql-only dump directives are safe to omit for this isolated
                # schema fixture. No data/credentials are copied from Synapse.
                schema = "\n".join(line for line in Path(options.schema).read_text().splitlines() if not line.startswith("\\"))
                cursor.execute(schema)
                cursor.execute("SET search_path TO public")
                cursor.execute("INSERT INTO schema_version(version,upgraded) VALUES (94,true)")
                source = source_connection(options.source)
                try:
                    for user in source.execute("SELECT user_id FROM users WHERE is_deactivated=0"):
                        cursor.execute("INSERT INTO users(name,creation_ts,admin,deactivated,is_guest,approved) VALUES (%s,0,0,0,0,true)", (user[0],))
                finally:
                    source.close()
            failed = copy.deepcopy(plan)
            room_id = next(iter(plan["native_events"].values()))["room_id"]
            duplicate = {"alias": "#migration-rollback:" + options.server_name, "room_id": room_id, "creator_id": None}
            failed["aliases"] += [duplicate, duplicate]
            try:
                import_plan(failed, options.server_name)
            except psycopg2.errors.UniqueViolation:
                pass
            else:
                raise AssertionError("Expected transactional alias conflict")
            with connection.cursor() as cursor:
                cursor.execute("SELECT (SELECT COUNT(*) FROM events),(SELECT COUNT(*) FROM rooms)")
                assert cursor.fetchone() == (0, 0), "Failed import leaked room/event rows"
            connection.rollback()
            report = import_plan(plan, options.server_name)
            replay = import_plan(plan, options.server_name)
            assert replay["resumed"] is True
            with connection.cursor() as cursor:
                cursor.execute("SELECT event_id,json,format_version FROM event_json")
                native_rows = cursor.fetchall()
                assert len(native_rows) == len(plan["native_events"])
                for eid, payload, format_version in native_rows:
                    assert json.loads(payload) == plan["native_events"][eid]["wire"], "PDU changed during import"
                    version = plan["rooms"][plan["native_events"][eid]["room_id"]]["room_version"]
                    assert format_version == (4 if version == "12" else 3)
                cursor.execute("SELECT COUNT(*) FROM current_state_events WHERE type='m.room.create'")
                assert cursor.fetchone()[0] == len(plan["rooms"]) - len(plan["legacy_rooms"])
                cursor.execute("SELECT COUNT(*) FROM event_forward_extremities f LEFT JOIN event_to_state_groups g USING(event_id) WHERE g.state_group IS NULL")
                assert cursor.fetchone()[0] == 0
                cursor.execute("SELECT last_value FROM events_stream_seq")
                assert cursor.fetchone()[0] == report["max_event_stream"]
                cursor.execute("SELECT stream_id FROM stream_positions WHERE stream_name='backfill' AND instance_name='master'")
                assert cursor.fetchone()[0] == -report["native_outliers"]
            print(json.dumps({"rollback": True, "idempotence": True, "unchanged_native_pdus": len(native_rows), "codec": True, "current_state": True, "sequence": True}))
        finally:
            connection.close()
    finally:
        if original_database is None:
            os.environ.pop("PGDATABASE", None)
        else:
            os.environ["PGDATABASE"] = original_database
        if created:
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
        admin.close()


if __name__ == "__main__":
    main()
