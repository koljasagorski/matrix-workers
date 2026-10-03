"""Explicit LOCAL staging API write test; never pointed at a public server.

Only runs against 127.0.0.1, requires stopped federation in target config, and
uses an imported native access token without printing it. A final fresh import
must discard all rooms/events from this staging-only test.
"""
import argparse
import json
from pathlib import Path
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import psycopg


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--environment", default="/opt/matrix/postgres.env")
    parser.add_argument("--synapse-config", default="/opt/matrix/synapse/data/homeserver.yaml")
    parser.add_argument("--port", type=int, default=18008)
    options = parser.parse_args()
    config_text = Path(options.synapse_config).read_text()
    try:
        config = json.loads(config_text)
    except ValueError:
        import yaml
        config = yaml.safe_load(config_text)
    if config.get("federation_domain_whitelist") != []:
        raise ValueError("This test requires staging federation whitelist[]")
    settings = dict(line.split("=", 1) for line in Path(options.environment).read_text().splitlines() if "=" in line and not line.startswith("#"))
    import subprocess
    address = subprocess.check_output(["docker", "inspect", "matrix-postgres", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"], text=True).strip()
    with psycopg.connect(host=address, dbname="synapse", user=settings["POSTGRES_USER"], password=settings["POSTGRES_PASSWORD"]) as database:
        with database.cursor() as cursor:
            cursor.execute("SELECT token FROM access_tokens WHERE user_id=%s AND (valid_until_ms IS NULL OR valid_until_ms>%s) ORDER BY id LIMIT 1", (options.user_id, int(time.time() * 1000)))
            token = cursor.fetchone()[0]
            cursor.execute("SELECT room_id FROM rooms WHERE room_version='12' AND creator=%s ORDER BY room_id LIMIT 1", (options.user_id,))
            migrated_room = cursor.fetchone()[0]
    def request(method, path, body=None):
        encoded = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(f"http://127.0.0.1:{options.port}/_matrix/client/v3" + path,
            data=encoded, method=method, headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    created = request("POST", "/createRoom", {"room_version": "12", "preset": "private_chat", "name": "Disposable migration rehearsal"})
    room = urllib.parse.quote(created["room_id"], safe="")
    sent = request("PUT", f"/rooms/{room}/send/m.room.message/migration-rehearsal", {"msgtype": "m.text", "body": "Disposable staging integrity check"})
    fetched = request("GET", f"/rooms/{room}/event/" + urllib.parse.quote(sent["event_id"], safe=""))
    assert fetched["event_id"] == sent["event_id"]
    assert fetched["content"]["body"] == "Disposable staging integrity check"
    encoded_migrated = urllib.parse.quote(migrated_room, safe="")
    topic = request("PUT", f"/rooms/{encoded_migrated}/state/m.room.topic", {"topic": "Disposable staging auth/state test"})
    assert topic.get("event_id", "").startswith("$")
    assert request("GET", f"/rooms/{encoded_migrated}/state/m.room.topic")["topic"] == "Disposable staging auth/state test"
    print(json.dumps({"native_create_v12": True, "native_send_and_fetch": True, "migrated_v12_state_auth": True}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("Native staging HTTP test failed: " + type(error).__name__, file=sys.stderr)
        sys.exit(1)
