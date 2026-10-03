"""Boot isolated Docker Synapse/PG, import synthetic signed v12 data, query API.

No production credentials/data are consumed. Network/container names and files
are temporary. Clean up only resources created by this run, even on failure.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time
import uuid
import sys


def run(*args, quiet=False):
    return subprocess.run(args, check=True, text=True, stdout=subprocess.PIPE if quiet else None,
                          stderr=subprocess.PIPE if quiet else None)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--synapse-image", required=True)
    parser.add_argument("--postgres-image", required=True)
    options = parser.parse_args()
    for image in (options.synapse_image, options.postgres_image):
        if not re.fullmatch(r"(?:ghcr\.io/element-hq/synapse:v\d+\.\d+\.\d+|postgres:\d+-alpine)@sha256:[0-9a-f]{64}", image):
            raise ValueError("Smoke test images must be digest-pinned deployment images")
    name = "matrix-ci-" + uuid.uuid4().hex[:12]
    pg, native = name + "-pg", name + "-synapse"
    created = []
    repo = Path(__file__).resolve().parents[4]
    password = uuid.uuid4().hex
    scratch = repo / ".local"
    scratch.mkdir(exist_ok=True)
    try:
        # Docker Desktop reliably shares the checked-out workspace; platform
        # default /var/folders temporary directories may not be shared.
        with tempfile.TemporaryDirectory(prefix="matrix-ci-", dir=scratch) as temporary:
            data = Path(temporary)
            data.chmod(0o777)  # Disposable synthetic container directory only.
            config = {"server_name": "example.org", "report_stats": False,
                "signing_key_path": "/data/signing.key", "federation_domain_whitelist": [],
                "listeners": [{"port": 8008, "tls": False, "type": "http", "bind_addresses": ["0.0.0.0"],
                    "resources": [{"names": ["client", "federation"], "compress": False}]}],
                "database": {"name": "psycopg2", "args": {"host": pg, "port": 5432, "database": "synapse", "user": "matrix", "password": password}}}
            (data / "homeserver.yaml").write_text(json.dumps(config))
            run("docker", "network", "create", name, quiet=True)
            run("docker", "run", "-d", "--name", pg, "--network", name,
                "-e", "POSTGRES_DB=synapse", "-e", "POSTGRES_USER=matrix", "-e", "POSTGRES_PASSWORD=" + password,
                "-e", "POSTGRES_INITDB_ARGS=--encoding=UTF8 --locale=C", options.postgres_image, quiet=True)
            created.append(pg)
            for attempt in range(60):
                status = subprocess.run(["docker", "exec", pg, "pg_isready", "-U", "matrix", "-d", "synapse"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if status.returncode == 0:
                    break
                time.sleep(1)
            else:
                raise RuntimeError("Isolated PostgreSQL did not become ready")
            run("docker", "run", "-d", "--name", native, "--network", name,
                "-e", "SYNAPSE_CONFIG_PATH=/data/homeserver.yaml", "-v", str(data) + ":/data",
                "--entrypoint", "python", options.synapse_image, "-m", "synapse.app.homeserver",
                "--config-path", "/data/homeserver.yaml", quiet=True)
            created.append(native)
            health = ["docker", "exec", native, "python", "-c", "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8008/_matrix/client/versions',timeout=2).read()"]
            def wait_ready():
                for attempt in range(90):
                    running = run("docker", "inspect", native, "--format", "{{.State.Running}}", quiet=True).stdout.strip()
                    if running != "true":
                        run("docker", "logs", native)
                        raise RuntimeError("Isolated native Synapse exited during startup")
                    status = subprocess.run(health, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    if status.returncode == 0:
                        return
                    time.sleep(1)
                raise RuntimeError("Isolated native Synapse did not become ready")
            wait_ready()
            run("docker", "stop", native, quiet=True)
            run("docker", "run", "--rm", "--network", "none", "-v", str(repo / "scripts/migration") + ":/migration:ro",
                "-v", str(data) + ":/fixture", "--entrypoint", "python", options.synapse_image,
                "/migration/rooms/tests/build_ci_fixture.py", "/fixture/source.sqlite", quiet=True)
            fixture_users = "INSERT INTO users(name,creation_ts,admin,deactivated,is_guest,approved) VALUES ('@alice:example.org',0,0,0,0,true); INSERT INTO access_tokens(id,user_id,token,used) VALUES (1,'@alice:example.org','disposable-native-fixture',false);"
            run("docker", "exec", pg, "psql", "-U", "matrix", "-d", "synapse", "-v", "ON_ERROR_STOP=1", "-c", fixture_users, quiet=True)
            # Also exercise actual transactional rollback/replay against the
            # initialized native schema, using a separate disposable database.
            schema = run("docker", "exec", pg, "pg_dump", "-U", "matrix", "-d", "synapse", "--schema-only", quiet=True).stdout
            (data / "schema94.sql").write_text(schema)
            base = ["docker", "run", "--rm", "--network", name, "-e", "PGHOST=" + pg,
                "-e", "PGDATABASE=synapse", "-e", "POSTGRES_USER=matrix", "-e", "POSTGRES_PASSWORD=" + password,
                "-v", str(repo / "scripts/migration") + ":/migration:ro", "-v", str(data) + ":/fixture",
                "--entrypoint", "python", options.synapse_image]
            run(*base, "/migration/rooms/tests/staging_postgres.py", "--source", "/fixture/source.sqlite",
                "--schema", "/fixture/schema94.sql", "--server-name", "example.org")
            run(*base, "/migration/import-rooms.py", "--source", "/fixture/source.sqlite", "--server-name", "example.org",
                "--manifest", "/fixture/manifest.json", "--apply", "--synapse-stopped", quiet=True)
            run("docker", "start", native, quiet=True)
            wait_ready()
            check = '''import json,urllib.request,urllib.parse
def get(path):
 r=urllib.request.Request('http://127.0.0.1:8008/_matrix/client/v3'+path,headers={'Authorization':'Bearer disposable-native-fixture'})
 return json.load(urllib.request.urlopen(r,timeout=20))
joined=get('/joined_rooms')['joined_rooms'];assert len(joined)==1
room=urllib.parse.quote(joined[0],safe='')
state=get('/rooms/'+room+'/state');assert any(e['type']=='m.room.create' for e in state)
sync=get('/sync');assert joined[0] in sync['rooms']['join']
history=get('/rooms/'+room+'/messages?dir=b&limit=10')['chunk'];assert any(e['content'].get('ciphertext')=='UNMODIFIED+/=' for e in history)
r=urllib.request.Request('http://127.0.0.1:8008/_matrix/client/v3/rooms/'+room+'/state/m.room.topic',data=b'{"topic":"Disposable CI native auth check"}',method='PUT',headers={'Authorization':'Bearer disposable-native-fixture','Content-Type':'application/json'})
assert json.load(urllib.request.urlopen(r,timeout=20))['event_id'].startswith('$')
assert get('/rooms/'+room+'/state/m.room.topic')['topic']=='Disposable CI native auth check'
print('Native schema94 startup, signed v12 state, classic sync, exact ciphertext history and future state authorization: OK')
'''
            run("docker", "exec", native, "python", "-c", check)
            spec = importlib.util.spec_from_file_location("deployment", repo / "deploy/vps/deploy-reviewed.py")
            deployment = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(deployment)
            command = ["docker", "exec", pg, "psql", "-U", "matrix", "-d", "synapse", "-At", "-v", "ON_ERROR_STOP=1", "-c"]
            assert run(*command, deployment.stream_check_sql(), quiet=True).stdout.strip() == "t"
            # Regression for the observed retained-position/rolled-back-sequence
            # crash. Corrupt only this disposable DB after all native API tests.
            run(*command, "INSERT INTO stream_positions(stream_name,instance_name,stream_id) VALUES ('to_device','master',1000000) ON CONFLICT(stream_name,instance_name) DO UPDATE SET stream_id=1000000;", quiet=True)
            assert run(*command, deployment.stream_check_sql(), quiet=True).stdout.strip() == "f"
            print("Reviewed deployment checks all16 native sequences, table high-waters and retained stream positions: OK")
    finally:
        for container in reversed(created):
            subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["docker", "network", "rm", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Subprocess exceptions can contain full commands, including disposable
        # DB env values. Report a type without echoing command arguments.
        print("Isolated migration smoke failed: " + type(error).__name__, file=sys.stderr)
        sys.exit(1)
