"""Trusted root coordinator. Exact public-main SHA on stdin, no deployment args.

Changes only reviewed compose/code/dependencies. Private homeserver config,
signing keys, token database/secret and immutable archive data are preserved.
Importers and Workers are never run. Keep this file root-owned beside the shell
entrypoint; the deploy workflow does not update this trusted coordinator itself.
"""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import urllib.error
import signal

ORIGIN = "https://github.com/koljasagorski/matrix-workers.git"
BASE = Path("/opt/matrix")
STATE = Path("/var/lib/matrix-deploy")
GATE = BASE / "compat/maintenance.flag"
CODE_PATHS = ("docker-compose.yml", "modules/migration_auth", "compat/auth/migration_auth", "compat/archive/archive.py", "compat/venv")
# Official Synapse 1.162 multiwriter generators. Native startup checks new
# generators too; this gate explicitly covers all current table/position pairs.
STREAMS = {
    "events": ("events_stream_seq", [("events", "stream_ordering", ">0"), ("current_state_delta_stream", "stream_id", ">0"), ("ex_outlier_stream", "event_stream_ordering", ">0")]),
    "backfill": ("events_backfill_stream_seq", [("events", "stream_ordering", "<0"), ("ex_outlier_stream", "event_stream_ordering", "<0")]),
    "account_data": ("account_data_sequence", [(name, "stream_id", "") for name in ("account_data", "room_account_data", "room_tags_revisions")]),
    "receipts": ("receipts_sequence", [("receipts_linearized", "stream_id", "")]),
    "to_device": ("device_inbox_sequence", [(name, "stream_id", "") for name in ("device_inbox", "device_federation_outbox")]),
    "device_lists_stream": ("device_lists_sequence", [(name, "stream_id", "") for name in ("device_lists_stream", "user_signature_stream", "device_lists_outbound_pokes", "device_lists_changes_in_room", "device_lists_remote_pending", "device_lists_changes_converted_stream_position")]),
    "pushers": ("pushers_sequence", [("pushers", "id", ""), ("deleted_pushers", "stream_id", "")]),
    "presence_stream": ("presence_stream_sequence", [("presence_stream", "stream_id", "")]),
    "caches": ("cache_invalidation_stream_seq", [("cache_invalidation_stream_by_instance", "stream_id", "")]),
    "push_rules_stream": ("push_rules_stream_sequence", [("push_rules_stream", "stream_id", "")]),
    "e2e_cross_signing_keys": ("e2e_cross_signing_keys_sequence", [("e2e_cross_signing_keys", "stream_id", "")]),
    "profile_updates": ("profile_updates_sequence", [("profile_updates", "stream_id", "")]),
    "sticky_events": ("sticky_events_sequence", [("sticky_events", "stream_id", "")]),
    "thread_subscriptions": ("thread_subscriptions_sequence", [("thread_subscriptions", "stream_id", "")]),
    "un_partial_stated_room_stream": ("un_partial_stated_room_stream_sequence", [("un_partial_stated_room_stream", "stream_id", "")]),
    "un_partial_stated_event_stream": ("un_partial_stated_event_stream_sequence", [("un_partial_stated_event_stream", "stream_id", "")]),
}


def stream_check_sql():
    values = []
    for stream, (sequence, tables) in STREAMS.items():
        highwaters = [f"COALESCE((SELECT MAX(ABS(stream_id)) FROM stream_positions WHERE stream_name='{stream}'),0)"]
        highwaters += [f"COALESCE((SELECT MAX(ABS({column})) FROM {table}" +
                       (f" WHERE {column}{condition}" if condition else "") + "),0)" for table, column, condition in tables]
        values.append(f"('{sequence}',GREATEST(0," + ",".join(highwaters) + "))")
    return ("WITH required(sequence_name,highwater) AS (VALUES " + ",".join(values) +
            ") SELECT COUNT(*)=" + str(len(STREAMS)) + " AND BOOL_AND(COALESCE(s.last_value,0)>=r.highwater) "
            "FROM required r JOIN pg_sequences s ON s.schemaname='public' AND s.sequencename=r.sequence_name;")


def validate_sha(value):
    if not re.fullmatch(r"[0-9a-f]{40}\n?", value):
        raise ValueError("Expected exactly one lowercase 40-character commit SHA")
    return value.rstrip("\n")


def relocate_venv(environment, original):
    """Keep copied console scripts usable after deleting the preparation path."""
    old, new = str(original).encode(), str(environment).encode()
    for executable in (environment / "bin").iterdir():
        if executable.is_symlink() or not executable.is_file():
            continue
        data = executable.read_bytes()
        first, separator, remaining = data.partition(b"\n")
        if first.startswith(b"#!" + old + b"/"):
            executable.write_bytes(first.replace(old, new, 1) + separator + remaining)
    config = environment / "pyvenv.cfg"
    config.write_text(config.read_text().replace(str(original), str(environment)))


def validate_transition(old, new):
    if set(new.get("services", {})) != {"postgres", "synapse"}:
        raise ValueError("Deployment may only operate the existing Matrix services")
    for service in ("postgres", "synapse"):
        before, after = dict(old["services"][service]), dict(new["services"][service])
        before_image, after_image = before.pop("image"), after.pop("image")
        if service == "postgres":
            images = [re.fullmatch(r"postgres:(\d+)-alpine@sha256:[0-9a-f]{64}", image) for image in (before_image, after_image)]
            if not all(images) or images[0][1] != images[1][1]:
                raise ValueError("PostgreSQL major upgrades require a separate reviewed migration")
        elif not re.fullmatch(r"ghcr\.io/element-hq/synapse:v\d+\.\d+\.\d+@sha256:[0-9a-f]{64}", after_image):
            raise ValueError("Synapse image must use the reviewed publisher and immutable digest")
        # Keep ports, env/roles, names, mounts, dependency ordering and networks.
        # Resource limits and restart policy can be tuned separately in review.
        for key in ("cpus", "mem_limit", "restart"):
            before.pop(key, None)
            after.pop(key, None)
        if before != after:
            raise ValueError("Deployment cannot change existing service identity/config/data mounts")
    for key in ("volumes", "networks"):
        if old.get(key) != new.get(key):
            raise ValueError("Deployment cannot change Matrix storage or network identity")


def deploy(operations, sha):
    """Pure sequencing with fake operations in regression tests."""
    stage = operations.prepare(sha)
    old, new = operations.specifications(stage)
    validate_transition(old, new)
    operations.pull(stage)
    operations.backup_online()
    operations.verify_main(sha)
    stopped = changed = False
    snapshot = None
    try:
        operations.close_gate()
        # Finish in-flight writes before the final rollback snapshot. The
        # complete online backup precedes any live maintenance/change action.
        stopped = True
        operations.stop_native()
        snapshot = operations.snapshot()
        changed = True  # Includes partial installation failures.
        operations.install(stage)
        operations.start_services()
        operations.healthy(new)
        operations.check_streams()
        operations.record(sha)
        operations.open_gate()
        return "deployed"
    except Exception:
        try:
            if changed:
                operations.stop_native()
                try:
                    schema_changed = operations.schema() != snapshot["schema"]
                except Exception:
                    schema_changed = True
                operations.restore_code(snapshot)
                if schema_changed:
                    operations.restore_database(snapshot)
            if stopped:
                operations.start_services()
            operations.healthy(old)
            operations.check_streams()
            operations.open_gate()
        except Exception:
            # Public traffic remains blocked if rollback readiness fails.
            raise RuntimeError("Deployment and recovery failed; maintenance remains closed") from None
        raise RuntimeError("Deployment failed; previous healthy release restored") from None


class Operations:
    def __init__(self):
        self.stage = None
        self.gateway_health = "http://127.0.0.1:18009/health"

    def command(self, *args, input=None):
        # Never echo commands, SQL, env values, config output or stderr: they can
        # contain private PG credentials or synthetic native access tokens.
        result = subprocess.run(args, input=input, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode:
            raise RuntimeError("A deployment operation failed")
        return result.stdout

    def compose(self, *args, path=None):
        return self.command("docker", "compose", "--project-directory", str(BASE), "-f", str(path or BASE / "docker-compose.yml"), *args)

    def verify_main(self, sha):
        branch = self.command("git", "ls-remote", ORIGIN, "refs/heads/main").decode().split()
        if not branch or branch[0] != sha:
            raise ValueError("Commit is no longer public origin main")

    def prepare(self, sha):
        self.verify_main(sha)
        config = json.loads((BASE / "compat/gateway.json").read_text())
        if GATE.exists():
            raise ValueError("An existing maintenance operation must finish before deployment")
        if config.get("maintenance_file") != str(GATE):
            raise ValueError("Gateway maintenance support must be configured before automated deployment")
        hosts = config.get("host", "127.0.0.1")
        hosts = hosts if isinstance(hosts, list) else [hosts]
        port = config.get("port", 18009)
        if "127.0.0.1" not in hosts or type(port) is not int or not 1 <= port <= 65535:
            raise ValueError("Gateway must expose a real loopback readiness listener")
        self.gateway_health = f"http://127.0.0.1:{port}/health"
        STATE.mkdir(mode=0o700, parents=True, exist_ok=True)
        stage = Path(tempfile.mkdtemp(prefix="stage-", dir=STATE))
        self.stage = stage
        checkout = stage / "checkout"
        self.command("git", "-c", "credential.helper=", "clone", "--no-checkout", "--single-branch", "--branch", "main", ORIGIN, str(checkout))
        self.command("git", "-C", str(checkout), "-c", "core.hooksPath=/dev/null", "checkout", "--detach", sha)
        for path in (checkout / "scripts/migration/auth/migration_auth", checkout / "scripts/migration/archive.py"):
            if path.is_symlink() or any(child.is_symlink() for child in path.rglob("*")):
                raise ValueError("Deployment source code must not contain symlinks")
        requirements = []
        for filename in ("scripts/migration/auth/requirements.txt", "scripts/migration/requirements-federation-replay.txt"):
            for line in (checkout / filename).read_text().splitlines():
                if not line.strip() or line.startswith("#"):
                    continue
                if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]*(?:\[[a-z,]+\])?==\d+\.\d+\.\d+", line):
                    raise ValueError("Only exact PyPI dependency pins are deployable")
                requirements.append(line)
        (stage / "requirements.txt").write_text("\n".join(sorted(set(requirements))) + "\n")
        self.command("python3", "-m", "venv", str(stage / "venv"))
        self.command(str(stage / "venv/bin/python"), "-m", "pip", "install", "--only-binary=:all:", "--requirement", str(stage / "requirements.txt"))
        return stage

    def pull(self, stage):
        # Validate service identity/image publishers before pulling any image.
        self.compose("pull", path=stage / "checkout/deploy/vps/docker-compose.yml")

    def specifications(self, stage):
        return (json.loads(self.compose("config", "--format", "json")),
                json.loads(self.compose("config", "--format", "json", path=stage / "checkout/deploy/vps/docker-compose.yml")))

    def backup_online(self):
        previous = Path("/var/backups/matrix/last-success")
        before = previous.read_bytes() if previous.exists() else None
        self.command("/opt/matrix/bin/backup.sh")
        if not previous.exists() or previous.read_bytes() == before:
            raise RuntimeError("Backup did not complete a new verified snapshot")
        self.command("docker", "exec", "-i", "matrix-postgres", "pg_restore", "--list", input=Path("/var/backups/matrix/synapse.dump").read_bytes())

    def close_gate(self):
        GATE.write_text("reviewed deployment in progress\n")
        GATE.chmod(0o644)
        try:
            urllib.request.urlopen(self.gateway_health, timeout=5)
        except urllib.error.HTTPError as response:
            if response.code == 503:
                return
        raise RuntimeError("Gateway did not close the public maintenance gate")

    def open_gate(self):
        GATE.unlink()

    def stop_native(self):
        self.command("docker", "stop", "--time", "180", "matrix-synapse")

    def schema(self):
        data = self.command("docker", "exec", "matrix-postgres", "pg_dump", "-U", "matrix", "-d", "synapse", "--schema-only")
        # PostgreSQL17's random psql restrict nonce is not schema data.
        normalized = b"\n".join(line for line in data.splitlines() if not line.startswith(b"\\") and not line.startswith(b"--"))
        # Migrations can change version/delta metadata or data without changing
        # SQL table definitions. Such upgrades must restore the old dump too.
        metadata = self.command("docker", "exec", "matrix-postgres", "psql", "-U", "matrix", "-d", "synapse", "-Atc",
            "SELECT json_build_object('version',(SELECT json_agg(s) FROM schema_version s),"
            "'compat',(SELECT json_agg(s) FROM schema_compat_version s),"
            "'deltas',(SELECT json_agg(s) FROM (SELECT * FROM applied_schema_deltas ORDER BY version,file) s),"
            "'modules',(SELECT json_agg(s) FROM (SELECT * FROM applied_module_schemas ORDER BY module_name,file) s));")
        return hashlib.sha256(normalized + b"\n" + metadata).hexdigest()

    def snapshot(self):
        snapshot = Path(tempfile.mkdtemp(prefix="rollback-", dir=STATE))
        dump = self.command("docker", "exec", "matrix-postgres", "pg_dump", "-U", "matrix", "-d", "synapse", "-Fc", "-Z0")
        (snapshot / "synapse.dump").write_bytes(dump)
        self.command("docker", "exec", "-i", "matrix-postgres", "pg_restore", "--list", input=dump)
        self.command("tar", "-C", str(BASE), "-cf", str(snapshot / "protected-data.tar"),
                     "synapse", "compat", "modules", "migration", "docker-compose.yml", "postgres.env")
        self.command("tar", "-tf", str(snapshot / "protected-data.tar"))
        for name in CODE_PATHS:
            path = BASE / name
            destination = snapshot / "code" / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            if path.is_dir():
                shutil.copytree(path, destination, symlinks=True)
            else:
                shutil.copy2(path, destination)
        previous = STATE / "deployed-sha"
        return {"directory": snapshot, "schema": self.schema(),
                "previous_sha": previous.read_bytes() if previous.exists() else None}

    def install(self, stage):
        checkout = stage / "checkout"
        sources = {"docker-compose.yml": checkout / "deploy/vps/docker-compose.yml",
            "modules/migration_auth": checkout / "scripts/migration/auth/migration_auth",
            "compat/auth/migration_auth": checkout / "scripts/migration/auth/migration_auth",
            "compat/archive/archive.py": checkout / "scripts/migration/archive.py", "compat/venv": stage / "venv"}
        for name, source in sources.items():
            destination = BASE / name
            if destination.is_dir():
                shutil.rmtree(destination)
            if source.is_dir():
                shutil.copytree(source, destination, symlinks=True)
            else:
                shutil.copy2(source, destination)
        relocate_venv(BASE / "compat/venv", stage / "venv")
        # Code and interpreter remain readable by the unprivileged gateway.
        self.command("chown", "-R", "root:matrix", str(BASE / "compat/auth"), str(BASE / "compat/venv"), str(BASE / "modules"))
        self.command("chmod", "-R", "g+rX", str(BASE / "compat/auth"), str(BASE / "compat/venv"), str(BASE / "modules"))
        # Synapse's container UID is not the host gateway group. These modules
        # contain public source code; private configs/keys are separate mounts.
        self.command("chmod", "-R", "a+rX", str(BASE / "modules/migration_auth"))
        self.command("chown", "root:matrix", str(BASE / "compat/archive/archive.py"))
        self.command("chmod", "644", str(BASE / "compat/archive/archive.py"))

    def start_services(self):
        self.compose("up", "-d", "--wait", "--wait-timeout", "120")
        self.command("systemctl", "restart", "matrix-gateway")

    def healthy(self, specification):
        image = specification["services"]["synapse"]["image"]
        expected = re.search(r":v(\d+\.\d+\.\d+)@", image)[1]
        for attempt in range(60):
            try:
                for url in ("http://127.0.0.1:18008/_matrix/client/versions", self.gateway_health + "?maintenance_probe=true"):
                    with urllib.request.urlopen(url, timeout=5) as response:
                        payload = json.load(response)
                        if response.status != 200 or not isinstance(payload, dict):
                            raise RuntimeError("Homeserver readiness failed")
                version = self.command("docker", "exec", "matrix-synapse", "python", "-c", "import synapse;print(synapse.__version__)").decode().strip()
                if version == expected:
                    try:
                        urllib.request.urlopen(self.gateway_health, timeout=5)
                    except urllib.error.HTTPError as response:
                        if response.code == 503:
                            return
                    raise RuntimeError("Public maintenance gate opened before deployment completed")
            except Exception:
                pass
            time.sleep(2)
        raise RuntimeError("Pinned native version or gateway did not become ready")

    def check_streams(self):
        expected = self.command("docker", "exec", "matrix-synapse", "python", "-c", "from synapse.storage.schema import SCHEMA_VERSION;print(SCHEMA_VERSION)").decode().strip()
        actual = self.command("docker", "exec", "matrix-postgres", "psql", "-U", "matrix", "-d", "synapse", "-Atc", "SELECT version FROM schema_version;").decode().strip()
        if not expected.isdecimal() or actual != expected:
            raise RuntimeError("Native database schema does not match the pinned server")
        result = self.command("docker", "exec", "matrix-postgres", "psql", "-U", "matrix", "-d", "synapse", "-Atc", stream_check_sql()).decode().strip()
        if result != "t":
            raise RuntimeError("Native sequence/table/stream-position high-water check failed")

    def restore_code(self, snapshot):
        for name in CODE_PATHS:
            source, destination = snapshot["directory"] / "code" / name, BASE / name
            if destination.is_dir():
                shutil.rmtree(destination)
            if source.is_dir():
                shutil.copytree(source, destination, symlinks=True)
            else:
                shutil.copy2(source, destination)
        self.command("chown", "-R", "root:matrix", str(BASE / "compat/auth"), str(BASE / "compat/venv"), str(BASE / "modules"))
        self.command("chmod", "-R", "g+rX", str(BASE / "compat/auth"), str(BASE / "compat/venv"), str(BASE / "modules"))
        self.command("chmod", "-R", "a+rX", str(BASE / "modules/migration_auth"))
        previous = STATE / "deployed-sha"
        if snapshot["previous_sha"] is None:
            previous.unlink(missing_ok=True)
        else:
            previous.write_bytes(snapshot["previous_sha"])

    def restore_database(self, snapshot):
        # Synapse remains stopped and public traffic remains gated. Preserve
        # database identity/role/volume, recreate only its schema/data via dump.
        self.command("docker", "exec", "matrix-postgres", "psql", "-U", "matrix", "-d", "synapse", "-v", "ON_ERROR_STOP=1", "-c", "DROP SCHEMA public CASCADE; CREATE SCHEMA public AUTHORIZATION matrix;")
        self.command("docker", "exec", "-i", "matrix-postgres", "pg_restore", "-U", "matrix", "-d", "synapse", "--exit-on-error", input=(snapshot["directory"] / "synapse.dump").read_bytes())

    def record(self, sha):
        (STATE / "deployed-sha").write_text(sha + "\n")


def main():
    if os.geteuid() != 0 or len(sys.argv) != 1:
        raise ValueError("Root forced-command entrypoint only")
    sha = validate_sha(sys.stdin.read(128))
    os.umask(0o077)
    with open("/run/matrix-deploy.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        def interrupted(signum, frame):
            raise InterruptedError("Deployment interrupted")
        for event in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
            signal.signal(event, interrupted)
        operations = Operations()
        try:
            result = deploy(operations, sha)
        finally:
            if operations.stage is not None:
                shutil.rmtree(operations.stage)
    print(result)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("Reviewed deployment failed: " + type(error).__name__, file=sys.stderr)
        sys.exit(1)
