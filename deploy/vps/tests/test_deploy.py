"""No network/root/Docker: exercise the real coordinator with failing operations."""
import copy
import importlib.util
from pathlib import Path
import unittest
import tempfile

spec = importlib.util.spec_from_file_location("reviewed_deploy", Path(__file__).resolve().parents[1] / "deploy-reviewed.py")
deployment = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deployment)
SHA = "a" * 40


def configuration(version="1.162.0", postgres="17"):
    return {"services": {
        "postgres": {"image": f"postgres:{postgres}-alpine@sha256:" + "1" * 64,
                     "container_name": "matrix-postgres", "volumes": ["persistent-data"]},
        "synapse": {"image": f"ghcr.io/element-hq/synapse:v{version}@sha256:" + "2" * 64,
                    "container_name": "matrix-synapse", "volumes": ["private-config"]}},
        "volumes": {"persistent-data": {}}, "networks": {"matrix": {"external": True}}}


class FakeOperations:
    def __init__(self, failure=None, schema_changes=False, recovery_failure=False):
        self.failure = failure
        self.schema_changes = schema_changes
        self.recovery_failure = recovery_failure
        self.events = []
        self.gate = False
        self.running = True
        self.code = "old"
        self.database = "old"
        self.recorded = "previous"
        self.protected = {"signing_key": "same", "token_bridge": "same", "archive_ciphertext": "same"}
        self.old, self.new = configuration(), configuration("1.163.0")

    def event(self, name):
        self.events.append(name)
        if self.failure == name:
            self.failure = None  # Inject a one-shot failure; recovery can work.
            raise InterruptedError("simulated operation failure")

    def prepare(self, sha):
        self.event("prepare")
        return "reviewed-source"

    def specifications(self, stage):
        self.event("specifications")
        return self.old, self.new

    def pull(self, stage): self.event("pull")
    def backup_online(self): self.event("backup")
    def verify_main(self, sha): self.event("verify_main")

    def close_gate(self):
        self.gate = True
        self.event("close_gate")

    def open_gate(self):
        self.event("open_gate")
        self.gate = False

    def stop_native(self):
        self.event("stop")
        self.running = False

    def snapshot(self):
        self.event("snapshot")
        assert self.gate and not self.running
        return {"schema": "old", "database": self.database, "recorded": self.recorded,
                "code": self.code, "protected": copy.deepcopy(self.protected)}

    def install(self, stage):
        assert self.gate and not self.running
        self.code = "new"  # Deliberately fail after a partial installation.
        self.event("install")

    def start_services(self):
        assert self.gate
        self.event("start")
        self.running = True
        if self.code == "new" and self.schema_changes:
            self.database = "migrated"

    def healthy(self, configuration):
        assert self.gate and self.running
        kind = "healthy_new" if configuration is self.new else "healthy_old"
        self.event(kind)
        if kind == "healthy_old" and self.recovery_failure:
            raise RuntimeError("old native server unavailable")
        assert self.code == ("new" if kind == "healthy_new" else "old")

    def check_streams(self): self.event("streams")

    def schema(self):
        self.event("schema")
        return "new" if self.database == "migrated" else "old"

    def restore_code(self, snapshot):
        assert self.gate and not self.running
        self.event("restore_code")
        self.code, self.recorded = snapshot["code"], snapshot["recorded"]
        assert self.protected == snapshot["protected"]

    def restore_database(self, snapshot):
        assert self.gate and not self.running
        self.event("restore_database")
        self.database = snapshot["database"]

    def record(self, sha):
        self.event("record")
        self.recorded = sha


class DeploymentTests(unittest.TestCase):
    def test_prepared_venv_remains_usable_after_its_source_path_is_removed(self):
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "live"
            (target / "bin").mkdir(parents=True)
            original = Path(temporary) / "prepare"
            (target / "bin/pip").write_text(f"#!{original}/bin/python\nprint('unchanged body')\n")
            (target / "bin/pip").chmod(0o755)
            (target / "bin/python").symlink_to('/usr/bin/python3')
            (target / "bin/other").write_text('#!/bin/sh\necho unchanged\n')
            (target / "pyvenv.cfg").write_text(f"home=/usr/bin\ncommand=/usr/bin/python3 -m venv {original}\n")
            deployment.relocate_venv(target, original)
            self.assertEqual((target / "bin/pip").read_text(), f"#!{target}/bin/python\nprint('unchanged body')\n")
            self.assertEqual((target / "bin/pip").stat().st_mode & 0o777, 0o755)
            self.assertTrue((target / "bin/python").is_symlink())
            self.assertEqual((target / "bin/other").read_text(), '#!/bin/sh\necho unchanged\n')
            self.assertNotIn(str(original), (target / "pyvenv.cfg").read_text())

    def test_exact_stdin_sha(self):
        self.assertEqual(deployment.validate_sha(SHA + "\n"), SHA)
        for value in ("", SHA.upper(), SHA + "\n\n", " " + SHA, SHA + " command", "$(id)" + SHA):
            with self.subTest(value=value), self.assertRaises(ValueError):
                deployment.validate_sha(value)

    def test_success_requires_backup_gate_native_and_stream_checks(self):
        operations = FakeOperations()
        self.assertEqual(deployment.deploy(operations, SHA), "deployed")
        self.assertEqual(operations.events, ["prepare", "specifications", "pull", "backup", "verify_main",
            "close_gate", "stop", "snapshot", "install", "start", "healthy_new", "streams", "record", "open_gate"])
        self.assertFalse(operations.gate)
        self.assertEqual(operations.recorded, SHA)
        self.assertEqual(operations.protected, {"signing_key": "same", "token_bridge": "same", "archive_ciphertext": "same"})

    def test_pg_major_mount_and_publisher_changes_rejected_before_backup_or_pull(self):
        for kind in ("major", "mount", "publisher", "network", "extra_service"):
            operations = FakeOperations()
            if kind == "major": operations.new = configuration(postgres="18")
            elif kind == "mount": operations.new["services"]["synapse"]["volumes"] = ["other"]
            elif kind == "publisher": operations.new["services"]["synapse"]["image"] = "evil/image:latest"
            elif kind == "network": operations.new["networks"] = {}
            else: operations.new["services"]["unrelated"] = {}
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                deployment.deploy(operations, SHA)
            self.assertEqual(operations.events, ["prepare", "specifications"])
            self.assertTrue(operations.running)
            self.assertFalse(operations.gate)

    def test_backup_or_moving_main_failure_leaves_live_services_untouched(self):
        for operation in ("backup", "verify_main"):
            operations = FakeOperations(failure=operation)
            with self.subTest(operation=operation), self.assertRaises(InterruptedError):
                deployment.deploy(operations, SHA)
            self.assertNotIn("close_gate", operations.events)
            self.assertTrue(operations.running)
            self.assertEqual(operations.code, "old")

    def test_partial_install_rolls_back_code_without_restoring_unchanged_database(self):
        operations = FakeOperations(failure="install")
        with self.assertRaisesRegex(RuntimeError, "previous healthy release restored"):
            deployment.deploy(operations, SHA)
        self.assertEqual(operations.code, "old")
        self.assertEqual(operations.database, "old")
        self.assertNotIn("restore_database", operations.events)
        self.assertLess(operations.events.index("restore_code"), operations.events.index("healthy_old"))
        self.assertFalse(operations.gate)
        self.assertEqual(operations.recorded, "previous")

    def test_stream_check_failure_rolls_back_before_any_release_is_recorded(self):
        operations = FakeOperations(failure="streams")
        with self.assertRaisesRegex(RuntimeError, "previous healthy release restored"):
            deployment.deploy(operations, SHA)
        self.assertEqual(operations.code, "old")
        self.assertNotIn("record", operations.events)
        self.assertEqual(operations.events.count("streams"), 2)
        self.assertFalse(operations.gate)

    def test_schema_migration_failure_restores_dump_before_starting_old_code(self):
        operations = FakeOperations(failure="healthy_new", schema_changes=True)
        with self.assertRaisesRegex(RuntimeError, "previous healthy release restored"):
            deployment.deploy(operations, SHA)
        self.assertEqual((operations.code, operations.database), ("old", "old"))
        restored = operations.events.index("restore_database")
        self.assertLess(restored, operations.events.index("start", restored))
        self.assertLess(operations.events.index("healthy_old"), operations.events.index("open_gate"))
        self.assertFalse(operations.gate)

    def test_unknown_schema_conservatively_restores_database(self):
        operations = FakeOperations(failure="healthy_new", schema_changes=True)
        original = operations.schema
        def unavailable_schema():
            operations.failure = "schema"
            return original()
        operations.schema = unavailable_schema
        with self.assertRaisesRegex(RuntimeError, "previous healthy release restored"):
            deployment.deploy(operations, SHA)
        self.assertIn("restore_database", operations.events)
        self.assertEqual(operations.database, "old")

    def test_failed_rollback_never_reopens_public_gate(self):
        operations = FakeOperations(failure="healthy_new", schema_changes=True, recovery_failure=True)
        with self.assertRaisesRegex(RuntimeError, "maintenance remains closed"):
            deployment.deploy(operations, SHA)
        self.assertTrue(operations.gate)
        self.assertNotIn("open_gate", operations.events)
        self.assertEqual(operations.recorded, "previous")

    def test_failure_while_closing_gate_recovers_old_readiness(self):
        operations = FakeOperations(failure="close_gate")
        with self.assertRaisesRegex(RuntimeError, "previous healthy release restored"):
            deployment.deploy(operations, SHA)
        self.assertNotIn("stop", operations.events)
        self.assertIn("healthy_old", operations.events)
        self.assertFalse(operations.gate)

    def test_gate_reopen_failure_restores_old_release_record(self):
        operations = FakeOperations(failure="open_gate")
        with self.assertRaisesRegex(RuntimeError, "previous healthy release restored"):
            deployment.deploy(operations, SHA)
        self.assertEqual(operations.code, "old")
        self.assertEqual(operations.recorded, "previous")
        self.assertFalse(operations.gate)


if __name__ == "__main__": unittest.main()
