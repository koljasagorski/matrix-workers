import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("media_import", Path(__file__).parents[1] / "import-media.py")
media_import = importlib.util.module_from_spec(spec)
spec.loader.exec_module(media_import)


class MediaPreflightTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.export = self.root / "export"
        self.export.mkdir()
        self.target = self.root / "target"
        self.source = self.root / "source.sqlite"
        source = sqlite3.connect(self.source)
        source.execute("CREATE TABLE media(media_id TEXT,content_type TEXT,content_length INTEGER,created_at INTEGER,upload_name TEXT,filename TEXT,user_id TEXT,quarantined INTEGER)")
        self.data = b"preserved-binary-image"
        self.media_id = "abcdef123456"
        source.execute("INSERT INTO media VALUES (?,?,?,?,?,?,?,?)", (self.media_id, "image/png", len(self.data), 1, "original.png", "original.png", "@alice:example.org", 0))
        source.commit()
        source.close()
        (self.export / "object.bin").write_bytes(self.data)
        self.inventory = {"complete": True, "objects": [{"key": self.media_id, "path": "object.bin", "size": len(self.data),
            "bytes": len(self.data), "sha256": hashlib.sha256(self.data).hexdigest(), "http_metadata": {"contentType": "image/png"}}]}

    def tearDown(self):
        self.directory.cleanup()

    def assert_preflight_fails_without_sql_or_files(self, exception):
        inventory = self.root / "inventory.json"
        inventory.write_text(json.dumps(self.inventory))
        connect = Mock(side_effect=AssertionError("Target SQL must not run before successful preflight"))
        args = ["import-media.py", "--source", str(self.source), "--inventory", str(inventory),
            "--export-dir", str(self.export), "--media-root", str(self.target), "--manifest", str(self.root / "manifest.json"), "--commit"]
        with patch.object(sys, "argv", args), patch.dict(sys.modules, {"psycopg": types.SimpleNamespace(connect=connect)}):
            with self.assertRaises(exception):
                media_import.main()
        connect.assert_not_called()
        self.assertFalse(self.target.exists())
        self.assertFalse((self.root / "manifest.json").exists())

    def test_complete_verified_blob_keeps_identifier_and_native_path(self):
        media, thumbs, files = media_import.prepare(self.source, self.inventory, self.export, self.target)
        self.assertEqual(media[0]["media_id"], self.media_id)
        self.assertEqual(files[0][0].read_bytes(), self.data)
        self.assertEqual(files[0][1], self.target / "local_content" / "ab" / "cd" / "ef123456")
        self.assertEqual(thumbs, [])
        self.assertFalse(self.target.exists())

    def test_corrupt_checksum_is_detected_before_any_target_change(self):
        (self.export / "object.bin").write_bytes(b"X" * len(self.data))
        self.assert_preflight_fails_without_sql_or_files(ValueError)

    def test_missing_blob_is_detected_before_any_target_change(self):
        (self.export / "object.bin").unlink()
        self.assert_preflight_fails_without_sql_or_files(FileNotFoundError)

    def test_path_traversal_is_rejected_before_any_target_change(self):
        self.inventory["objects"][0]["path"] = "../outside.bin"
        (self.root / "outside.bin").write_bytes(self.data)
        self.assert_preflight_fails_without_sql_or_files(ValueError)

    def test_duplicate_or_incomplete_inventory_is_rejected_before_sql(self):
        self.inventory["objects"].append(dict(self.inventory["objects"][0]))
        self.assert_preflight_fails_without_sql_or_files(ValueError)
        self.inventory["objects"].pop()
        self.inventory["complete"] = False
        self.assert_preflight_fails_without_sql_or_files(ValueError)

    def test_original_metadata_size_mismatch_is_rejected_before_sql(self):
        source = sqlite3.connect(self.source)
        source.execute("UPDATE media SET content_length=content_length+1")
        source.commit()
        source.close()
        self.assert_preflight_fails_without_sql_or_files(ValueError)


if __name__ == "__main__":
    unittest.main()
