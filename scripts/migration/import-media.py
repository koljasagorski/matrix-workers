#!/usr/bin/env python3
"""Preserve original MXC identifiers and verify every byte before importing media."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile


def prepare(source, inventory, export_dir, media_root):
    records = {row["key"]: row for row in inventory["objects"]}
    if inventory.get("complete") is not True or len(records) != len(inventory["objects"]):
        raise ValueError("Media inventory is incomplete or contains duplicate keys")
    objects = {}
    export_dir = export_dir.resolve()
    for key, record in records.items():
        path = (export_dir / record["path"]).resolve()
        if not path.is_relative_to(export_dir):
            raise ValueError("Media export path escapes the backup")
        data = path.read_bytes()
        if len(data) != record["size"] or len(data) != record["bytes"] or hashlib.sha256(data).hexdigest() != record["sha256"]:
            raise ValueError("Media object fails size/checksum verification")
        objects[key] = path
    connection = sqlite3.connect(source.resolve().as_uri() + "?mode=ro&immutable=1", uri=True)
    connection.row_factory = sqlite3.Row
    media = [dict(row) for row in connection.execute("SELECT * FROM media")]
    connection.close()
    files, thumbnails = [], []
    for row in media:
        media_id = row["media_id"]
        if not re.fullmatch(r"[A-Za-z0-9_-]{5,255}", media_id):
            raise ValueError("Unsafe media identifier")
        record = records[media_id]
        if record["bytes"] != row["content_length"]:
            raise ValueError("Media metadata differs from the preserved object")
        files.append((objects[media_id], media_root / "local_content" / media_id[:2] / media_id[2:4] / media_id[4:]))
        for key, thumb in records.items():
            match = re.fullmatch(re.escape("thumb_" + media_id + "_") + r"(\d+)x(\d+)_(scale|crop)", key)
            if not match:
                continue
            width, height, method = int(match[1]), int(match[2]), match[3]
            mime = thumb["http_metadata"].get("contentType")
            if mime not in ("image/jpeg", "image/png", "image/webp") or not 0 < width <= 10000 or not 0 < height <= 10000:
                raise ValueError("Unsupported thumbnail metadata")
            filename = f"{width}-{height}-{mime.replace('/', '-')}-{method}"
            files.append((objects[key], media_root / "local_thumbnails" / media_id[:2] / media_id[2:4] / media_id[4:] / filename))
            thumbnails.append((media_id, width, height, mime, method, thumb["bytes"]))
    if len(files) != len(objects):
        raise ValueError("Unmapped media objects must be reconciled before cutover")
    return media, thumbnails, files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--export-dir", type=Path, required=True)
    parser.add_argument("--media-root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--dsn-env", default="MATRIX_DATABASE_DSN")
    parser.add_argument("--commit", action="store_true")
    args = parser.parse_args()
    inventory_bytes = args.inventory.read_bytes()
    media, thumbnails, files = prepare(args.source, json.loads(inventory_bytes), args.export_dir, args.media_root)
    manifest = {"source_sha256": hashlib.sha256(args.source.read_bytes()).hexdigest(),
                "inventory_sha256": hashlib.sha256(inventory_bytes).hexdigest(),
                "media": len(media), "thumbnails": len(thumbnails), "objects": len(files), "complete": False}
    if args.commit:
        import psycopg
        # Source object checks happen before any target file or SQL changes.
        for source, target in files:
            target.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as temporary:
                shutil.copyfileobj(source.open("rb"), temporary)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            temporary_path.chmod(0o640)
            os.replace(temporary_path, target)
        with psycopg.connect(os.environ[args.dsn_env]) as connection:
            with connection.cursor() as cursor:
                for row in media:
                    cursor.execute("""INSERT INTO local_media_repository
                        (media_id,media_type,media_length,created_ts,upload_name,user_id,quarantined_by,authenticated)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,false)
                        ON CONFLICT (media_id) DO UPDATE SET media_type=EXCLUDED.media_type,
                        media_length=EXCLUDED.media_length,created_ts=EXCLUDED.created_ts,
                        upload_name=EXCLUDED.upload_name,user_id=EXCLUDED.user_id,
                        quarantined_by=EXCLUDED.quarantined_by,authenticated=false""",
                        (row["media_id"], row["content_type"], row["content_length"], row["created_at"],
                         row["upload_name"] or row["filename"], row["user_id"],
                         "migration-quarantine" if row["quarantined"] else None))
                for row in thumbnails:
                    cursor.execute("""INSERT INTO local_media_repository_thumbnails
                        (media_id,thumbnail_width,thumbnail_height,thumbnail_type,thumbnail_method,thumbnail_length)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (media_id,thumbnail_width,thumbnail_height,thumbnail_type,thumbnail_method)
                        DO UPDATE SET thumbnail_length=EXCLUDED.thumbnail_length""", row)
        manifest["complete"] = True
        args.manifest.write_text(json.dumps(manifest, indent=2) + "\n")
        args.manifest.chmod(0o600)
    print(json.dumps(manifest))


if __name__ == "__main__":
    main()
