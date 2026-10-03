#!/usr/bin/env python3
"""Offline room import for the pinned Synapse image. Synapse must be stopped."""
import argparse
import json
import os
import sys
from pathlib import Path

from rooms.plan import build_plan, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--server-name", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--archive", help="Credential-free immutable archive SQLite output")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--synapse-stopped", action="store_true")
    options = parser.parse_args()
    plan = build_plan(options.source)
    report = summary(plan)
    if options.apply:
        if not options.synapse_stopped:
            parser.error("Stop Synapse and pass --synapse-stopped before importing")
        from rooms.postgres import import_plan
        report.update(import_plan(plan, options.server_name))
    if options.archive:
        from rooms.archive_export import export_archive
        report.update(export_archive(options.source, options.archive, plan))
    target = Path(options.manifest)
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as output:
        json.dump(report, output, indent=2)
    print(json.dumps({key: value for key, value in report.items() if key not in ("source_sha256",)}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # DSN/password/SQL values must never appear in terminal output.
        print("Room migration failed: " + type(error).__name__, file=sys.stderr)
        sys.exit(1)
