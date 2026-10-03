The room importer runs only in the pinned Synapse `v1.162.0` image against
PostgreSQL schema94, with Synapse stopped and a freshly initialized room store.
The user importer must commit first. Use environment variables for database
credentials; never pass a password or access token as a command-line argument.

```sh
python /migration/import-rooms.py --source /migration/source.sqlite \
  --server-name m.sgr.ski --manifest /migration/rooms-manifest.json
python /migration/import-rooms.py --source /migration/source.sqlite \
  --server-name m.sgr.ski --manifest /migration/rooms-manifest.json \
  --archive /migration/archive.sqlite \
  --apply --synapse-stopped
```

`MATRIX_IMPORT_DSN` is supported. Alternatively the importer reads `PGHOST`,
`PGDATABASE`, `POSTGRES_USER`, and `POSTGRES_PASSWORD`. The apply phase uses one
transaction, an advisory lock, an empty-store guard and a source-hash import
marker. A retry of the same committed source returns its saved report. A new
source cannot overwrite an occupied room store. Restore an isolated rehearsal
database from its empty backup for a second snapshot rehearsal.

Source room IDs, reference event IDs, graph edges, content, hashes, signatures,
current state and ciphertext are retained. The native parser computes every
reference ID before import. Native database event format is taken from the room
version:10/11 use3;12 uses4. Cached auth/state PDUs are native outliers with
negative stream positions; positive source timeline positions stay unchanged.
Every room extremity receives the complete source current-state group, so new
events authorize against complete state. Saved historic snapshots are retained.
For older events without a complete source snapshot, the native state resolver
reconstructs only known previous/auth state; the manifest explicitly identifies
those events. Missing prior graph events remain ordinary backward extremities.

The actual first snapshot contained19rooms/735source events:14native rooms,
675unique native PDUs,184timeline events,491cached outliers, and119native
receipts. The remaining5receipt records belong to5older rooms with malformed
unsigned current-state PDUs. Those rooms remain available through the archive
hook with all original IDs and ciphertext. One independent malformed historical
leave in a current remote room is excluded from native storage and served only
as authenticated history. It never changes native membership/state.

The archive hook is `scripts/migration/archive.py`. Add its parent directory to
the gateway's Python module path, set `archive_module` to `archive`, and provide
`MATRIX_ARCHIVE_SOURCE` and `MATRIX_ARCHIVE_MANIFEST`. Use `--archive` to produce
a standalone SQLite subset with only event/state/membership/per-room account-data
and receipt tables for the affected rooms. It contains no credentials, token
tables, password hashes, global secret-storage settings or private key tables.
The manifest binds both its archive SHA256 and the original full-source SHA256;
an internal provenance row carries the same original-source hash. The full source
remains in the protected migration backup. Give the gateway user read access to
the sanitized archive and manifest only. Native whoami runs before every hook
request, including already-native sessions. The hook checks historical joined
membership separately. Initial/full-state classic syncs contain immutable
archive snapshots and retain the native next_batch. Incremental syncs contain
only native changes. Archived room writes are denied. Native history pagination
can overlay the isolated malformed leave inside its exact `(depth,stream)` page;
its current membership remains native.

Tests use Synapse's actual parser/state resolver for signed version12 fixtures,
and the actual aiohttp gateway for archive authorization and sync behavior:

```sh
python -m unittest discover -s /migration/rooms/tests -v
python -m unittest discover -s scripts/migration/archive_tests -v
```

Before cutover, restart the isolated Synapse and verify every native joined room's
state/history, classic/sliding sync, backup/device counts, archive access,
revocation, and a new native event. Compare `native_pdu_sha256` with native
event_json, verify migration source hashes, and preserve the full source SQLite
file and authoritative Durable Object exports alongside native backups.

`rooms/tests/staging_postgres.py` exercises late-failure rollback, idempotent
replay, all native wire JSON/codecs/current-state references and sequence values
against a uniquely named disposable PostgreSQL database. It requires a schema-only
dump of the actual initialized schema94 and drops only its own test database.
`rooms/tests/native_http.py` tests native v12 room creation, message send/fetch and
new state authorization in a migrated local v12 room. It enforces loopback access
and a configured empty federation whitelist. Those staging-only events and
devices must be discarded by the final fresh-database rehearsal/import.
