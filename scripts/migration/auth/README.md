The bridge is pinned to the official Synapse 1.162.0 module API and schema94. It
preserves existing full user IDs, device IDs, passwords, signed public keys,
encrypted secret-storage account data, backup versions, backup ciphertext and
pending device messages. It never exports decrypted room keys.

Existing access tokens cannot be inserted directly into Synapse because the
source stores only SHA256 hashes. The offline importer creates a corresponding
native token from HMAC(secret, source token hash), on the same device and with the
same expiry. The local gateway hashes an incoming legacy bearer and forwards the
corresponding native token. Native logout and device deletion remain final: the
gateway never creates or recreates tokens. Native tokens pass through unchanged.
Refresh tokens are treated similarly and retain their exported KV expiration.
Their HMAC bearer is enclosed in Synapse's required `syr_..._<CRC32>` token
format, which is validated before native database lookup.

The password module uses the official `auth_checkers` callback. It verifies the
source's PBKDF2-SHA256/100000 hash only while the native account has no password
hash. A password reset in Synapse immediately disables the previous password.
Locked, suspended and deactivated users cannot use the legacy password.

Install gateway/importer dependencies in a private virtual environment:

```sh
python3 -m venv /opt/matrix/compat/venv
/opt/matrix/compat/venv/bin/pip install -r /opt/matrix/compat/auth/requirements.txt
```

Mount `migration_auth` at `/modules/migration_auth` inside the Synapse container,
add `/modules` to its `PYTHONPATH`, and configure:

```yaml
modules:
  - module: migration_auth.password.LegacyPasswordProvider
    config: {}
```

With Synapse stopped and its freshly initialized PostgreSQL database available,
set `MATRIX_IMPORT_DSN` privately and run the importer. Do not put database
passwords or bearer tokens on a command line:

```sh
PYTHONPATH=/opt/matrix/compat/auth /opt/matrix/compat/venv/bin/python \
  /opt/matrix/migration/import-user-data.py \
  --source /opt/matrix/migration/source.sqlite --server-name m.sgr.ski \
  --user-keys /opt/matrix/migration/user-keys.json \
  --refresh-tokens /opt/matrix/migration/refresh-tokens.json \
  --client-filters /opt/matrix/migration/client-filters.json \
  --compat-dir /opt/matrix/compat
```

All three supplementary exports are required for a final import. An explicitly
incomplete staging rehearsal may use `--allow-incomplete-export`; its manifest
is marked `complete:false`. `--replace-staging` may replace an earlier staging
import only while Synapse remains stopped. It must never be run after cutover.
Restart Synapse after import so caches and native ID generators read the imported
high-water marks. The importer advances the native PostgreSQL stream sequences.

The gateway configuration is a private JSON file:

```json
{
  "host": ["172.20.0.1", "127.0.0.1"],
  "port": 18009,
  "upstream": "http://127.0.0.1:18008",
  "server_name": "m.sgr.ski",
  "token_database": "/opt/matrix/compat/tokens.sqlite",
  "secret_file": "/opt/matrix/compat/token-secret.bin",
  "archive_module": "archive",
  "maintenance_file": "/opt/matrix/compat/maintenance.flag"
}
```

Run the gateway as the dedicated `matrix` user with access logs disabled:

```sh
PYTHONPATH=/opt/matrix/compat/auth:/opt/matrix/migration \
  /opt/matrix/compat/venv/bin/python -m migration_auth.gateway \
  --config /opt/matrix/compat/gateway.json
```

The secret, token map and configuration must be owned by that user, with private
directory/file permissions. Bind only to loopback or the private reverse-proxy
network and prevent public access to the native Synapse listener. Public admin
routes are rejected by the gateway; use the private native listener through SSH
for administration. The bridge does not log request
bodies, passwords or tokens.

`/health` checks the real native versions endpoint and returns HTTP200 with
`{"status":"ok"}` only while Synapse responds successfully; outages return503.
The gateway serves both Matrix well-known discovery documents. Client discovery
has public CORS headers and supports OPTIONS. Optional `public_base_url` and
`federation_server` configuration override their defaults.

When the optional `maintenance_file` exists, proxy requests and public health
return HTTP503 with `Retry-After: 30`. Discovery remains available. A deployment
can probe native readiness at `http://127.0.0.1:18009/health?maintenance_probe=true`;
this bypass requires an actual loopback peer and never trusts forwarded headers.
It applies only to health, so clients cannot bypass maintenance for Matrix APIs.

The gateway recognizes only the exact old numeric/`_td`/`_dk`/`_rr`/`_ad` sync
position grammar. Such positions become an initial sync; opaque native positions
are preserved. No device state or crypto data is reset. Existing nonnumeric saved
filter IDs are looked up for the authenticated owner and forwarded as inline
filters. The old sliding-sync paths are mapped to Synapse's built-in simplified
sliding-sync endpoint. An optional archive module receives the normalized query,
body and a user/device identity validated by a native `whoami` request; legacy
history can therefore remain readable without being inserted as fabricated PDUs.

One unavoidable metadata conversion is the source's opaque backup etag to a
native integer etag. Backup version IDs, signed auth data and all encrypted
sessions remain unchanged. The conversion count appears in `user-import.json`.

Run the focused regressions before the real Synapse rehearsal:

```sh
python -m unittest discover -s scripts/migration/auth/tests -v
```

The integration rehearsal must additionally verify native password login and
UIA, legacy and native session sync, refresh, cross-signing/device-key queries,
backup metadata/session count, preserved `m.direct`/SSSS/tags, and revocation
after logout/device deletion. Keep all resulting credentials in private files.

While original federation transactions are replayed, an additional temporary
module prevents a new transaction from overtaking a saved transaction:

```yaml
  - module: migration_auth.federation.LegacyFederationGate
    config:
      journal: /migration-federation/federation-replay.sqlite
      destinations: [the-exact-exported-destinations]
      retry_ms: 30000
```

Mount only the journal directory read-only at `/migration-federation`, readable
by the native Synapse UID. Initialize it before starting Synapse. The module
delays only `/send` for the explicitly configured destinations until all retained
rows are `complete`; missing rows/files or unexpected statuses keep those sends
blocked. Key, state and join requests and other destinations remain available.
The replay daemon sends the exact old transaction IDs/bodies in their original
order and commits a valid HTTP200 acknowledgement before marking them complete.
This narrowly scoped wrapper uses a private Synapse transport interface and
therefore checks the exact 1.162.0 version at startup. An explicit empty
`destinations: []` disables the gate for an isolated startup rehearsal. Remove it
only after replay
is complete and before changing the pinned native version.

References: [password-provider callbacks](https://github.com/element-hq/synapse/blob/v1.162.0/docs/modules/password_auth_provider_callbacks.md),
[module API](https://github.com/element-hq/synapse/blob/v1.162.0/synapse/module_api/__init__.py),
[native registration storage](https://github.com/element-hq/synapse/blob/v1.162.0/synapse/storage/databases/main/registration.py),
[native E2EE storage](https://github.com/element-hq/synapse/blob/v1.162.0/synapse/storage/databases/main/end_to_end_keys.py).
