# Migration to the Debian 13 VPS

Production cutover completed on **2026-10-03 at 19:21 UTC**. Public client and
federation checks pass on both VPS addresses. All **735 original event payloads**,
**44 encrypted recovery sessions**, **11 existing devices**, **126 receipts** and
**15 original media/thumbnail objects** were reconciled. Native Synapse holds
14 rooms; five already replaced legacy rooms remain accessible through the
protected archive. The original signing identity and homeserver URL are unchanged.
The final frozen source and complete native backup were restored from encrypted
offsite storage into an isolated database before source retirement.

The old Cloudflare build trigger and custom Worker domain are detached. Keep the
new DNS-only A/AAAA records. Pending delivery to the offline `m.h4.ddnss.org` peer
continues from the durable VPS journal with the original transaction IDs;
successful peer acknowledgements and per-event errors remain recorded. A new
Synapse version requires review while the temporary transport gate is installed.

The target is `nbg.patchletter.com` (`89.58.54.21`), with Synapse **1.162.0**, PostgreSQL **17**, and the unchanged Matrix identity **`m.sgr.ski`**. The Synapse image is pinned to `sha256:6b84a7bbac36f080b2d2e51e0289cf1b08b349598ea44a558df38d558f2c2311`. This host also runs Patchletter and other services: use the separate `/opt/matrix` stack, `matrix-internal` network, `matrix-postgres` database, volumes and loopback ports. Caddy remains the shared TLS entry point. Do not modify or remove another service's database, bucket, containers, DNS or credentials.

## Preserve these data sources

| Source | Matrix resource | Required handling |
| --- | --- | --- |
| Worker | `matrix-workers`, account `9a562df9759b0d525872d399986f10ef` | Retain source/deployed versions and configuration for rollback. |
| D1 | `matrix-workers-db`, `dd9c4c21-9f5d-48cf-a679-02ef5bb269cd` | Export all application tables, including auth/device state, ciphertext, snapshots, archived events, signing keys, memberships and upgrade jobs. FTS indexes/shadow tables are rebuildable. |
| R2 | `matrix-workers-media` | Export **every** key, including thumbnails and objects absent from D1 metadata; preserve bytes, original key, MIME/custom metadata, size, ETag and SHA256. Keep existing `mxc://m.sgr.ski/...` URLs usable. |
| KV | `SESSIONS`, `DEVICE_KEYS`, `CACHE`, `CROSS_SIGNING_KEYS`, `ACCOUNT_DATA`, `ONE_TIME_KEYS` | Export every key's raw bytes, expiration and metadata. Refresh-token mappings and saved filter IDs need compatibility handling. KV can lag newer DO values. |
| UserKeys DO | `60faf76584f14329a2084499f8215c3c` | Authoritative E2EE/secret-storage account data, device keys, cross-signing keys and signatures; enumerate raw storage, including keys absent from tracking lists. |
| Federation DO | `34cf2b1f8ca3463fb166dcac2a758d11` | Pending PDUs, encrypted to-device EDUs, transaction IDs, retry state, rejection diagnostics and alarms. Enumerate **all existing object IDs**, including peers no longer in room membership. |
| Room DO | `b8ea3b6d015949c4b1a6a0a654dccfbc` | Persistent receipts may predate D1 mirroring; preserve them. Typing/socket state expires naturally. |
| Sync DO | `6ee2bf7566d64356b7196dae3268e2de` | Back up cursors and markers for rollback. Legacy sync positions must be translated/reset, not interpreted as native Synapse stream positions. |
| Admin DO | `24264018bf734044a645faffcf8767e8` | Preserve effective registration/configuration; statistics are rebuildable. |
| Call / Push / RateLimit DO | `baa0c94407ac4e42b6863e1268a013e0` / `ef2a5975ffa34ab7867b5c69d4a94a89` / `9587e4c93c7f417f8c029bc67b5a4779` | Enumerate and export any stored state. Active sockets/calls, push batches and counters also contain nonportable memory state; drain/reconnect before cutover. |
| Workflows | `matrix-workers-room-join`, `matrix-workers-push-notification` | Inventory instances and reconcile queued/running/waiting work. A source archive does not prove messages or notifications reached their destination. |
| Signing/secrets | D1 `server_keys` and Worker secret names | Preserve current/private and old verification keys. Recover configured secrets from operator stores: Cloudflare secret listing gives names, not secret values. Do not regenerate server identity or expose credentials in logs/commits. |

Cloudflare currently contains unrelated Worker namespaces. Only the eight bindings above belong to this migration. DNS remains managed in Cloudflare.

## Export and rehearse

All exports belong in ignored `.local/vps-migration`, directory mode `0700` and files `0600`. Store an encrypted offsite copy and verify restoration before retiring the source. The export scripts never print key/token/password/ciphertext values. They use the existing local Wrangler OAuth credential or `CLOUDFLARE_API_TOKEN` for read-only Cloudflare API calls. Refresh local OAuth with `npx wrangler whoami` if the API reports 401.

```console
node scripts/migration/export-cloudflare.mjs --inventory-only
node scripts/migration/export-cloudflare.mjs
```

The first command writes `approved-do-objects.json` and `do-inventory.json` by fully paginating Cloudflare's existing-object listing. The public DO export is disabled until the operator configures `MIGRATION_EXPORT_OBJECTS` from that approval file and deploys the tested backup routes. It requires an administrator access token **and the current account password on every request**; a UIA session ID alone is insufficient. It accepts only eight exact bindings and approved IDs, never arbitrary URLs/SQL/object creation. Remove this temporary configuration once the rollback window ends.

The Cloudflare script additionally writes six raw KV snapshots, `refresh-tokens.json`, `client-filters.json`, R2 byte files/inventory, workflow/resource inventory and a checksum manifest. D1 still requires a separate export: unfiltered `wrangler d1 export` can fail for FTS5 virtual tables. Export an explicit list of ordinary application tables, restore into `source.sqlite`, run SQLite integrity checks and compare table counts to the source. Keep D1 Time Travel available during the rollback window.

```console
node scripts/migration/export-durable-objects.mjs --credentials=.local/kolja-remote.json
node scripts/verify-migration.mjs --mode=backup
```

The DO script logs in with a temporary device and removes that device in `finally`; existing user devices are untouched. It writes a lossless tagged raw `do-state.json`, the authoritative `user-keys.json` overlay and a separate integrity manifest. Tags encode every value/node to avoid collisions with application keys; undefined fields, typed arrays/backing buffers, maps, cycles and BigInt survive the archive. The normal UserKeys overlay retains the JSON representation expected by Matrix. The decoder is `scripts/migration/stored-values.mjs`.

Rehearse the user/room import into an isolated, stopped Synapse database. Do not import into an occupied production database. Import original passwords through the compatibility authentication provider; translate hashed legacy access/refresh tokens and preserve device identity; import saved user-specific filter IDs. Verify device-key query, cross-signing signatures, encrypted secret storage and megolm backup/recovery through actual authenticated APIs. Compare original ciphertext and private room history as well as row counts.

### Legacy event history is an explicit cutover gate

Some old events have unsigned/random IDs which disagree with the event ID computed by Synapse's room-version rules. Native Synapse rejects that mismatch as database corruption. Keep these exact original rows in a protected archive and provide an authenticated, membership-checked history/compatibility path. Do not rewrite their IDs or signatures, silently discard ciphertext or claim that copying an inaccessible SQL file preserves usable chat history. Invalid historical events in otherwise valid rooms need the same treatment. The final room manifest and operational proof must account for them before all-data cutover/retirement.

## Take the final consistent snapshot

1. Finish the rehearsal and retain a verified rollback copy. Pre-create a backup session before maintenance using `--prepare-session`; protect `backup-session.json`. Preserve the exact snapshot including this temporary device for audit; test its migrated session, then delete the temporary device through native UIA before directing public traffic.
2. Drain/reconcile workflow jobs and outbound federation. Retain exact transaction IDs/payloads if pending delivery must resume. A backup alone does not finish delivery; provide a tested VPS replay path or keep the source until delivery completes.
3. Set the optional `MIGRATION_FREEZE=1` only during the agreed cutover. The tested middleware returns 503/`M_RESOURCE_LIMIT_EXCEEDED` with `Retry-After: 30` for public traffic except `GET /health` and authenticated backup POST. DO fetches and existing socket writes are blocked; alarms hold their queues and reschedule. Already-running Worker/Workflow code must also finish or be paused; wait for those operations before declaring the snapshot consistent.
4. Re-enumerate all objects and repeat **D1 + KV + R2 + DO** export after quiescence. The approved DO inventory must match the final enumeration; configure updated approval before the final freeze if necessary. Run the DO export with `--session=.local/vps-migration/backup-session.json`; leave temporary-device cleanup to the operator after the final snapshot.
5. Import the final snapshot into stopped Synapse, preserve the original signing keys, start the isolated stack and verify all data/compatibility routes. Record the import/overlay checksums and operational checks in `cutover-proof.json`.

`scripts/verify-migration.mjs` defaults to backup integrity only and explicitly does **not** assert live consistency. Modes `cutover` and `retire` additionally require native import manifests tied to the same source/overlay hashes and recorded operator verification. The proof file contains `source_sha256`, `server_name: "m.sgr.ski"` and true values for `consistent_frozen_snapshot`, `server_signing_keys_verified`, `e2ee_recovery_verified`, `sessions_verified`, `media_verified`, `federation_queue_reconciled`, `workflows_reconciled`, `rollback_backup_restored` and `all_expected_data_reconciled`. If legacy rows exist, it also requires `legacy_history_accessible`. These attestations represent completed external checks; they are not automatically established by a file copy.

### Resume pending federation without changing original transactions

Let the corrected source discovery/alarm delivery drain reachable destinations during rehearsal. Do not reset transaction metadata, delete pending PDUs or re-sign their event payloads. After the final freeze/export, prepare the exact remaining queue:

```console
node scripts/migration/prepare-federation-replay.mjs
python scripts/migration/replay-federation.py --export-signing-key .local/vps-migration/source.sqlite --signing-key-output .local/vps-migration/original-signing-key.json
```

The plan binds the final source/DO checksums and includes every queued PDU/EDU exactly once. Saved in-flight IDs, timestamps, batch order and JSON payloads take precedence. Later batches retain the source's deterministic transaction-ID algorithm and limits. The replay daemon signs only the HTTP request with the preserved server key; it never changes event signatures, IDs or ciphertext.

Install the pinned `requirements-federation-replay.txt` in the isolated VPS virtual environment. Put plan, signing key and proof in a private directory readable only by the replay service. The key file is `0600`; Synapse never receives the private key file or user-source database through the gate mount. Use a separate shared journal directory `/opt/matrix/compat/federation`, owned by `matrix:991` with mode `0750`; the journal is `matrix:991` mode `0640`. Its read-only Synapse mount is `/migration-federation`. SQLite uses DELETE journaling and FULL synchronous commits; the replay owner needs directory write permission for temporary rollback journals. Keep all these files in encrypted offsite backups.

Example private replay configuration `/opt/matrix/compat/federation-replay.json`:

```json
{
  "plan": "/opt/matrix/compat/federation-private/federation-replay-plan.json",
  "signing_key": "/opt/matrix/compat/federation-private/original-signing-key.json",
  "proof": "/opt/matrix/compat/federation-private/cutover-proof.json",
  "journal": "/opt/matrix/compat/federation/federation-replay.sqlite",
  "endpoints": { "m.h4.ddnss.org": "https://m.h4.ddnss.org:8448" }
}
```

The shown offline-peer endpoint is its validated original fallback after no actual SRV records were found. Revalidate before using this override. Automatic discovery supports bounded HTTPS well-known redirects and explicit delegated ports; a destination requiring SRV resolution needs an operator-validated endpoint override. The transport rejects private/literal/resolved addresses, credentials, unsafe ports and redirecting transaction sends. TLS certificate verification remains enabled.

Run `--config <file> --import-only` after the proof records `consistent_frozen_snapshot: true`, before starting native federation. This initializes an immutable, resumable journal without sending anything. Set the journal's final group/mode and configure `migration_auth.federation.LegacyFederationGate` for every destination in the final plan. The gate is pinned and tested against Synapse **1.162.0** because it wraps a private transport API. It delays only new `/_matrix/federation/v1/send/` transactions for destinations whose original queue remains pending; key/state/join calls and other peers continue. Missing/corrupt journals fail closed for watched send destinations. Verify the gate on the actual native image, then record `native_federation_replay_gate_verified: true`. Install the script in `/opt/matrix/compat/replay` and start `matrix-federation-replay.service` paused: missing/false `federation_replay_enabled` in the proof prevents every network request. Verify service restart/journal recovery and record its operational proof before DNS cutover. Only after directing traffic to the VPS should the operator atomically set `federation_replay_enabled: true`; the daemon rereads this flag every loop and before each original send, with malformed/missing proof failing closed.

The daemon accepts only HTTP 200 with a valid transaction acknowledgement, stores the peer response durably, and then releases the next transaction for that destination. Per-PDU rejection errors are retained as diagnostics and do not retry an already completed transaction indefinitely. Network failures persist an exponential retry schedule capped at one hour; a restart or ambiguous network failure repeats the same transaction ID and exact body. An offline peer therefore cannot block unrelated peers or the overall migration. A peer acknowledgement does not imply that every individual PDU was accepted; inspect retained rejection diagnostics. Record `federation_replay_service_verified: true` only after tested service restart/resume and backup restoration. Cutover verification reconstructs and compares the plan against the final raw snapshot and requires both gate/service proofs whenever anything remains pending.

Back up an active replay journal with SQLite's online backup API or stop the replay service while taking a file copy; copying a database during a write is insufficient. Retain the original plan, signing identity and pending journal after the Cloudflare resources are retired. Monitor pending counts and per-PDU rejection counts without logging ciphertext or credentials. Until this verified continuation exists, pending federation is a cutover blocker.

## Route and retire

Root/operator handles Caddy, certificate validation and DNS. Keep `m.sgr.ski` server name and Matrix well-known discovery stable; verify key-server signatures, federation, client versions, login/refresh, sync/reconnection, encrypted history, media downloads and receipt progress against the VPS before directing traffic. Avoid two active homeservers accepting writes for the same identity.

The original native Cloudflare build trigger is `ab74f3a1-456f-4f7b-a043-8a164ee61d38`, Worker external ID `10975afcf73b41ee96bda4dea9e33b3a`. It deploys `main` using `npm run check` / `npm run deploy`. Disable/detach **this trigger only** before any final source retirement so later GitHub pushes cannot resurrect the Worker. Its displayed build token may be shared: do not revoke it without checking other triggers. Keep GitHub CI/security/dependency updates, hourly health checks, the repository and Cloudflare DNS active; update deployment automation to target the new VPS with verified releases.

Retirement also requires proven post-cutover operation, restored encrypted offsite backups and the completed rollback retention window. Record `vps_post_cutover_checks_passed`, `offsite_backup_restore_passed`, `cloudflare_build_trigger_disabled`, `cloudflare_routes_detached` and `rollback_retention_completed` in the proof and run `--mode=retire`. Only then may the operator remove the Matrix custom Worker route, the Matrix Worker, its two workflows and the exclusive Matrix D1/KV/R2/DO resources. Deleting a Worker/binding can make its DO data inaccessible; do not assume that those deletions are reversible. Retain required DNS/TLS records and unrelated account resources. No migration check script performs these deletions.
