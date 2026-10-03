# Matrix homeserver: m.sgr.ski

[![CI](https://github.com/koljasagorski/matrix-workers/actions/workflows/security.yml/badge.svg)](https://github.com/koljasagorski/matrix-workers/actions/workflows/security.yml)

Production runs on **Debian 13 at `nbg.patchletter.com`**, using Synapse 1.162.0, PostgreSQL 17 and Caddy. The homeserver URL and account identities remain **https://m.sgr.ski**. The stack lives in `/opt/matrix`, separately from the existing Patchletter services. Cloudflare continues to manage DNS.

In Element Desktop, Element X or another Matrix client, keep the existing account and homeserver URL. Existing devices, sessions, public device keys, cross-signing, encrypted recovery backups, settings and `mxc://m.sgr.ski/...` media URLs are preserved. Native Synapse handles new room events, sync, receipts and federation. Registration is closed; native administration endpoints are private to the operator.

The migration retains every original event payload. Five already replaced legacy rooms contain event IDs/signatures that native Synapse cannot accept; their exact history is available through a protected, membership-checked archive. Native sliding sync follows the current native rooms; classic sync also exposes the accessible archived history. The gateway translates old access/refresh tokens, saved filter IDs and sync cursors without resetting devices or encryption keys. See [migration details](MIGRATION.md).

## Deployment and maintenance

GitHub CI checks Node 22/24, dependency vulnerabilities, CodeQL, Python compatibility code and a real disposable Synapse/PostgreSQL import. The `Reviewed VPS deployment` workflow deploys only the current `main` commit after its complete push CI passes. A dedicated forced-command SSH account validates the public commit again on the VPS. Each rollout creates an encrypted offsite backup, gates traffic during the update, checks native health and stream sequences, and restores the previous release if verification fails. PostgreSQL major upgrades require a separate migration.

Dependabot checks npm, GitHub Actions, Python and pinned container images daily. Eligible version updates merge only after required checks succeed; incompatible server versions remain blocked for review. The temporary federation ordering module uses a tested private Synapse API and must be reviewed before changing its pinned server version. Debian security updates run through unattended-upgrades. TLS certificates renew automatically. [Deployment setup and trust boundary](deploy/vps/REVIEWED-DEPLOYMENT.md).

`matrix-backup.timer` saves PostgreSQL, media, private configuration, compatibility data, delivery journals and the complete original migration exports every 15 minutes to the encrypted Kopia offsite repository. A full offsite restore has been tested. The existing hourly GitHub production check verifies health, client versions and signing-key publication; CI also runs daily. Credentials and exports are excluded from Git.

## Repository

- [deploy/vps](deploy/vps): pinned compose stack, systemd services, Caddy configuration, backup and protected deployment coordinator.
- [scripts/migration](scripts/migration): source exports, native importers, authenticated archive, token/password compatibility and durable federation continuation.
- [MIGRATION.md](MIGRATION.md): preservation requirements, verification and operator recovery procedure.
- `src/`, `wrangler.jsonc`, D1 migrations and TypeScript tests: the original Cloudflare Worker implementation, retained as migration provenance and regression coverage. Its production build trigger is retired; these sources do not run the VPS.

For the historical Worker development commands and architecture, see [the archived implementation guide](deploy/vps/LEGACY-WORKER.md). `npm run deploy` and remote D1 commands belong to that retired implementation; production VPS releases use the reviewed workflow.

Local verification:

```sh
npm ci
npm run check
node --test deploy/vps/tests/check-workflow-policy.cjs
python3 -m unittest discover -s deploy/vps/tests -v
```

Python/native integration checks also run in CI against the deployment's exact image digests. Encryption and decryption stay in clients; copying ciphertext and recovery material cannot create missing client-side encryption keys.
