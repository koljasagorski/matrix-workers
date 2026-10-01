# Deployment and operations

## Current installation

| Setting | Value |
| --- | --- |
| Repository | `koljasagorski/matrix-workers` |
| Production branch | `main` |
| Worker | `matrix-workers` |
| Domain / Matrix server name | `m.sgr.ski` |
| D1 | `matrix-workers-db` |
| R2 | `matrix-workers-media` |
| KV | Six namespaces prefixed `matrix-workers-` |
| Workflows | `matrix-workers-room-join`, `matrix-workers-push-notification` |
| Registration | Closed by default; controlled through `/admin` |

All eight Durable Objects use SQLite-backed namespaces.

The Worker, resources and GitHub build connection were provisioned through Cloudflare MCP. `wrangler.jsonc` is the source of truth for bindings, domain, compatibility date and non-secret settings. Public `workers.dev` and version preview URLs are disabled.

## Automatic deployments

Cloudflare Workers Builds is connected to GitHub with:

- Repository: `koljasagorski/matrix-workers`
- Branch: `main` only
- Root directory: `/`
- Build command: `npm run check`
- Deploy command: `npm run deploy`
- Node version: `22.22.3`

The native GitHub integration triggers Cloudflare builds on pushes to `main`, including documentation changes. Events may take a few minutes to appear after initial setup. Inspect failures under **Workers & Pages → matrix-workers → Builds**. A failed check or database migration prevents deployment. GitHub Actions independently runs checks and CodeQL; Cloudflare runs its own checks before deploying. No deploy hook or Cloudflare secret is needed in GitHub.

To redeploy manually with authenticated Cloudflare CLI access:

```bash
npm ci
npm run check
npm run deploy
npm run smoke -- https://m.sgr.ski
```

The deployment runs pending database migrations first. Design future migrations to remain compatible with the currently deployed Worker during rollout. Worker rollbacks do not roll back D1 data.

## Create the first administrator

Public registration remains closed during bootstrap. Authenticate the CLI to the account owning this installation, then run:

```bash
npm run admin:create -- admin --remote
```

The command inserts a new administrator, hashes a generated password using the server's PBKDF2 format, and saves credentials to `.local/admin-remote.json` with mode `0600`. It does not print the password or overwrite an existing account. Store the password in your password manager and remove the local credentials file when no longer needed.

Sign in at `https://m.sgr.ski/admin`, then create ordinary accounts there. To create a local test admin, use `--local` instead. The dashboard registration toggle controls both public user and guest registration.

## Database migrations

Use `wrangler d1 migrations apply`, not a loop that reruns SQL files:

```bash
npm run db:migrate:local
npm run db:migrate
```

D1 tracks applied filenames in `d1_migrations`. The original `schema.sql` is now `001_initial_schema.sql` so a fresh database initializes before later migrations. Migration 016 rebuilds event FTS and keeps it current after message updates/redaction. Existing source data is retained.

**For an older installation with manually applied SQL:** export/back up the database and reconcile the already-applied filenames before adopting the migration runner. Do not blindly replay the full chain: historical migrations include `ALTER TABLE` and table rebuilds. The installation documented here uses a fresh database and the migration ledger from its first deployment.

Before major database changes:

```bash
npx wrangler d1 export DB --remote --output .local/matrix-backup.sql
```

Create `.local` first if needed. Treat exports as private: they include account and message data. Recovery options are described in [D1 Time Travel](https://developers.cloudflare.com/d1/reference/time-travel/).

## Deploy a separate fork

The checked-in account and resource IDs belong to this deployment. Provision a separate D1 database, six KV namespaces and an R2 bucket, then replace their IDs, `account_id`, Worker/workflow names, `SERVER_NAME` and custom domain in `wrangler.jsonc`. Keep binding names unchanged.

```bash
npx wrangler d1 create your-matrix-db
npx wrangler kv namespace create SESSIONS
npx wrangler kv namespace create DEVICE_KEYS
npx wrangler kv namespace create CACHE
npx wrangler kv namespace create CROSS_SIGNING_KEYS
npx wrangler kv namespace create ACCOUNT_DATA
npx wrangler kv namespace create ONE_TIME_KEYS
npx wrangler r2 bucket create your-matrix-media
```

After updating the config, run `npm run types`, `npm run check` and `npm run deploy`. Connect the new repository through [Workers Builds](https://developers.cloudflare.com/workers/ci-cd/builds/). The build credential needs permission for Worker deployments, D1 migrations and the configured bindings/custom domain.

Set the Matrix server name before creating users or federated rooms; it is embedded in their IDs. Changing a hostname later is not an account migration.

## Optional services

| Service | Additional configuration |
| --- | --- |
| LiveKit / MatrixRTC | `LIVEKIT_URL`, `LIVEKIT_API_KEY`, secret `LIVEKIT_API_SECRET`; VPC `LIVEKIT_API` for room-management calls |
| Cloudflare TURN | `TURN_KEY_ID` and secret `TURN_API_TOKEN` |
| Cloudflare Calls | Secrets `CALLS_APP_ID`, `CALLS_APP_SECRET` |
| Email verification | Compatible `EMAIL` binding, authorized sender and `EMAIL_FROM` |
| Direct Apple push | Secrets `APNS_KEY_ID`, `APNS_TEAM_ID`, `APNS_PRIVATE_KEY`, `APNS_ENVIRONMENT` |
| External OIDC | Provider configuration and secret `OIDC_ENCRYPTION_KEY` |
| Browser / Analytics / AI | Optional `BROWSER`, `ANALYTICS`, `AI` bindings |

These services are not configured by the base deployment. Validate the relevant integration before enabling it; the repository contains prototype implementations. Add secrets with `npx wrangler secret put NAME` and never commit credentials. Rerun `npm run types` after binding changes.

## Verification and troubleshooting

```bash
npm run smoke -- https://m.sgr.ski
npx wrangler tail matrix-workers
```

Health is a liveness check. The smoke test also exercises registration configuration and federation signing-key generation. Local integration checks during this refresh additionally exercised password login, admin settings, room creation, messaging, sync and logout/refresh rejection.

Use `https://m.sgr.ski` as the homeserver in clients, and `https://m.sgr.ski/admin` for administration. The admin page is public HTML; its APIs require an authenticated administrator. There is no bundled Element frontend.

For federation diagnostics, use the [Matrix Federation Tester](https://federationtester.matrix.org/#m.sgr.ski). Passing its connectivity tests does not establish full Matrix protocol conformance.
