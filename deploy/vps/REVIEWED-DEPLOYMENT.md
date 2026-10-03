The VPS deployment workflow accepts only the current public `main` commit after
the full push CI succeeds, including `migration-check`. Manual runs have the
same checks. It never checks out PR code in the job holding the SSH secret.

Install `deploy-reviewed.sh` (root:root, 0755) and `deploy-reviewed.py`
(root:root, 0644) beside each other under `/opt/matrix/bin`. These trusted
coordinators require separate administrator review when changed; application
deployments do not replace them. The existing root-owned backup command must
be `/opt/matrix/bin/backup.sh` and must finish a new verified encrypted backup
before any live service action. Rollback snapshots under
`/var/lib/matrix-deploy/rollback-*` are private to root and retained for recovery.

The gateway configuration must set
`maintenance_file=/opt/matrix/compat/maintenance.flag` and expose its actual
listener on `127.0.0.1` as well as the private Caddy network address. The
coordinator reads the protected gateway port and uses
`/health?maintenance_probe=true` for readiness while ordinary traffic gets 503.
Only a real loopback socket can bypass maintenance for health; forwarded
headers cannot. The native listener remains `127.0.0.1:18008`.

Create a dedicated `matrix-deploy` SSH user/key. Its authorized key must use
`restrict,command="sudo -n /opt/matrix/bin/deploy-reviewed.sh"` and its only
sudo permission must be that exact command without arguments. Do not grant
a shell command, Docker group membership or access to private Matrix files.
The forced command ignores client commands and accepts one lowercase hexadecimal
SHA of exactly 40 characters on stdin; the root coordinator verifies it against the public origin.

Set GitHub environment `matrix-vps` to allow deployments only from `main`.
Set secret `MATRIX_VPS_SSH_KEY` to this dedicated private key and variable
`MATRIX_VPS_KNOWN_HOSTS` to the separately verified host key entry for
`nbg.patchletter.com`. The workflow keeps strict host checking enabled.

The coordinator serializes deployments, fetches an isolated public checkout,
prepares exact pinned Python dependencies and pulls only validated official
digest images. It preserves PostgreSQL major version, roles, mounts, volumes,
ports and network identities. It changes only compose, the auth/gateway module,
the archive module and the Python environment. Private config, signing keys,
token bridge data/secrets, media and archive SQLite/manifest stay in place.
Importers, the frozen Worker and migration/replay journals are not executed.

After the online backup, public traffic is gated and native writes are drained
before a final database dump and complete protected filesystem snapshot.
Healthy pinned Synapse, matching native schema, gateway readiness and all 16
known multiwriter sequence/table/position checks must pass before reopening.
Failure restores old code; if schema changed or cannot be checked, it restores
the pre-deployment database dump. Recovery also requires old server health and
stream checks. A failed recovery keeps maintenance closed for administrator
repair. Deploying a PostgreSQL major upgrade requires a separate migration.

Regression commands use fake operations/API responses and never production:

```
python3 -m unittest discover -s deploy/vps/tests -v
node --test deploy/vps/tests/check-workflow-policy.cjs
```

`migration-check` also boots disposable pinned Synapse/PostgreSQL containers,
checks real schema startup and import, then proves the deployment stream guard
rejects a retained position ahead of its sequence.

The trigger and privilege boundary follow GitHub's official
[workflow event documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflow_run)
and [secure use guidance](https://docs.github.com/en/actions/reference/security/secure-use).
