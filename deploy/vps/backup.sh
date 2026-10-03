#!/bin/bash
# Sichert Matrix konsistent und verschlüsselt über das vorhandene Kopia-Ziel.
set -euo pipefail
umask 077
exec 9>/run/matrix-backup.lock
flock -n 9 || exit 0
destination=/var/backups/matrix
install -d -m 700 "$destination"
snapshot_directory=$(mktemp -d "$destination/.sqlite-snapshot.XXXXXX")
trap 'rm -rf -- "$snapshot_directory"' EXIT
install -d -m 700 "$snapshot_directory/compat/federation"
# The delivery journal can change while a backup runs. SQLite's backup API
# captures committed state without copying an in-progress rollback journal.
python3 - "$snapshot_directory/compat/federation/federation-replay.sqlite" <<'PY'
import os, sqlite3, stat, sys
source_path = '/opt/matrix/compat/federation/federation-replay.sqlite'
permissions = os.stat(source_path)
source = sqlite3.connect('file:' + source_path + '?mode=ro', uri=True)
target = sqlite3.connect(sys.argv[1])
with target:
    source.backup(target)
assert target.execute('PRAGMA quick_check').fetchone()[0] == 'ok'
target.close()
source.close()
os.chown(sys.argv[1], permissions.st_uid, permissions.st_gid)
os.chmod(sys.argv[1], stat.S_IMODE(permissions.st_mode))
PY
docker exec matrix-postgres pg_dump -U matrix -d synapse -Fc -Z0 > "$destination/synapse.dump.new"
docker exec -i matrix-postgres pg_restore --list < "$destination/synapse.dump.new" > /dev/null
mv "$destination/synapse.dump.new" "$destination/synapse.dump"
tar -C /opt/matrix -cf "$destination/config-and-media.tar.new" \
  --exclude='synapse/data/homeserver.pid' --exclude='synapse/data/homeserver.log*' \
  --exclude='compat/federation/federation-replay.sqlite*' \
  synapse compat modules migration docker-compose.yml postgres.env
tar -C "$snapshot_directory" -rf "$destination/config-and-media.tar.new" compat/federation/federation-replay.sqlite
mv "$destination/config-and-media.tar.new" "$destination/config-and-media.tar"
backup_config=/etc/patchletter-backup-mailbox.env
export KOPIA_PASSWORD
KOPIA_PASSWORD=$(sed -n 's/^KOPIA_PASSWORD=//p' "$backup_config" | head -1 | sed 's/^"//;s/"$//')
test -n "$KOPIA_PASSWORD"
export KOPIA_CONFIG_PATH=/root/.config/kopia/repository.config
export KOPIA_CHECK_FOR_UPDATES=false
rm -rf -- "$snapshot_directory"
kopia snapshot create "$destination" --log-level=warning
date -u +%FT%TZ > "$destination/last-success"
