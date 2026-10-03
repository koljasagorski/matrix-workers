#!/bin/bash
# Überträgt das erneuerte Matrix-Zertifikat und lädt Caddy ohne Neustart neu.
set -euo pipefail
test "${RENEWED_LINEAGE:-}" = /etc/letsencrypt/live/matrix-m.sgr.ski || exit 0
destination=/opt/patchletter/caddy/data/matrix
install -d -m 700 "$destination"
install -m 644 "$RENEWED_LINEAGE/fullchain.pem" "$destination/fullchain.pem"
install -m 600 "$RENEWED_LINEAGE/privkey.pem" "$destination/privkey.pem"
docker exec patchletter-caddy-1 caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
