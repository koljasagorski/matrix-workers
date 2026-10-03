#!/bin/bash
# Root-owned forced-SSH entrypoint. Only an exact main commit SHA is read on stdin.
set -euo pipefail
umask 077
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
test "$#" -eq 0 || exit 64
test "$(id -u)" -eq 0 || exit 77
script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
exec /usr/bin/python3 "$script_directory/deploy-reviewed.py"
