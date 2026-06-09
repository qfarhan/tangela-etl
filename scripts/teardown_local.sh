#!/usr/bin/env bash
# Stop the mock stack. `--purge` also wipes generated keys and uploaded files.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker compose -f "$ROOT/docker-compose.yml" --profile ui down -v

if [ "${1:-}" = "--purge" ]; then
    echo "==> purging local/"
    rm -rf "$ROOT/local/keys" "$ROOT/local/sftp"
fi

echo "stopped."
