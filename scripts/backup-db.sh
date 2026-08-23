#!/bin/sh
set -eu

# Occa database backup helper — run before a binary or schema upgrade.
#
# Usage: backup-db.sh [output-dir]
#
# Resolves the database path from the occa config (OCCA_CONFIG or the default
# ~/.occa/config.yaml) or the OCCA_DB_PATH override, then writes a
# SQLite-consistent backup under the output directory. Safe to run while occa
# is live because the backup uses a consistent snapshot.
#
# Environment:
#   OCCA_CONFIG  config file path (default ~/.occa/config.yaml)
#   OCCA_BIN     occa binary path (default: occa on PATH, else ./occa)

config="${OCCA_CONFIG:-}"
out_dir="${1:-$HOME/.occa/backups}"
self="${OCCA_BIN:-}"

if [ -z "$self" ]; then
    if command -v occa >/dev/null 2>&1; then
        self=occa
    elif [ -x ./occa ]; then
        self=./occa
    else
        echo "backup-db: occa binary not found (set OCCA_BIN)" >&2
        exit 1
    fi
fi

if ! mkdir -p "$out_dir" 2>/dev/null; then
    echo "backup-db: could not create backup directory" >&2
    exit 1
fi
stamp="$(date +%Y%m%d-%H%M%S)-$$"
output="$out_dir/occa-$stamp.db"

set --
[ -z "$config" ] || set -- --config "$config"

"$self" db backup "$@" --output "$output"
echo "backup-db: backup complete"