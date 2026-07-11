#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ops/production/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"
require_root
dump="$(find "$BACKUP_DIR" -maxdepth 1 -type f -name 'beacon-daily-*.dump' -printf '%T@ %p\n' | sort -nr | head -n 1 | cut -d' ' -f2-)"
[[ -n "$dump" ]] || die "no daily backup is available for the restore drill"
exec "${SCRIPT_DIR}/restore-verify.sh" "$dump" "${dump%.dump}.manifest.json" --update-manifest
