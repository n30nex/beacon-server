#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ops/production/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"
require_root; require_cmd docker; require_cmd sha256sum; require_cmd python3; require_cmd flock
validate_image_state "$CURRENT_IMAGES"
mkdir -p "$BACKUP_DIR"
exec 9>"/run/lock/beacon-db-maintenance.lock"; flock -w 30 9 || die "database maintenance lock is held"

timestamp="$(date -u +'%Y%m%d-%H%M%S')"
created_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
dump="${BACKUP_DIR}/beacon-daily-${timestamp}.dump"
manifest="${dump%.dump}.manifest.json"
temp="${dump}.partial"
trap 'rm -f "$temp"' EXIT

snapshot="$(database_snapshot beacon)"
log "Creating consistent custom-format PostgreSQL backup"
compose exec -T postgres pg_dump -Fc -Z6 --no-owner --no-privileges -U beacon -d beacon > "$temp"
[[ -s "$temp" ]] || die "pg_dump produced an empty archive"
compose exec -T postgres pg_restore --list < "$temp" >/dev/null
sha="$(sha256sum "$temp" | awk '{print $1}')"
bytes="$(stat -c %s "$temp")"
mv -f "$temp" "$dump"

python3 - "$manifest" "$created_at" "$dump" "$sha" "$bytes" "$snapshot" <<'PY'
import json, os, sys
path, created, archive, sha, size, snapshot = sys.argv[1:]
payload = {
    "createdAt": created,
    "archive": os.path.basename(archive),
    "sha256": sha,
    "bytes": int(size),
    "format": "postgres-custom",
    "listVerified": True,
    "scratchRestoreVerified": False,
    **json.loads(snapshot),
}
tmp = path + ".tmp"
with open(tmp, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, sort_keys=True, indent=2)
    fh.write("\n")
os.chmod(tmp, 0o600)
os.replace(tmp, path)
PY
chmod 0600 "$dump" "$manifest"

if [[ "$(date -u +%u)" -eq 7 ]]; then
  weekly="${BACKUP_DIR}/beacon-weekly-${timestamp}.dump"
  ln "$dump" "$weekly"
  cp --preserve=mode,timestamps "$manifest" "${weekly%.dump}.manifest.json"
fi

prune() {
  local pattern="$1" keep="$2" files file
  mapfile -t files < <(find "$BACKUP_DIR" -maxdepth 1 -type f -name "$pattern" -printf '%T@ %p\n' | sort -nr | cut -d' ' -f2-)
  for file in "${files[@]:$keep}"; do rm -f -- "$file" "${file%.dump}.manifest.json"; done
}
prune 'beacon-daily-*.dump' 7
prune 'beacon-weekly-*.dump' 4
log "Backup verified: $dump sha256=$sha bytes=$bytes"
