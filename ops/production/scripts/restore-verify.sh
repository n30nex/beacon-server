#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ops/production/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"
usage() { echo "usage: $0 DUMP [MANIFEST] [--update-manifest]" >&2; exit 2; }
[[ $# -ge 1 ]] || usage
dump="$1"; shift; manifest=""; update=false
if [[ $# -gt 0 && "$1" != --update-manifest ]]; then manifest="$1"; shift; fi
if [[ $# -gt 0 && "$1" == --update-manifest ]]; then update=true; shift; fi
[[ $# -eq 0 ]] || usage
require_root; require_cmd docker; require_cmd sha256sum; require_cmd python3; require_cmd flock
[[ -f "$dump" && -s "$dump" && ! -L "$dump" ]] || die "invalid dump: $dump"
[[ -z "$manifest" || -f "$manifest" ]] || die "manifest not found: $manifest"

min_free="${BEACON_RESTORE_MIN_FREE_BYTES:-12884901888}"
free_bytes="$(df --output=avail -B1 /var/lib/docker | tail -n 1 | tr -d ' ')"
(( free_bytes >= min_free )) || die "scratch restore requires at least ${min_free} free bytes"
acquire_production_lock 30
validate_image_state "$CURRENT_IMAGES"
exec 9>"/run/lock/beacon-db-maintenance.lock"
flock -w 30 9 || die "database maintenance lock is held"
compose exec -T postgres pg_restore --list < "$dump" >/dev/null
sha="$(sha256sum "$dump" | awk '{print $1}')"
if [[ -n "$manifest" ]]; then
  expected_sha="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["sha256"])' "$manifest")"
  [[ "$sha" == "$expected_sha" ]] || die "archive hash does not match manifest"
fi

scratch="beacon_verify_$(date -u +%Y%m%d%H%M%S)_$$"
scratch_exists=false
# shellcheck disable=SC2329 # invoked by the EXIT trap on error paths
cleanup() {
  if [[ "$scratch_exists" == true ]]; then
    compose exec -T postgres dropdb --if-exists --force -U beacon "$scratch" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
scratch_exists=true
compose exec -T postgres createdb -U beacon "$scratch"
compose exec -T postgres pg_restore --exit-on-error --no-owner --no-privileges -U beacon -d "$scratch" < "$dump"
compose exec -T postgres psql -X -v ON_ERROR_STOP=1 -U beacon -d "$scratch" -c 'ANALYZE' >/dev/null
extensions="$(compose exec -T postgres psql -X -v ON_ERROR_STOP=1 -U beacon -d "$scratch" -Atc "SELECT string_agg(extname, ',' ORDER BY extname) FROM pg_extension;")"
[[ ",$extensions," == *,pg_stat_statements,* ]] || die "scratch restore is missing pg_stat_statements"
restored="$(database_snapshot "$scratch")"

if [[ -n "$manifest" ]]; then
  python3 - "$manifest" "$restored" <<'PY'
import json, sys
expected = json.load(open(sys.argv[1], encoding="utf-8"))["rowCounts"]
actual = json.loads(sys.argv[2])["rowCounts"]
if expected != actual:
    raise SystemExit(f"row-count mismatch: expected={expected} actual={actual}")
PY
fi

log "Dropping the verified scratch database before recording success"
compose exec -T postgres dropdb --force -U beacon "$scratch"
scratch_exists=false
trap - EXIT

if [[ "$update" == true && -n "$manifest" ]]; then
  python3 - "$manifest" <<'PY'
import json, os, sys
path = sys.argv[1]
with open(path, encoding="utf-8") as fh: payload = json.load(fh)
payload["scratchRestoreVerified"] = True
payload["scratchRestoreVerifiedAt"] = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat().replace("+00:00", "Z")
tmp = path + ".tmp"
with open(tmp, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, sort_keys=True, indent=2); fh.write("\n")
os.chmod(tmp, 0o600); os.replace(tmp, path)
PY
fi
if [[ -n "$manifest" ]]; then
  publish_backup_metadata "$manifest"
fi
log "Scratch restore verified: sha256=$sha counts=$restored extensions=$extensions"
