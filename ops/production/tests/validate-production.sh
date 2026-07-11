#!/usr/bin/env bash
# shellcheck disable=SC2016 # grep assertions intentionally match literal shell variables
set -Eeuo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
PRODUCTION="${ROOT}/ops/production"

for script in "${PRODUCTION}"/scripts/*.sh "${PRODUCTION}"/tests/*.sh; do bash -n "$script"; done
if command -v shellcheck >/dev/null 2>&1; then
  shellcheck -x "${PRODUCTION}"/scripts/*.sh "${PRODUCTION}"/tests/*.sh
fi

grep -Eq '^ARG GO_IMAGE=.*@sha256:[0-9a-f]{64}$' "${ROOT}/.build/Dockerfile"
grep -Eq '^ARG RUNTIME_IMAGE=.*@sha256:[0-9a-f]{64}$' "${ROOT}/.build/Dockerfile"
if grep -Fxq '*.sql' "${ROOT}/.dockerignore"; then
  echo "broad SQL ignore would remove embedded migrations from the image" >&2
  exit 1
fi
test -f "${ROOT}/db/migrations/001_initial_schema.sql"
compose_file="${PRODUCTION}/compose.production.yml"
grep -Fxq 'name: beacon-production' "$compose_file"
grep -Fq 'BEACON_API_IMAGE must be an immutable digest reference' "$compose_file"
grep -Fq 'BEACON_WEB_IMAGE must be an immutable digest reference' "$compose_file"
grep -Fq '    internal: true' "$compose_file"
grep -Fq '    networks: [backend, egress]' "$compose_file"
grep -Fq '  egress: {}' "$compose_file"
python3 - "$compose_file" <<'PY'
import re, sys
text = open(sys.argv[1], encoding="utf-8").read()
def service(name: str) -> str:
    match = re.search(rf"(?ms)^  {name}:\n(.*?)(?=^  [a-z][a-z0-9_-]*:\n|^networks:\n)", text)
    assert match, name
    return match.group(1)
assert "    networks: [backend]\n" in service("postgres")
assert "    networks: [backend]\n" in service("redis")
assert "    networks: [backend, egress]\n" in service("api")
assert "    networks: [edge, backend]\n" in service("web")
PY
grep -Fq '      - "80:8080"' "$compose_file"
grep -Fq '    user: "65532:65532"' "$compose_file"
grep -Fq '    mem_limit: 448m' "$compose_file"
grep -Fq '    mem_limit: 320m' "$compose_file"
grep -Fq '    mem_limit: 64m' "$compose_file"
grep -Fq '        condition: service_healthy' "$compose_file"
grep -Fq 'BEACON_BACKUP_DIR: /var/lib/beacon/backup-metadata' "$compose_file"
grep -Fq 'BEACON_BACKUP_METADATA_DIR:-/var/lib/beacon/backup-metadata' "$compose_file"
if grep -Fq 'BEACON_BACKUP_DIR:-/var/backups/beacon}:/var/backups/beacon:ro' "$compose_file"; then
  echo "API must not receive the PostgreSQL backup directory" >&2
  exit 1
fi
grep -Fq 'compose_with_state "$state" stop --timeout 45 web api' "${PRODUCTION}/scripts/deploy.sh"
grep -Fq "rm -f \"\$ACTIVE_MARKER\"" "${PRODUCTION}/scripts/deploy.sh"
grep -Fq 'was_active=false; inactive_guard=false' "${PRODUCTION}/scripts/deploy.sh"
grep -Fq 'rm -f "$CURRENT_IMAGES" "$PREVIOUS_IMAGES"' "${PRODUCTION}/scripts/deploy.sh"
grep -Fq 'trap cleanup_inactive_activation EXIT' "${PRODUCTION}/scripts/deploy.sh"
grep -Fq 'SELECT pg_export_snapshot();' "${PRODUCTION}/scripts/backup.sh"
grep -Fq 'database_snapshot beacon "$snapshot_id"' "${PRODUCTION}/scripts/backup.sh"
grep -Fq -- '--snapshot="$snapshot_id"' "${PRODUCTION}/scripts/backup.sh"
grep -Fq 'publish_backup_metadata "$manifest"' "${PRODUCTION}/scripts/backup.sh"
grep -Fq 'publish_backup_metadata "$manifest"' "${PRODUCTION}/scripts/restore-verify.sh"
for script in deploy.sh rollback.sh infra-up.sh backup.sh restore-verify.sh; do
  grep -Fq 'acquire_production_lock 30' "${PRODUCTION}/scripts/${script}"
done
rollback_script="${PRODUCTION}/scripts/rollback.sh"
grep -Fq 'trap cleanup EXIT' "$rollback_script"
grep -Fq 'manual rollback requires an active deployment' "$rollback_script"
grep -Fq 'compose_with_state "$original_previous" pull api web' "$rollback_script"
grep -Fq 'install_image_state "$original_current" "$CURRENT_IMAGES"' "$rollback_script"
grep -Fq 'install_image_state "$original_previous" "$PREVIOUS_IMAGES"' "$rollback_script"
grep -Fq 'compose stop --timeout 45 web api' "$rollback_script"
grep -Fq 'rm -f "$ACTIVE_MARKER"' "$rollback_script"
rollback_pull_line="$(grep -n 'compose_with_state "$original_previous" pull api web' "$rollback_script" | cut -d: -f1)"
rollback_swap_line="$(grep -n 'install_image_state "$original_previous" "$CURRENT_IMAGES"' "$rollback_script" | head -n1 | cut -d: -f1)"
(( rollback_pull_line < rollback_swap_line ))
grep -Fq 'try_acquire_production_lock' "${PRODUCTION}/scripts/watchdog.sh"
grep -Fq 'for service in postgres redis api web' "${PRODUCTION}/scripts/watchdog.sh"
grep -Fq 'for service in postgres redis' "${PRODUCTION}/scripts/watchdog.sh"
grep -Fq 'recovery_order+=("api")' "${PRODUCTION}/scripts/watchdog.sh"
grep -Fq 'recovery_order+=("web")' "${PRODUCTION}/scripts/watchdog.sh"
grep -Fq 'compose restart --timeout "$timeout" "$service"' "${PRODUCTION}/scripts/watchdog.sh"
grep -Fq 'compose up -d --no-deps "$service"' "${PRODUCTION}/scripts/watchdog.sh"
grep -Fq 'api-readyz:failed' "${PRODUCTION}/scripts/watchdog.sh"
grep -Fq 'edge-readyz:failed' "${PRODUCTION}/scripts/watchdog.sh"
grep -Fq 'require_cmd timeout' "${PRODUCTION}/scripts/watchdog.sh"
grep -Fq 'compose_with_state_timeout "$CURRENT_IMAGES" 10s exec -T api' "${PRODUCTION}/scripts/watchdog.sh"
grep -Fq 'wget -q -T 5 -O /dev/null' "${PRODUCTION}/scripts/watchdog.sh"
grep -Fq 'write_watchdog_state' "${PRODUCTION}/scripts/watchdog.sh"
grep -Fq 'TimeoutStartSec=8min' "${PRODUCTION}/systemd/beacon-watchdog.service"
grep -Fq 'OnCalendar=*-*-* 03:30:00 America/Toronto' "${PRODUCTION}/systemd/beacon-backup.timer"
grep -Fq 'OnCalendar=*-*-01 04:30:00 America/Toronto' "${PRODUCTION}/systemd/beacon-restore-drill.timer"
grep -Fq 'BEACON_CURRENT_IMAGES=/root/beacon-candidate.env' "${PRODUCTION}/README.md"

test_temp="$(mktemp -d)"
trap 'rm -rf "$test_temp"' EXIT
cat > "${test_temp}/full.manifest.json" <<'JSON'
{
  "archive": "secret.dump",
  "createdAt": "2026-07-11T14:00:00Z",
  "listVerified": true,
  "rowCounts": {"packets": 123},
  "scratchRestoreVerified": true,
  "scratchRestoreVerifiedAt": "2026-07-11T14:05:00Z",
  "sha256": "secret-hash"
}
JSON
python3 "${PRODUCTION}/scripts/sanitize-backup-metadata.py" \
  "${test_temp}/full.manifest.json" > "${test_temp}/latest.manifest.json"
python3 - "${test_temp}/latest.manifest.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as fh:
    payload = json.load(fh)
assert payload == {
    "createdAt": "2026-07-11T14:00:00Z",
    "listVerified": True,
    "scratchRestoreVerified": True,
    "scratchRestoreVerifiedAt": "2026-07-11T14:05:00Z",
}
PY
cat > "${test_temp}/invalid-bool.manifest.json" <<'JSON'
{"createdAt":"2026-07-11T14:00:00Z","listVerified":"true","scratchRestoreVerified":true}
JSON
cat > "${test_temp}/invalid-created.manifest.json" <<'JSON'
{"createdAt":"not-a-time","listVerified":true,"scratchRestoreVerified":true}
JSON
cat > "${test_temp}/invalid-verified-at.manifest.json" <<'JSON'
{"createdAt":"2026-07-11T14:00:00Z","listVerified":true,"scratchRestoreVerified":true,"scratchRestoreVerifiedAt":"yesterday"}
JSON
for malformed in "${test_temp}"/invalid-*.manifest.json; do
  if python3 "${PRODUCTION}/scripts/sanitize-backup-metadata.py" "$malformed" >/dev/null 2>&1; then
    echo "sanitizer accepted malformed manifest: $malformed" >&2
    exit 1
  fi
done
update_line="$(grep -n 'payload\["scratchRestoreVerified"\] = True' "${PRODUCTION}/scripts/restore-verify.sh" | cut -d: -f1)"
publish_line="$(grep -n 'publish_backup_metadata "$manifest"' "${PRODUCTION}/scripts/restore-verify.sh" | cut -d: -f1)"
drop_line="$(grep -n 'compose exec -T postgres dropdb --force' "${PRODUCTION}/scripts/restore-verify.sh" | cut -d: -f1)"
(( drop_line < update_line && update_line < publish_line ))
attempt_write_line="$(grep -n 'write_watchdog_state' "${PRODUCTION}/scripts/watchdog.sh" | tail -n2 | head -n1 | cut -d: -f1)"
recovery_action_line="$(grep -n 'for service in "${recovery_order\[@\]}"' "${PRODUCTION}/scripts/watchdog.sh" | cut -d: -f1)"
(( attempt_write_line < recovery_action_line ))
bash "${PRODUCTION}/tests/ops-scenarios.sh"

if docker compose version >/dev/null 2>&1; then
  temp="${test_temp}/compose"; mkdir -p "$temp"
  digest="$(printf '1%.0s' {1..64})"
  printf 'BEACON_API_IMAGE=ghcr.io/n30nex/beacon-server@sha256:%s\nBEACON_WEB_IMAGE=ghcr.io/n30nex/beacon-web@sha256:%s\n' "$digest" "$digest" > "${temp}/images.env"
  printf 'POSTGRES_DSN=postgres://beacon:test@postgres:5432/beacon?sslmode=disable\n' > "${temp}/beacon.env"
  printf 'regions: []\n' > "${temp}/config.yaml"
  printf 'test-password\n' > "${temp}/postgres-password"
  BEACON_SECRET_ENV_FILE="${temp}/beacon.env" \
  BEACON_CONFIG_FILE="${temp}/config.yaml" \
  BEACON_POSTGRES_PASSWORD_FILE="${temp}/postgres-password" \
  BEACON_BACKUP_DIR="${temp}" \
  BEACON_BACKUP_METADATA_DIR="${temp}" \
    docker compose --env-file "${temp}/images.env" -f "${PRODUCTION}/compose.production.yml" config --quiet
elif [[ "${CI:-}" == true ]]; then
  echo "Docker Compose is required for CI production validation" >&2
  exit 1
fi

echo "production operations bundle validation passed"
