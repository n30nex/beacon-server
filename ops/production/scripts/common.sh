#!/usr/bin/env bash
# shellcheck disable=SC2034
set -Eeuo pipefail
umask 077

BEACON_ROOT="${BEACON_ROOT:-/opt/beacon}"
COMPOSE_FILE="${BEACON_COMPOSE_FILE:-${BEACON_ROOT}/compose.production.yml}"
STATE_DIR="${BEACON_STATE_DIR:-/var/lib/beacon/deploy}"
CURRENT_IMAGES="${BEACON_CURRENT_IMAGES:-${STATE_DIR}/current.env}"
PREVIOUS_IMAGES="${BEACON_PREVIOUS_IMAGES:-${STATE_DIR}/previous.env}"
BACKUP_DIR="${BEACON_BACKUP_DIR:-/var/backups/beacon}"
PROJECT_NAME="${BEACON_PROJECT_NAME:-beacon-production}"
SECRET_ENV_FILE="${BEACON_SECRET_ENV_FILE:-/etc/beacon/beacon.env}"
CONFIG_FILE="${BEACON_CONFIG_FILE:-/etc/beacon/config.yaml}"
POSTGRES_PASSWORD_FILE="${BEACON_POSTGRES_PASSWORD_FILE:-/etc/beacon/postgres-password}"
ACTIVE_MARKER="${STATE_DIR}/enabled"

log() { printf '%s %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }
require_root() { [[ ${EUID} -eq 0 ]] || die "run as root"; }
require_cmd() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"; }

state_value() {
  local key="$1" file="$2" value
  value="$(sed -n "s/^${key}=//p" "$file" | tail -n 1)"
  [[ -n "$value" ]] || die "${key} is missing from ${file}"
  printf '%s' "$value"
}

validate_image_state() {
  local file="$1" key value
  [[ -f "$file" ]] || die "image state not found: $file"
  [[ ! -L "$file" ]] || die "image state must not be a symlink: $file"
  for key in BEACON_API_IMAGE BEACON_WEB_IMAGE; do
    value="$(state_value "$key" "$file")"
    [[ "$value" =~ ^[a-z0-9][a-z0-9./:_-]*@sha256:[0-9a-f]{64}$ ]] || \
      die "${key} must be a lowercase immutable sha256 manifest reference"
    [[ ! "$value" =~ @sha256:0{64}$ ]] || die "${key} uses a placeholder digest"
  done
  [[ "$(grep -Ec '^[A-Z0-9_]+=' "$file")" -eq 2 ]] || die "unexpected keys in $file"
}

compose_with_state() {
  local state="$1"; shift
  validate_image_state "$state"
  export BEACON_SECRET_ENV_FILE="$SECRET_ENV_FILE"
  export BEACON_CONFIG_FILE="$CONFIG_FILE"
  export BEACON_POSTGRES_PASSWORD_FILE="$POSTGRES_PASSWORD_FILE"
  export BEACON_BACKUP_DIR="$BACKUP_DIR"
  docker compose --project-name "$PROJECT_NAME" --env-file "$state" -f "$COMPOSE_FILE" "$@"
}

compose() { compose_with_state "$CURRENT_IMAGES" "$@"; }

install_image_state() {
  local source="$1" destination="$2" temp
  validate_image_state "$source"
  mkdir -p "$STATE_DIR"
  temp="$(mktemp "${STATE_DIR}/.images.XXXXXX")"
  install -m 0600 -o root -g root "$source" "$temp"
  mv -f "$temp" "$destination"
}

require_fresh_cutover_proof() {
  local proof="$1" now modified age
  [[ -f "$proof" && -s "$proof" && ! -L "$proof" ]] || die "missing local-stop proof: $proof"
  [[ "$(stat -c '%u:%a' "$proof")" == "0:600" ]] || die "local-stop proof must be root-owned mode 0600"
  grep -Fxq 'local_api_stopped=true' "$proof" || die "local-stop proof does not confirm the API is stopped"
  grep -Fxq 'local_web_stopped=true' "$proof" || die "local-stop proof does not confirm Vite is stopped"
  now="$(date +%s)"; modified="$(stat -c %Y "$proof")"; age=$((now - modified))
  (( age >= 0 && age <= 21600 )) || die "local-stop proof is older than six hours"
}

wait_for_url() {
  local url="$1" attempts="${2:-60}" delay="${3:-2}" i
  for ((i=1; i<=attempts; i++)); do
    if curl --fail --silent --show-error --max-time 5 "$url" >/dev/null; then return 0; fi
    sleep "$delay"
  done
  return 1
}

database_snapshot() {
  local database="${1:-beacon}"
  compose exec -T postgres psql -X -v ON_ERROR_STOP=1 -U beacon -d "$database" -Atc \
    "SELECT json_build_object(
      'rowCounts', json_build_object(
        'packets', (SELECT count(*) FROM packets),
        'observations', (SELECT count(*) FROM packet_observations),
        'routes', (SELECT count(*) FROM known_routes),
        'nodes', (SELECT count(*) FROM nodes),
        'observers', (SELECT count(*) FROM observers),
        'migrations', (SELECT count(*) FROM schema_migrations)
      ),
      'maxTimestamps', json_build_object(
        'packets', (SELECT max(last_heard_at) FROM packets),
        'observations', (SELECT max(heard_at) FROM packet_observations)
      )
    )::text;"
}
