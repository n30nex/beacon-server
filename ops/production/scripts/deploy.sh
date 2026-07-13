#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ops/production/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

usage() { echo "usage: $0 --images FILE --activate --cutover-proof FILE" >&2; exit 2; }
images=""; proof=""; activate=false
while (($#)); do
  case "$1" in
    --images) [[ $# -ge 2 ]] || usage; images="$2"; shift 2 ;;
    --cutover-proof) [[ $# -ge 2 ]] || usage; proof="$2"; shift 2 ;;
    --activate) activate=true; shift ;;
    *) usage ;;
  esac
done
[[ -n "$images" && -n "$proof" && "$activate" == true ]] || usage

require_root; require_cmd docker; require_cmd curl; require_cmd flock
validate_image_state "$images"
require_fresh_cutover_proof "$proof"
[[ -f "$SECRET_ENV_FILE" && -f "$CONFIG_FILE" && -f "$POSTGRES_PASSWORD_FILE" ]] || \
  die "production secrets/configuration are incomplete under /etc/beacon"
chown root:root "$SECRET_ENV_FILE" "$POSTGRES_PASSWORD_FILE"
chmod 0600 "$SECRET_ENV_FILE" "$POSTGRES_PASSWORD_FILE"
# The API runs as uid/gid 65532. Its bind-mounted channel-key configuration is
# group-readable by that identity without becoming world-readable.
chown root:65532 "$CONFIG_FILE"
chmod 0640 "$CONFIG_FILE"
mkdir -p "$STATE_DIR"
install -d -m 0700 -o root -g root "$BACKUP_DIR"
install -d -m 0750 -o root -g 65532 "$BACKUP_METADATA_DIR"
acquire_production_lock 30

stop_application() {
  local state="$1"
  compose_with_state "$state" stop --timeout 45 web api || true
  compose_with_state "$state" rm -f web api || true
}

deactivate_application() {
  local state="$1"
  log "Failing closed: stopping and removing API/web while leaving PostgreSQL and Redis intact"
  stop_application "$state"
  rm -f "$ACTIVE_MARKER"
}

was_active=false; inactive_guard=false
if [[ -f "$ACTIVE_MARKER" && ! -L "$ACTIVE_MARKER" ]]; then
  was_active=true
else
  inactive_guard=true
fi

# An inactive activation must never survive without the enabled marker. This
# also protects against interruption after Compose starts API/web but before the
# marker is promoted atomically.
# shellcheck disable=SC2329 # invoked by the EXIT trap
cleanup_inactive_activation() {
  local status=$?
  trap - EXIT
  if [[ "$inactive_guard" == true ]]; then
    log "Inactive activation did not commit; removing application containers and candidate state"
    deactivate_application "$images"
    rm -f "$CURRENT_IMAGES" "$PREVIOUS_IMAGES" "${STATE_DIR}/.enabled.tmp"
  fi
  exit "$status"
}
trap cleanup_inactive_activation EXIT

if [[ "$was_active" == true ]]; then
  validate_image_state "$CURRENT_IMAGES"
else
  log "Inactive activation: removing orphan application containers and rehearsal image state"
  stop_application "$images"
  rm -f "$CURRENT_IMAGES" "$PREVIOUS_IMAGES" "$ACTIVE_MARKER" "${STATE_DIR}/.enabled.tmp"
fi

restore_active_deployment() {
  install_image_state "$PREVIOUS_IMAGES" "$CURRENT_IMAGES"
  if compose up -d --remove-orphans && wait_for_url http://127.0.0.1/readyz 60 2; then
    log "Previous active deployment restored"
    return 0
  fi
  log "Previous active deployment could not be restored"
  deactivate_application "$CURRENT_IMAGES"
  return 1
}

log "Pulling immutable candidate images"
compose_with_state "$images" pull
if [[ "$was_active" == true ]]; then install_image_state "$CURRENT_IMAGES" "$PREVIOUS_IMAGES"; fi
install_image_state "$images" "$CURRENT_IMAGES"

log "Activating the candidate; this is the only command that starts the API"
if ! compose up -d --remove-orphans; then
  log "Activation failed"
  if [[ "$was_active" == true ]]; then restore_active_deployment || true; fi
  exit 1
fi
if ! wait_for_url http://127.0.0.1/readyz 60 2; then
  log "Candidate never became ready"
  if [[ "$was_active" == true ]]; then restore_active_deployment || true; fi
  exit 1
fi
if ! bash "${SCRIPT_DIR}/functional-smoke.sh"; then
  log "Candidate failed the functional route smoke test"
  if [[ "$was_active" == true ]]; then restore_active_deployment || true; fi
  exit 1
fi
printf 'activated_at=%s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" > "${STATE_DIR}/.enabled.tmp"
mv -f "${STATE_DIR}/.enabled.tmp" "$ACTIVE_MARKER"
chmod 0600 "$ACTIVE_MARKER"
inactive_guard=false
log "Beacon production deployment is ready"
