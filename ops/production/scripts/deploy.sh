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
mkdir -p "$STATE_DIR" "$BACKUP_DIR"
exec 9>"/run/lock/beacon-production.lock"
flock -w 30 9 || die "another Beacon operation holds the deployment lock"

deactivate_application() {
  log "Failing closed: stopping and removing API/web while leaving PostgreSQL and Redis intact"
  compose stop --timeout 45 web api || true
  compose rm -f web api || true
  rm -f "$ACTIVE_MARKER"
}

restore_previous_or_deactivate() {
  if [[ -f "$PREVIOUS_IMAGES" ]]; then
    install_image_state "$PREVIOUS_IMAGES" "$CURRENT_IMAGES"
    if compose up -d --remove-orphans && wait_for_url http://127.0.0.1/readyz 60 2; then
      log "Previous deployment restored"
      return
    fi
    log "Previous deployment could not be restored"
  fi
  deactivate_application
}

log "Pulling immutable candidate images"
compose_with_state "$images" pull
if [[ -f "$CURRENT_IMAGES" ]]; then install_image_state "$CURRENT_IMAGES" "$PREVIOUS_IMAGES"; fi
install_image_state "$images" "$CURRENT_IMAGES"

log "Activating the candidate; this is the only command that starts the API"
if ! compose up -d --remove-orphans; then
  log "Activation failed"
  restore_previous_or_deactivate
  exit 1
fi
if ! wait_for_url http://127.0.0.1/readyz 60 2; then
  log "Candidate never became ready; restoring previous image state"
  restore_previous_or_deactivate
  exit 1
fi
printf 'activated_at=%s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" > "${STATE_DIR}/.enabled.tmp"
mv -f "${STATE_DIR}/.enabled.tmp" "$ACTIVE_MARKER"
chmod 0600 "$ACTIVE_MARKER"
log "Beacon production deployment is ready"
