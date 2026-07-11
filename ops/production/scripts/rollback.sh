#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ops/production/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"
usage() { echo "usage: $0 --activate --cutover-proof FILE" >&2; exit 2; }
activate=false; proof=""
while (($#)); do
  case "$1" in --activate) activate=true; shift;; --cutover-proof) [[ $# -ge 2 ]] || usage; proof="$2"; shift 2;; *) usage;; esac
done
[[ "$activate" == true && -n "$proof" ]] || usage
require_root; require_cmd docker; require_cmd curl; require_cmd flock
require_fresh_cutover_proof "$proof"
acquire_production_lock 30
[[ -f "$ACTIVE_MARKER" && ! -L "$ACTIVE_MARKER" ]] || die "manual rollback requires an active deployment"
validate_image_state "$CURRENT_IMAGES"; validate_image_state "$PREVIOUS_IMAGES"
original_current="$(mktemp "${STATE_DIR}/.rollback-current.XXXXXX")"
original_previous="$(mktemp "${STATE_DIR}/.rollback-previous.XXXXXX")"
# shellcheck disable=SC2329 # invoked by the EXIT trap
cleanup() { rm -f "$original_current" "$original_previous"; }
trap cleanup EXIT
install -m 0600 -o root -g root "$CURRENT_IMAGES" "$original_current"
install -m 0600 -o root -g root "$PREVIOUS_IMAGES" "$original_previous"

log "Pre-pulling immutable rollback images before changing deployment state"
compose_with_state "$original_previous" pull api web
install_image_state "$original_previous" "$CURRENT_IMAGES"
install_image_state "$original_current" "$PREVIOUS_IMAGES"

if compose up -d --remove-orphans && wait_for_url http://127.0.0.1/readyz 60 2; then
  log "Rollback completed; current and previous image states were swapped"
  exit 0
fi

log "Rollback activation failed; restoring original image state and runtime"
install_image_state "$original_current" "$CURRENT_IMAGES"
install_image_state "$original_previous" "$PREVIOUS_IMAGES"
if compose up -d --remove-orphans && wait_for_url http://127.0.0.1/readyz 60 2; then
  log "Original deployment restored after rollback failure"
  exit 1
fi

log "Original deployment could not be restored; failing closed"
compose stop --timeout 45 web api || true
compose rm -f web api || true
rm -f "$ACTIVE_MARKER"
die "rollback and original deployment both failed readiness"
