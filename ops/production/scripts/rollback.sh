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
require_fresh_cutover_proof "$proof"; validate_image_state "$CURRENT_IMAGES"; validate_image_state "$PREVIOUS_IMAGES"
exec 9>"/run/lock/beacon-production.lock"; flock -w 30 9 || die "deployment lock is held"
temp="$(mktemp "${STATE_DIR}/.rollback.XXXXXX")"
install -m 0600 "$CURRENT_IMAGES" "$temp"
install_image_state "$PREVIOUS_IMAGES" "$CURRENT_IMAGES"
install_image_state "$temp" "$PREVIOUS_IMAGES"
rm -f "$temp"
compose pull
compose up -d --remove-orphans
wait_for_url http://127.0.0.1/readyz 60 2 || die "rollback image did not become ready"
log "Rollback completed; current and previous image states were swapped"
