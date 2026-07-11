#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ops/production/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"
require_root; require_cmd docker; require_cmd curl; require_cmd flock
[[ -f "$ACTIVE_MARKER" ]] || { log "deployment is not active; watchdog is idle"; exit 0; }
validate_image_state "$CURRENT_IMAGES"
exec 9>"/run/lock/beacon-watchdog.lock"; flock -n 9 || exit 0
state="${STATE_DIR}/watchdog.state"; failures=0; restart_epochs=""
if [[ -f "$state" ]]; then
  failures="$(sed -n 's/^failures=//p' "$state" | tail -n1)"; failures="${failures:-0}"
  restart_epochs="$(sed -n 's/^restart_epochs=//p' "$state" | tail -n1)"
fi
now="$(date +%s)"; recent=()
IFS=',' read -ra epochs <<< "$restart_epochs"
for epoch in "${epochs[@]}"; do [[ "$epoch" =~ ^[0-9]+$ ]] && (( now - epoch < 21600 )) && recent+=("$epoch"); done

healthy=false
if compose ps --status running -q postgres redis api web >/dev/null 2>&1 && \
   curl --fail --silent --max-time 5 http://127.0.0.1/readyz >/dev/null; then healthy=true; fi
if [[ "$healthy" == true ]]; then failures=0; else failures=$((failures + 1)); log "readiness/dependency failure ${failures}/3"; fi

if (( failures >= 3 )); then
  if (( ${#recent[@]} >= 2 )); then
    log "restart suppressed: two attempts already occurred in the last six hours"
  else
    log "restarting API after three consecutive readiness/dependency failures"
    compose restart --timeout 45 api
    recent+=("$now"); failures=0
  fi
fi
joined="$(IFS=,; echo "${recent[*]}")"
tmp="${state}.tmp"; printf 'failures=%s\nrestart_epochs=%s\n' "$failures" "$joined" > "$tmp"; chmod 0600 "$tmp"; mv -f "$tmp" "$state"
