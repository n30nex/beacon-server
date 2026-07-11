#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ops/production/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"
require_root; require_cmd docker; require_cmd curl; require_cmd flock; require_cmd timeout
if ! try_acquire_production_lock; then
  log "another production operation is active; watchdog is idle"
  exit 0
fi
[[ -f "$ACTIVE_MARKER" ]] || { log "deployment is not active; watchdog is idle"; exit 0; }
validate_image_state "$CURRENT_IMAGES"
exec 9>"/run/lock/beacon-watchdog.lock"
flock -n 9 || exit 0
state="${STATE_DIR}/watchdog.state"; failures=0; restart_epochs=""
if [[ -f "$state" ]]; then
  failures="$(sed -n 's/^failures=//p' "$state" | tail -n1)"; failures="${failures:-0}"
  restart_epochs="$(sed -n 's/^restart_epochs=//p' "$state" | tail -n1)"
fi
[[ "$failures" =~ ^[0-9]+$ ]] || failures=0
now="$(date +%s)"; recent=()
IFS=',' read -ra epochs <<< "$restart_epochs"
for epoch in "${epochs[@]}"; do [[ "$epoch" =~ ^[0-9]+$ ]] && (( now - epoch < 21600 )) && recent+=("$epoch"); done

write_watchdog_state() {
  local joined tmp
  joined="$(IFS=,; echo "${recent[*]}")"
  tmp="${state}.tmp"
  printf 'failures=%s\nrestart_epochs=%s\n' "$failures" "$joined" > "$tmp"
  chmod 0600 "$tmp"
  mv -f "$tmp" "$state"
}

failure_reasons=(); failed_services=(); api_readyz_failed=false; edge_readyz_failed=false
for service in postgres redis api web; do
  id="$(compose ps -q "$service" 2>/dev/null || true)"
  if [[ -z "$id" ]]; then
    failure_reasons+=("${service}:missing")
    failed_services+=("$service")
    continue
  fi
  state_health="$(docker inspect --format '{{.State.Status}}:{{if .State.Health}}{{.State.Health.Status}}{{else}}missing-healthcheck{{end}}' "$id" 2>/dev/null || true)"
  if [[ "$state_health" != "running:healthy" ]]; then
    failure_reasons+=("${service}:${state_health:-inspect-failed}")
    failed_services+=("$service")
  fi
done
if ! compose_with_state_timeout "$CURRENT_IMAGES" 10s exec -T api \
  wget -q -T 5 -O /dev/null http://127.0.0.1:8080/readyz >/dev/null 2>&1; then
  failure_reasons+=("api-readyz:failed")
  api_readyz_failed=true
fi
if ! curl --fail --silent --max-time 5 http://127.0.0.1/readyz >/dev/null; then
  failure_reasons+=("edge-readyz:failed")
  edge_readyz_failed=true
fi
if (( ${#failure_reasons[@]} == 0 )); then
  failures=0
else
  failures=$((failures + 1))
  reasons="$(IFS=,; echo "${failure_reasons[*]}")"
  log "readiness/dependency failure ${failures}/3: ${reasons}"
fi

service_failed() {
  local expected="$1" failed
  for failed in "${failed_services[@]}"; do [[ "$failed" == "$expected" ]] && return 0; done
  return 1
}

wait_for_service_healthy() {
  local service="$1" id state_health i
  for ((i=1; i<=30; i++)); do
    id="$(compose ps -q "$service" 2>/dev/null || true)"
    if [[ -n "$id" ]]; then
      state_health="$(docker inspect --format '{{.State.Status}}:{{if .State.Health}}{{.State.Health.Status}}{{else}}missing-healthcheck{{end}}' "$id" 2>/dev/null || true)"
      [[ "$state_health" == "running:healthy" ]] && return 0
    fi
    sleep 2
  done
  return 1
}

recover_service() {
  local service="$1" timeout="$2" id
  id="$(compose ps -a -q "$service" 2>/dev/null || true)"
  if [[ -n "$id" ]]; then
    compose restart --timeout "$timeout" "$service" || return 1
  else
    compose up -d --no-deps "$service" || return 1
  fi
  wait_for_service_healthy "$service"
}

if (( failures >= 3 )); then
  if (( ${#recent[@]} >= 2 )); then
    log "restart suppressed: two attempts already occurred in the last six hours"
  else
    recovery_order=(); dependency_failed=false; api_recovery_needed=false
    for service in postgres redis; do
      if service_failed "$service"; then recovery_order+=("$service"); dependency_failed=true; fi
    done
    if service_failed api || [[ "$dependency_failed" == true || "$api_readyz_failed" == true ]]; then
      recovery_order+=("api"); api_recovery_needed=true
    fi
    if service_failed web || [[ "$edge_readyz_failed" == true && "$api_recovery_needed" != true ]]; then
      recovery_order+=("web")
    fi
    targets="$(IFS=,; echo "${recovery_order[*]}")"
    log "starting targeted recovery after three consecutive readiness/dependency failures: ${targets}"
    recent+=("$now"); failures=0
    # Persist the attempt before the first restart. A hung recovery or systemd
    # timeout must still count against the two-attempt/six-hour limit.
    write_watchdog_state
    recovery_ok=true
    for service in "${recovery_order[@]}"; do
      case "$service" in postgres) timeout=60;; redis) timeout=30;; api) timeout=45;; web) timeout=30;; esac
      if ! recover_service "$service" "$timeout"; then
        log "targeted recovery failed: ${service} did not become healthy"
        recovery_ok=false
        break
      fi
    done
    if [[ "$recovery_ok" == true ]] && ! curl --fail --silent --max-time 5 http://127.0.0.1/readyz >/dev/null; then
      log "targeted recovery completed but readiness is still failing"
      recovery_ok=false
    fi
    if [[ "$recovery_ok" != true ]]; then
      failures=3
    fi
  fi
fi
write_watchdog_state
