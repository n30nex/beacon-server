#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${EUID} -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || { echo "scenario tests require root or sudo" >&2; exit 1; }
  exec sudo bash "$0" --as-root
fi

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
PRODUCTION="${ROOT}/ops/production"
TEST_ROOT="$(mktemp -d)"
trap 'rm -rf "$TEST_ROOT"' EXIT
MOCK_BIN="${TEST_ROOT}/bin"
mkdir -p "$MOCK_BIN"

cat > "${MOCK_BIN}/docker" <<'MOCK'
#!/usr/bin/env bash
set -Eeuo pipefail
printf '%s\n' "$*" >> "${MOCK_LOG}"
if [[ ${1:-} == inspect ]]; then
  id="${*: -1}"; service="${id#id-}"
  if grep -Fxq "$service" "${MOCK_RECOVERED}" 2>/dev/null; then
    echo running:healthy
  elif [[ " ${MOCK_UNHEALTHY_SERVICES:-} " == *" ${service} "* ]]; then
    echo running:unhealthy
  else
    echo running:healthy
  fi
  exit 0
fi
[[ ${1:-} == compose ]] || exit 0
command=""
for argument in "$@"; do
  case "$argument" in pull|up|stop|rm|ps|exec|restart) command="$argument"; break;; esac
done
case "$command" in
  ps)
    echo "id-${*: -1}"
    ;;
  exec)
    if [[ ${MOCK_API_READYZ_FAIL:-false} == true ]] && ! grep -Fxq api "${MOCK_RECOVERED}" 2>/dev/null; then exit 1; fi
    ;;
  restart)
    service="${*: -1}"
    if grep -Eq '^restart_epochs=[0-9]+' "${MOCK_WATCHDOG_STATE:-/nonexistent}" 2>/dev/null; then
      echo "attempt-persisted-before-restart:${service}" >> "${MOCK_LOG}"
    fi
    echo "$service" >> "${MOCK_RECOVERED}"
    ;;
  up)
    if [[ " $* " == *" --no-deps "* ]]; then
      echo "${*: -1}" >> "${MOCK_RECOVERED}"
    else
      count=0; [[ -f ${MOCK_UP_COUNT} ]] && count="$(cat "${MOCK_UP_COUNT}")"
      count=$((count + 1)); echo "$count" > "${MOCK_UP_COUNT}"
      (( count > ${MOCK_UP_FAIL_COUNT:-0} )) || exit 1
    fi
    ;;
  pull|stop|rm) ;;
  *) echo "unhandled docker compose command: $*" >&2; exit 1;;
esac
MOCK

cat > "${MOCK_BIN}/curl" <<'MOCK'
#!/usr/bin/env bash
set -Eeuo pipefail
if [[ ${MOCK_EDGE_READYZ_FAIL:-false} == true ]] && ! grep -Fxq web "${MOCK_RECOVERED}" 2>/dev/null; then
  exit 22
fi
exit 0
MOCK
chmod 0755 "${MOCK_BIN}/docker" "${MOCK_BIN}/curl"

write_images() {
  local file="$1" api_digit="$2" web_digit="$3"
  printf 'BEACON_API_IMAGE=ghcr.io/n30nex/beacon-server@sha256:%s\nBEACON_WEB_IMAGE=ghcr.io/n30nex/beacon-web@sha256:%s\n' \
    "$(printf "${api_digit}%.0s" {1..64})" "$(printf "${web_digit}%.0s" {1..64})" > "$file"
  chmod 0600 "$file"
}

prepare_scenario() {
  local name="$1" base
  base="${TEST_ROOT}/${name}"
  mkdir -p "${base}/state" "${base}/backups" "${base}/metadata"
  printf 'POSTGRES_DSN=postgres://beacon:test@postgres:5432/beacon\n' > "${base}/beacon.env"
  printf 'regions: []\n' > "${base}/config.yaml"
  printf 'password\n' > "${base}/postgres-password"
  printf 'local_api_stopped=true\nlocal_web_stopped=true\n' > "${base}/proof"
  chmod 0600 "${base}/beacon.env" "${base}/config.yaml" "${base}/postgres-password" "${base}/proof"
  write_images "${base}/candidate.env" c d
  : > "${base}/docker.log"; : > "${base}/recovered"
  echo "$base"
}

run_production_script() {
  local base="$1"; shift
  env PATH="${MOCK_BIN}:${PATH}" \
    BEACON_COMPOSE_FILE="${PRODUCTION}/compose.production.yml" \
    BEACON_STATE_DIR="${base}/state" \
    BEACON_BACKUP_DIR="${base}/backups" \
    BEACON_BACKUP_METADATA_DIR="${base}/metadata" \
    BEACON_SECRET_ENV_FILE="${base}/beacon.env" \
    BEACON_CONFIG_FILE="${base}/config.yaml" \
    BEACON_POSTGRES_PASSWORD_FILE="${base}/postgres-password" \
    BEACON_PRODUCTION_LOCK="${base}/production.lock" \
    MOCK_LOG="${base}/docker.log" \
    MOCK_RECOVERED="${base}/recovered" \
    MOCK_UP_COUNT="${base}/up.count" \
    MOCK_WATCHDOG_STATE="${base}/state/watchdog.state" \
    MOCK_UP_FAIL_COUNT="${MOCK_UP_FAIL_COUNT:-0}" \
    MOCK_UNHEALTHY_SERVICES="${MOCK_UNHEALTHY_SERVICES:-}" \
    MOCK_API_READYZ_FAIL="${MOCK_API_READYZ_FAIL:-false}" \
    MOCK_EDGE_READYZ_FAIL="${MOCK_EDGE_READYZ_FAIL:-false}" \
    bash "$@"
}

scenario_inactive_deploy_failure() {
  local base
  base="$(prepare_scenario inactive-failure)"
  write_images "${base}/state/current.env" 1 2
  write_images "${base}/state/previous.env" a b
  MOCK_UP_FAIL_COUNT=1
  if run_production_script "$base" "${PRODUCTION}/scripts/deploy.sh" \
    --images "${base}/candidate.env" --activate --cutover-proof "${base}/proof"; then
    echo "inactive failed deployment unexpectedly succeeded" >&2; exit 1
  fi
  [[ ! -e ${base}/state/current.env && ! -e ${base}/state/previous.env && ! -e ${base}/state/enabled ]]
  grep -Fq 'stop --timeout 45 web api' "${base}/docker.log"
  grep -Fq 'rm -f web api' "${base}/docker.log"
}

scenario_inactive_deploy_success() {
  local base
  base="$(prepare_scenario inactive-success)"
  write_images "${base}/state/current.env" 1 2
  MOCK_UP_FAIL_COUNT=0
  run_production_script "$base" "${PRODUCTION}/scripts/deploy.sh" \
    --images "${base}/candidate.env" --activate --cutover-proof "${base}/proof"
  cmp -s "${base}/candidate.env" "${base}/state/current.env"
  [[ -f ${base}/state/enabled && ! -e ${base}/state/previous.env ]]
}

scenario_active_deploy_failure_restores_previous() {
  local base
  base="$(prepare_scenario active-failure)"
  write_images "${base}/state/current.env" 1 2
  cp "${base}/state/current.env" "${base}/original-current.env"
  write_images "${base}/state/previous.env" a b
  printf 'activated_at=2026-07-11T14:00:00Z\n' > "${base}/state/enabled"
  chmod 0600 "${base}/state/enabled"
  MOCK_UP_FAIL_COUNT=1
  if run_production_script "$base" "${PRODUCTION}/scripts/deploy.sh" \
    --images "${base}/candidate.env" --activate --cutover-proof "${base}/proof"; then
    echo "failed active candidate unexpectedly succeeded" >&2; exit 1
  fi
  cmp -s "${base}/original-current.env" "${base}/state/current.env"
  cmp -s "${base}/original-current.env" "${base}/state/previous.env"
  [[ -f ${base}/state/enabled ]]
}

scenario_inactive_rollback_rejected() {
  local base
  base="$(prepare_scenario inactive-rollback)"
  write_images "${base}/state/current.env" 1 2
  write_images "${base}/state/previous.env" a b
  if run_production_script "$base" "${PRODUCTION}/scripts/rollback.sh" \
    --activate --cutover-proof "${base}/proof"; then
    echo "inactive rollback unexpectedly succeeded" >&2; exit 1
  fi
  [[ ! -s ${base}/docker.log ]]
}

scenario_web_only_watchdog_recovery() {
  local base
  base="$(prepare_scenario web-watchdog)"
  write_images "${base}/state/current.env" 1 2
  printf 'activated_at=2026-07-11T14:00:00Z\n' > "${base}/state/enabled"
  printf 'failures=2\nrestart_epochs=\n' > "${base}/state/watchdog.state"
  chmod 0600 "${base}/state/enabled" "${base}/state/watchdog.state"
  MOCK_UNHEALTHY_SERVICES=web MOCK_EDGE_READYZ_FAIL=true
  run_production_script "$base" "${PRODUCTION}/scripts/watchdog.sh"
  grep -Fq 'exec -T api wget -q -T 5 -O /dev/null http://127.0.0.1:8080/readyz' "${base}/docker.log"
  grep -Eq 'restart .* web$' "${base}/docker.log"
  if grep -Eq 'restart .* api$' "${base}/docker.log"; then
    echo "web-only recovery restarted the API" >&2; exit 1
  fi
  grep -Fq 'attempt-persisted-before-restart:web' "${base}/docker.log"
  grep -Eq '^restart_epochs=[0-9]+$' "${base}/state/watchdog.state"
}

scenario_inactive_deploy_failure
scenario_inactive_deploy_success
scenario_active_deploy_failure_restores_previous
scenario_inactive_rollback_rejected
scenario_web_only_watchdog_recovery
echo "production operations scenario tests passed"
