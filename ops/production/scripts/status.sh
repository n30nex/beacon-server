#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ops/production/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"
strict=false
while (($#)); do
  case "$1" in
    --strict) strict=true; shift ;;
    *) echo "usage: $0 [--strict]" >&2; exit 2 ;;
  esac
done
require_root; require_cmd docker; require_cmd curl
validate_image_state "$CURRENT_IMAGES"
echo "Beacon production containers"
compose ps
echo
echo "Resource state"
free -h
df -h / /var/lib/docker "$BACKUP_DIR" 2>/dev/null | awk '!seen[$1]++'
echo
echo "Container restarts/OOM"
for service in postgres redis api web; do
  id="$(compose ps -q "$service")"
  [[ -n "$id" ]] || { printf '%-10s missing\n' "$service"; continue; }
  docker inspect --format "${service} restarts={{.RestartCount}} oom={{.State.OOMKilled}} status={{.State.Status}} health={{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}}" "$id"
done
echo
curl --fail --silent --show-error --max-time 5 http://127.0.0.1/healthz
echo
curl --fail --silent --show-error --max-time 5 http://127.0.0.1/readyz
echo
if [[ "$strict" == true ]]; then
  echo
  echo "Functional routes"
  bash "${SCRIPT_DIR}/functional-smoke.sh"
fi
