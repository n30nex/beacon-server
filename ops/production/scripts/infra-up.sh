#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ops/production/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"
require_root
require_cmd docker
mkdir -p "$STATE_DIR" "$BACKUP_DIR"
exec 9>"/run/lock/beacon-production.lock"
flock -w 30 9 || die "another Beacon operation holds the deployment lock"
compose_with_state "${1:-$CURRENT_IMAGES}" up -d --no-deps postgres redis
log "PostgreSQL and Redis are running; API and web were not started"
