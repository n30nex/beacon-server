#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ops/production/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"
require_root
require_cmd docker
mkdir -p "$STATE_DIR"
install -d -m 0700 -o root -g root "$BACKUP_DIR"
install -d -m 0750 -o root -g 65532 "$BACKUP_METADATA_DIR"
acquire_production_lock 30
compose_with_state "${1:-$CURRENT_IMAGES}" up -d --no-deps postgres redis
log "PostgreSQL and Redis are running; API and web were not started"
