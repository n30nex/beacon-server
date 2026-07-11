#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ops/production/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"
require_root; require_cmd systemctl
UNIT_DIR="$(cd -- "${SCRIPT_DIR}/../systemd" && pwd)"
for unit in beacon-backup.service beacon-backup.timer beacon-restore-drill.service beacon-restore-drill.timer beacon-watchdog.service beacon-watchdog.timer; do
  install -m 0644 -o root -g root "${UNIT_DIR}/${unit}" "/etc/systemd/system/${unit}"
done
systemctl daemon-reload
systemctl enable --now beacon-backup.timer beacon-restore-drill.timer beacon-watchdog.timer
systemctl list-timers 'beacon-*' --no-pager
