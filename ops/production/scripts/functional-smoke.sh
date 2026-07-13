#!/usr/bin/env bash
set -Eeuo pipefail

origin="${BEACON_SMOKE_ORIGIN:-http://127.0.0.1}"
# A cold normalized analytics window may need to populate the shared cache on
# the 1 vCPU production database. Subsequent requests are served from cache.
max_time="${BEACON_SMOKE_MAX_TIME_SECONDS:-65}"

checks=(
  "home|/api/v1/stats/home?range=24h"
  "summary|/api/v1/stats/summary?range=24h"
  "regions|/api/v1/stats/regions?range=24h"
  "payloads|/api/v1/stats/payloads?range=24h"
  "topology|/api/v1/stats/topology?range=24h&limit=25"
  "subpaths|/api/v1/stats/subpaths?range=24h&limit=25"
)

failed=0
for check in "${checks[@]}"; do
  IFS='|' read -r name path <<< "$check"
  metrics="$(curl --fail --silent --show-error --output /dev/null \
    --max-time "$max_time" \
    --write-out 'status=%{http_code} total=%{time_total}' \
    "${origin}${path}")" || {
      printf '%s failed\n' "$name" >&2
      failed=1
      continue
    }
  printf '%-10s %s\n' "$name" "$metrics"
done

((failed == 0)) || exit 1
