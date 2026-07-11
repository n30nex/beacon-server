#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
PRODUCTION="${ROOT}/ops/production"

for script in "${PRODUCTION}"/scripts/*.sh "${PRODUCTION}"/tests/*.sh; do bash -n "$script"; done
if command -v shellcheck >/dev/null 2>&1; then
  shellcheck -x "${PRODUCTION}"/scripts/*.sh "${PRODUCTION}"/tests/*.sh
fi

grep -Eq '^ARG GO_IMAGE=.*@sha256:[0-9a-f]{64}$' "${ROOT}/.build/Dockerfile"
grep -Eq '^ARG RUNTIME_IMAGE=.*@sha256:[0-9a-f]{64}$' "${ROOT}/.build/Dockerfile"
if grep -Fxq '*.sql' "${ROOT}/.dockerignore"; then
  echo "broad SQL ignore would remove embedded migrations from the image" >&2
  exit 1
fi
test -f "${ROOT}/db/migrations/001_initial_schema.sql"
compose_file="${PRODUCTION}/compose.production.yml"
grep -Fxq 'name: beacon-production' "$compose_file"
grep -Fq 'BEACON_API_IMAGE must be an immutable digest reference' "$compose_file"
grep -Fq 'BEACON_WEB_IMAGE must be an immutable digest reference' "$compose_file"
grep -Fq '    internal: true' "$compose_file"
grep -Fq '      - "80:8080"' "$compose_file"
grep -Fq '    user: "65532:65532"' "$compose_file"
grep -Fq '    mem_limit: 448m' "$compose_file"
grep -Fq '    mem_limit: 320m' "$compose_file"
grep -Fq '    mem_limit: 64m' "$compose_file"
grep -Fq '        condition: service_healthy' "$compose_file"
grep -Fq 'compose stop --timeout 45 web api' "${PRODUCTION}/scripts/deploy.sh"
grep -Fq "rm -f \"\$ACTIVE_MARKER\"" "${PRODUCTION}/scripts/deploy.sh"

if docker compose version >/dev/null 2>&1; then
  temp="$(mktemp -d)"; trap 'rm -rf "$temp"' EXIT
  digest="$(printf '1%.0s' {1..64})"
  printf 'BEACON_API_IMAGE=ghcr.io/n30nex/beacon-server@sha256:%s\nBEACON_WEB_IMAGE=ghcr.io/n30nex/beacon-web@sha256:%s\n' "$digest" "$digest" > "${temp}/images.env"
  printf 'POSTGRES_DSN=postgres://beacon:test@postgres:5432/beacon?sslmode=disable\n' > "${temp}/beacon.env"
  printf 'regions: []\n' > "${temp}/config.yaml"
  printf 'test-password\n' > "${temp}/postgres-password"
  BEACON_SECRET_ENV_FILE="${temp}/beacon.env" \
  BEACON_CONFIG_FILE="${temp}/config.yaml" \
  BEACON_POSTGRES_PASSWORD_FILE="${temp}/postgres-password" \
  BEACON_BACKUP_DIR="${temp}" \
    docker compose --env-file "${temp}/images.env" -f "${PRODUCTION}/compose.production.yml" config --quiet
elif [[ "${CI:-}" == true ]]; then
  echo "Docker Compose is required for CI production validation" >&2
  exit 1
fi

echo "production operations bundle validation passed"
