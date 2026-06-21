# Beacon Project Status

Last updated: 2026-06-21

This document is the shared operator/developer status note for the active Beacon stack. The workspace root at `F:\Beacon` is not itself a Git repository, so this tracked copy lives in `beacon-server` and describes both active repos.

## Active Repositories

| Repo | Branch | Role |
| --- | --- | --- |
| `F:\Beacon\beacon-server` | `dev` tracking `fork/dev` | Go API, MQTT ingest, PostgreSQL persistence, Redis-backed read cache, WebSocket fan-out, Swagger docs. |
| `F:\Beacon\beacon-web` | `main` tracking `origin/main` | React/Vite/Tailwind operator console for Atlas, Live, Packets, Channels, Map, Nodes, Observers, Routes, Traces, and Stats. |

CoreScope is intentionally out of scope for current Beacon implementation work.

## Local Runtime

| Surface | Default |
| --- | --- |
| Startup | `F:\Beacon\Start-BeaconLocal.ps1` |
| API | `http://127.0.0.1:8080` |
| WebSocket | `ws://127.0.0.1:8080/ws` |
| Web UI | `http://127.0.0.1:5174` from the launcher, or the active Vite port if started manually |
| PostgreSQL | `127.0.0.1:5432` |
| Redis | `127.0.0.1:6379` |

## Baseline Validation

Run these before merging broad Beacon changes:

```powershell
cd F:\Beacon\beacon-web
npm run lint
npm test
npm run build

cd F:\Beacon\beacon-server
go build ./...
gofmt -l .
go vet ./...
go test ./...
.\scripts\Test-BeaconLocal.ps1
```

The local smoke script checks `/healthz`, `/readyz`, `/api/v1/brokers`, `/api/v1/atlas/briefing`, `/api/v1/live/backfill`, `/api/v1/search`, the web UI, WebSocket hello, and optional local PostgreSQL/Redis TCP ports. It exits nonzero on failed required checks. Use `-RequireLocalPorts` when the database and cache are expected to be exposed on `127.0.0.1`.

## Current Improvement Tracks

- Stabilize and commit the current modern UI work in `beacon-web`.
- Stabilize and commit the current Atlas/topology/API work in `beacon-server`.
- Keep operator value first: Atlas, Live, Map, Packets, Nodes, Observers, Routes, Traces, Channels, and Stats must remain readable, fast, and drilldown-friendly.
- Keep `/api/v1` and `/ws` backward compatible; additive fields are allowed, breaking shape changes require a compatibility path.
- Treat browser QA as part of completion: desktop and phone widths, no horizontal overflow, no error boundary, readable retro and modern themes.

## Follow-Up Backlog

- Generate or contract-check frontend API types from Swagger/OpenAPI.
- Add REST and WebSocket contract tests for major payloads.
- Expand `/healthz` into separate readiness/dependency details for database, cache, MQTT brokers, ingest workers, WebSocket hub, and build metadata.
- Add structured logs for WebSocket parse failures, reconnects, request latency, slow DB queries, cache behavior, ingest errors, and packet decode failures.
- Add saved views, shareable investigation URLs, export/copy affordances, and accessibility/contrast checks.
