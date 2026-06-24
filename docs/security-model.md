# Beacon Security Model

Last reviewed: 2026-06-24

Beacon v1 is a public read-only telemetry service unless a deployment places it behind an external access layer. There is no end-user authentication layer in the application today, so every public route must be safe to expose as read-only telemetry.

## Public Routes

| Route | Current policy | Notes |
| --- | --- | --- |
| `/api/v1/*` | Public read-only | Packet, node, observer, channel, route, trace, stats, live, atlas, and search data. |
| `/ws` | Public read-only stream | Requires same-origin by default; cross-origin browser access must match `websocket.allowed_origins`. |
| `/healthz` | Public operational status | Liveness/dependency summary only. |
| `/readyz` | Public deployment readiness | Intended for deploy checks and reverse proxy health probes. |
| `/swagger` | Public API docs | Keep public only while the API remains read-only. |

## Private Routes

The router has a private group stubbed behind `mw.NoopAuth`. Do not mount admin, write, mutation, import, export, or maintenance routes there until a real authentication and authorization middleware replaces the no-op guard.

Future private routes must define:

- authentication mechanism
- authorization roles or scopes
- audit logging requirements
- rate limits
- public-data impact if called incorrectly

## Origin Policy

REST CORS and WebSocket origins are separate controls.

- `cors.allowed_origins` controls browser REST access.
- `websocket.allowed_origins` controls cross-origin browser WebSocket access.
- Omit `websocket.allowed_origins` to allow same-origin handshakes only.
- Use `["*"]` only for intentionally public deployments that accept the CSRF risk.
- Production config should list concrete origins such as `https://beacon.canadaverse.org`.

## Rate and Abuse Controls

Current controls:

- The public Caddy proxy in `beacon-web/docker` builds a custom Caddy image with `github.com/mholt/caddy-ratelimit@v0.1.0` and applies per-IP edge limits to `/api*`, `/healthz*`, `/readyz*`, and `/ws*`.
- `/ws` limits concurrent connections per IP with `websocket.max_connections_per_ip`.
- `/ws` limits subscribe/unsubscribe churn per IP with `rate_limits.websocket_messages_per_ip_per_minute`; pings are excluded.
- `/api/v1/*` read routes are protected by a per-IP token bucket configured under `rate_limits`.
- Endpoint handlers enforce bounded `limit` values on list and analytics queries.
- Redis cache TTLs reduce repeated heavy read pressure when enabled.
- `/healthz` includes `rateLimits` counters with allowed/rejected totals and active buckets.

Required before adding private/write surfaces:

- operator-visible rate-limit metrics
- route-specific edge-proxy limits for any new private/write surface

## Public Data Sensitivity

Treat these fields as public only when the deployment owner has explicitly accepted that policy:

- node IDs and public keys
- observer locations and IATA groupings
- channel metadata
- decrypted channel messages
- route traces and path hashes
- broker health and ingest freshness

If any of the above should not be public for a deployment, put Beacon behind an external access layer before exposing it.
