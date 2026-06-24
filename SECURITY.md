# Security Policy

## Reporting a Vulnerability

Please do not report security vulnerabilities through public GitHub issues.

Instead, contact the maintainers directly via the MeshCore Canada Discord
server: [MeshCore Canada Discord](https://discord.gg/Gz3KvJx2hf) — reach out to
**dedskelly** directly. Include as much detail as possible: the nature of the
issue, steps to reproduce, and any potential impact.

We will acknowledge receipt within 48 hours and aim to provide a fix or
mitigation within 14 days depending on severity.

Once a fix is released we will publish a security advisory on the repository.

## Scope

Beacon's current v1 API is a public read-only telemetry surface. There is
currently no end-user authentication layer.

Public routes:

- `/api/v1/*`
- `/ws`
- `/healthz`
- `/readyz`
- `/swagger`

Deployment requirements:

- Set explicit CORS origins in `config.yaml`.
- Set explicit `websocket.allowed_origins` for any cross-origin browser use.
- Keep `rate_limits` enabled for public deployments. The Caddy public proxy in
  `beacon-web/docker` also builds a Caddy image with `mholt/caddy-ratelimit`
  and applies per-IP edge limits to `/api*`, `/healthz*`, `/readyz*`, and
  `/ws*`; tune the `CADDY_*_RATE_LIMIT_*` environment variables per deployment.
- Keep future write/admin routes behind the private route group until real
  authentication is implemented.
- Treat decrypted channel messages, node IDs, observer locations, channel
  metadata, and route traces as public telemetry only when that is an explicit
  deployment decision.

See `docs/security-model.md` for the full public/private contract.
