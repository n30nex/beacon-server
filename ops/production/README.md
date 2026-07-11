# Beacon production operations

This bundle runs Beacon on one Docker host without exposing PostgreSQL, Redis,
or the API directly. Only the compiled web/Caddy image binds host port 80.
`infra-up.sh` starts PostgreSQL and Redis for rehearsal; only `deploy.sh` can
start the API and its fixed-ID MQTT clients.

## Host layout

- `/opt/beacon`: this Compose file and the `scripts` directory, root-owned.
- `/etc/beacon/beacon.env`: API DSN and MQTT credentials, mode `0600`.
- `/etc/beacon/config.yaml`: production regions/channel keys, owner
  `root:65532`, mode `0640` so the non-root API can read it.
- `/etc/beacon/postgres-password`: generated database password, mode `0600`.
- `/var/lib/beacon/deploy`: atomic `current.env` and `previous.env` image state.
- `/var/backups/beacon`: custom-format dumps and JSON manifests.

Install Docker Engine/Compose, `curl`, `python3`, and `util-linux`. Copy this
directory to `/opt/beacon`, copy the two examples into their host locations,
replace every placeholder, and keep the environment/password files root-owned
mode `0600`. `deploy.sh` enforces those modes and the configuration's
`root:65532`/`0640` access before starting anything. Candidate image
files must use `repository@sha256:<manifest-digest>`; mutable tags are rejected.

## Rehearsal and cutover

1. Install the candidate image file as `/var/lib/beacon/deploy/current.env`, then
   run `sudo /opt/beacon/scripts/infra-up.sh`. This never starts API or web.
2. Restore the transferred archive with PostgreSQL tooling, or verify it safely:
   `sudo /opt/beacon/scripts/restore-verify.sh DUMP MANIFEST`.
3. On Windows, disable all Beacon scheduled tasks, stop API and Vite first,
   prove ports 8080/5174 are closed, take and verify the final dump, then stop
   PostgreSQL and Redis. Preserve their containers and volumes.
4. Only after those checks, create a fresh proof on the droplet:

   ```sh
   sudo install -m 0600 -o root -g root /dev/stdin /etc/beacon/local-stack-stopped.approved <<'EOF'
   local_api_stopped=true
   local_web_stopped=true
   EOF
   ```

5. Activate explicitly:

   ```sh
   sudo /opt/beacon/scripts/deploy.sh \
     --images /root/beacon-candidate.env \
     --activate \
     --cutover-proof /etc/beacon/local-stack-stopped.approved
   ```

The proof expires after six hours. Deployment atomically retains the prior image
state and rolls it back automatically when readiness does not pass. Manual image
rollback uses the same proof and `rollback.sh --activate --cutover-proof FILE`.
After remote ingestion begins, never restart the stale local database; first
stop remote ingestion and back-migrate a newly verified remote dump.

## Operations

Run `status.sh` for status, resource/OOM state, health, and readiness. Install
the timers with `install-systemd.sh`: backup runs daily at 03:30 (seven daily and
four Sunday copies), scratch restore runs monthly at 04:30 on day one, and the
watchdog checks each minute. The watchdog is idle before first activation,
requires three consecutive readiness failures, and allows at most two API
restarts per six hours. Performance degradation alone does not restart Beacon.
