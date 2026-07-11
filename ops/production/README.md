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
- `/var/lib/beacon/backup-metadata`: sanitized latest-backup metadata readable by
  API uid/gid `65532`; PostgreSQL dumps and full manifests are never mounted.

Install Docker Engine/Compose, `curl`, `python3`, and `util-linux`. Copy this
directory to `/opt/beacon`, copy the two examples into their host locations,
replace every placeholder, and keep the environment/password files root-owned
mode `0600`. `deploy.sh` enforces those modes and the configuration's
`root:65532`/`0640` access before starting anything. Candidate image
files must use `repository@sha256:<manifest-digest>`; mutable tags are rejected.

## Rehearsal and cutover

1. Keep the candidate image file outside deployment state and run
   `sudo /opt/beacon/scripts/infra-up.sh /root/beacon-candidate.env`. This never
   starts API or web and prevents rehearsal state from becoming rollback state.
2. Restore the transferred archive with PostgreSQL tooling, or verify it safely:
   `sudo /opt/beacon/scripts/restore-verify.sh DUMP MANIFEST`.
3. On Windows, disable all Beacon scheduled tasks, stop API and Vite first,
   prove ports 8080/5174 are closed, take and verify the final dump, then stop
   PostgreSQL and Redis. Preserve their containers and volumes.
4. After the final restore and reconciliation, remove any rehearsal image state.
   Then take and scratch-restore an initial remote backup while ingestion is
   still stopped. The image-state override resolves Compose without installing
   a fake `current.env` or starting API/web:

   ```sh
   sudo flock -w 30 /run/lock/beacon-production.lock \
     rm -f /var/lib/beacon/deploy/current.env \
           /var/lib/beacon/deploy/previous.env \
           /var/lib/beacon/deploy/enabled \
           /var/lib/beacon/deploy/watchdog.state
   sudo env BEACON_CURRENT_IMAGES=/root/beacon-candidate.env \
     /opt/beacon/scripts/backup.sh
   dump="$(sudo find /var/backups/beacon -maxdepth 1 -type f \
     -name 'beacon-daily-*.dump' -printf '%T@ %p\n' | sort -nr | head -n1 | cut -d' ' -f2-)"
   sudo env BEACON_CURRENT_IMAGES=/root/beacon-candidate.env \
     /opt/beacon/scripts/restore-verify.sh "$dump" \
     "${dump%.dump}.manifest.json" --update-manifest
   ```

5. Only after those checks, create a fresh proof on the droplet:

   ```sh
   sudo install -m 0600 -o root -g root /dev/stdin /etc/beacon/local-stack-stopped.approved <<'EOF'
   local_api_stopped=true
   local_web_stopped=true
   EOF
   ```

6. Activate explicitly:

   ```sh
   sudo /opt/beacon/scripts/deploy.sh \
     --images /root/beacon-candidate.env \
     --activate \
     --cutover-proof /etc/beacon/local-stack-stopped.approved
   ```

The proof expires after six hours. Deployment atomically retains the prior image
state and rolls it back automatically when readiness does not pass. Manual image
rollback uses the same proof and `rollback.sh --activate --cutover-proof FILE`.
The first real activation intentionally has no previous image state; if it
fails, its exit guard removes API/web, `current.env`, and `previous.env` and
remains fail-closed. Confirm those files and the active marker are absent before
retrying. Manual rollback is accepted only for an active deployment with a
validated current and previous image state.
After remote ingestion begins, never restart the stale local database; first
stop remote ingestion and back-migrate a newly verified remote dump.

## Operations

Run `status.sh` for status, resource/OOM state, health, and readiness. Install
the timers with `install-systemd.sh`: backup runs daily at 03:30 America/Toronto
(seven daily and four Sunday copies), scratch restore runs at 04:30
America/Toronto on day one of each month, and the watchdog checks each minute.
The calendar timezone is explicit and does not depend on the host's UTC setting.
The watchdog is idle before first activation,
requires three consecutive readiness/dependency failures, and allows at most
two dependency-ordered targeted recovery attempts per six hours. Performance
degradation alone does not restart Beacon.
After first activation, prove the installed units explicitly:

```sh
sudo /opt/beacon/scripts/install-systemd.sh
sudo systemctl start beacon-backup.service
sudo systemctl show beacon-backup.service -p Result --value | grep -Fx success
sudo systemctl start beacon-restore-drill.service
sudo systemctl show beacon-restore-drill.service -p Result --value | grep -Fx success
sudo systemctl is-active --quiet beacon-backup.timer
sudo systemctl is-active --quiet beacon-restore-drill.timer
sudo systemctl is-active --quiet beacon-watchdog.timer
sudo /opt/beacon/scripts/status.sh
```
