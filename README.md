# gdrive-agent

Per-NAS Google Drive backup agent for Control Center. One binary serves
HTTP+WebSocket on `:8080`, gated by a bearer token presented by the
controller router. Backups land under `/mnt/backups/gdrive/<account_id>/`
with atomic streaming writes, manifest-driven idempotency, and rotated
versions.

## What it does

- Pulls per-account OAuth bundles from the router on demand.
- Streams Drive files into a local mirror with byte-range resume and
  in-band md5 verification.
- Three-tier change detection (md5 → modifiedTime+size → If-Modified-Since
  304) so steady-state runs touch almost no data.
- Rotates the previous live copy of each changed file into
  `_versions/<fileId>/<iso>__<name>` and prunes to `keep_versions`.
- Buries files that disappeared from Drive into `graveyard/`, retained for
  `graveyard_keep_days`.
- Optional per-file `Revisions.List` pull into `_drive_revisions/<id>/<rev>`.
- Serves restore as either an on-NAS path copy (`scratch` or `custom`) or
  a direct browser stream-download via `GET /files/.../raw`.
- Pushes live state to the UI over `/ws/fleet` and per-job log tails over
  `/ws/jobs/:id/logs`.

## Build

```bash
go build ./...
go test ./...

# Container build:
docker build -t gdrive-agent:dev --build-arg VERSION=$(git describe --tags --always) .
```

## Run

The agent expects a bearer token from the controller router on every
authenticated route. Health probes (`/health/live`, `/health/ready`) are
unauthenticated.

```bash
GDRIVE_NODE_TOKEN=…  \
LOCAL_TLS_DOMAIN=example.com  \
CONTROL_CENTER_URL=https://controller-api.example.com:1443  \
TRAEFIK_DOCKER_DNS=traefik  \
SITE_ID=kd  \
NODE_NAME=nas  \
GDRIVE_BACKUP_ROOT=/mnt/backups/gdrive  \
./gdrive-agent
```

See `./gdrive-agent --help` for the complete env-var contract.

## Required mounts and env vars (for ansible role authors)

The deployment role mirrors the existing duplicacy-agent-api role one-to-one:

| Concern | What the role must provide |
|---|---|
| Bearer token | `GDRIVE_NODE_TOKEN` env, sourced from Bitwarden `gdrive-node-tokens` per-host field |
| Backup volume | host bind-mount at `${GDRIVE_BACKUP_ROOT}` (default `/mnt/backups/gdrive`), writable by the container's nonroot user |
| Controller URL | `CONTROL_CENTER_URL=https://controller-api.<domain>:1443` — the public cross-site hostname. Stays as Host header / TLS SNI on every outbound call so the local Traefik can match `Host(controller-api.<domain>)` |
| Traefik DNS rewrite | `TRAEFIK_DOCKER_DNS=traefik` (and optional `TRAEFIK_DIAL_PORT=1443`) — when set, the agent's DialContext routes every outbound TCP to the named docker container instead of resolving the public hostname. Matches duplicacy-agent-api's pattern; required on NAS hosts so the agent never has to depend on the host's DNS for `controller-api.<domain>` |
| Site id | `SITE_ID=kd` on `kd-nas01`, `SITE_ID=ng` on `ng-nas01` |
| Node name | `NODE_NAME=nas` (matches the `:node` segment in the router's `/api/gdrive/:node/*` route) |

The agent never holds the cluster's mTLS client cert. Outbound HTTPS to
the router goes plain via the host's local Traefik, which attaches the
client cert on the way out. Inbound from the router lands on the same
Traefik instance and reaches the agent's port 8080 over the docker bridge.

## Layout

```
gdrive-agent/
  cmd/gdrive-agent/main.go            Server entrypoint, signal handling, wiring
  Dockerfile                          Two-stage golang:1.25 → distroless static
  go.mod / go.sum
  internal/
    auth/bearer.go                    SHA-256 const-time bearer middleware
    config/config.go                  env parsing + validation
    db/sqlite.go                      modernc SQLite, repos + jobs + schedule cache
    drive/client.go                   ClientFactory: oauth2 + HTTP/2 transport reuse
    drive/limiter.go                  AdaptiveLimiter (RPS + concurrency + halve-on-429)
    drive/revisions.go                Revisions.List paginated wrapper
    storage/atomic.go                 .part + fsync + rename + md5-streaming download
    storage/manifest.go               per-account manifest.json (+ .bak) atomic R/W
    storage/versions.go               _versions/<id>/<iso>__<name> rotate + prune
    storage/graveyard.go              graveyard/<id>/<iso>__<name> retention sweep
    storage/sanitize.go               filename sanitization (control chars, separators)
    job/runner.go                     Backup + restore lifecycle; three-tier idempotency
    job/logs.go                       Per-job log ring + subscriber fan-out
    job/helpers.go                    mime helpers, pooled copy
    fleet/snapshot.go                 Builder + Hub for /ws/fleet
    ws/fleet.go                       Gin handler for /ws/fleet
    ws/joblog.go                      Gin handler for /ws/jobs/:id/logs
    router/handlers.go                All HTTP endpoints + outbound emitters
```

## Notable decisions

- **No CGO**: modernc.org/sqlite is pure-Go, so the binary statically links
  against musl-free libc-free runtime. `CGO_ENABLED=0` everywhere. This
  buys us a distroless-static base and reproducible builds.
- **Adaptive limiter**: halves both RPS and concurrency on 429 (floor=1).
  Ramps back +1/+1 every 60s when the trailing window is 429-free.
  Implementation: `rate.Limiter.SetLimit()` for RPS (mutable in place);
  `atomic.Pointer[semaphore.Weighted]` for concurrency (semaphore is
  immutable so we swap on resize). In-flight Wait/Done pairs retain
  their original semaphore reference so a concurrent resize never
  double-releases.
- **HTTP/2 multiplexing**: one shared `http.Transport` with
  `ForceAttemptHTTP2: true` is the foundation of "concurrent downloads
  are free". The semaphore is the only cap — Go's transport pool muxes
  freely below it.
- **Streaming md5**: every download passes through
  `io.MultiWriter(file, md5.New())` via a `sync.Pool` of 64KB buffers.
  Memory footprint is independent of file size.
- **Atomic writes**: `.part` → fsync → `os.Rename` for the file; for the
  manifest, promote current to `.bak` then atomic-write the new file.
  Crash recovery falls back to `.bak` if the current file is corrupt.
- **Resume support**: `.part` files survive across runs. Next attempt
  sends `Range: bytes=N-` and appends; the agent re-hashes the existing
  bytes on disk before continuing so the in-band md5 covers the whole
  file.

## What's NOT here yet

- Scheduler loop. The `/schedules/refresh` endpoint is a stub that 200s
  with a log line. The scheduler arrives in the next phase along with the
  router-side `gdrive_schedules` table + cron parser.
- Ansible deployment role (kept in `/home/user/ansible/` as a sibling
  of `duplicacy_agent`).
