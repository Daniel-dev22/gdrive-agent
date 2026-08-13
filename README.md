# gdrive-agent

A single-host daemon that mirrors Google Drive accounts to local disk.

One binary serves HTTP + WebSocket on `:8080`. It pulls per-account OAuth
bundles from a central controller on demand, walks a Drive folder tree,
downloads changed files with resumable byte-range requests and streaming md5
verification, keeps rotated versions and a graveyard of deleted files, fires
its own cron schedules, and reports job state upward through a durable outbox
that survives restarts and controller outages.

It is one of a family of small agents that each own one host-local job and
share their operational scaffolding through
[agent-kit-go](https://github.com/Daniel-dev22/agent-kit-go).

---

## ⚠ Security: there is no inbound authentication

**Every route on this agent is unauthenticated at the application layer.**
This is not an oversight to be worked around; it is the design, and you need
to know it before you run this.

- `RegisterRoutes` builds a bare `gin` group with **no middleware**. The group
  is named `auth` for historical reasons — it does not authenticate anything.
- The bearer token the agent holds (`GDRIVE_NODE_TOKEN`) is **outbound only**.
  It is attached to calls the agent makes *to* the controller. Nothing checks
  it on the way in.
- Both WebSocket upgraders set `CheckOrigin` to return `true`, so **any**
  origin may open `/ws/fleet` and `/ws/jobs/:id/logs`.
- `GET /files/:repo/:version/*path` streams arbitrary mirrored file content.

Concretely: **exposing `:8080` gives anyone who can reach it an unauthenticated
read of every file you have mirrored, plus the ability to start, cancel and
target backup and restore jobs.**

The security model is entirely *"an mTLS-terminating reverse proxy sits in
front of this"*. The agent listens on plain HTTP inside its container and
assumes the proxy has already established who the caller is. Run it that way,
on a private network, or put your own authentication in front of it.

---

## Architecture

| Path | What it does |
|---|---|
| `cmd/gdrive-agent/main.go` | Entrypoint. Wires every component, opens SQLite, sweeps orphaned jobs, starts the outbox, scheduler, reconcile loop and HTTP server; handles SIGINT/SIGTERM with a 15s graceful shutdown. |
| `internal/config` | Env-var parsing and validation. No flags, no config files. |
| `internal/db` | The job store: a single-writer SQLite database (`modernc.org/sqlite`, pure Go, no cgo) holding the `repos` and `jobs` tables, plus the boot-time orphan sweep. |
| `internal/job/runner.go` | The backup/restore state machine — job lifecycle, the three-tier change check, version rotation, graveyard burial, restore path allowlisting. The largest and most important file here. |
| `internal/job/logs.go` | Per-job log ring (500 lines) with subscriber fan-out; slow subscribers drop, they never block the job. |
| `internal/job/helpers.go` | Mime helpers (native-Google detection, export format mapping) and a pooled copy. |
| `internal/drive/client.go` | `ClientFactory`: per-account OAuth token sources over one shared HTTP/2 transport. |
| `internal/drive/limiter.go` | `AdaptiveLimiter` — see below. |
| `internal/drive/revisions.go` | Paginated `Revisions.List` wrapper. |
| `internal/drivestore/download.go` | `StreamDownload`: the resumable, md5-verified media download. |
| `internal/drivestore/stats.go` | The per-account `stats.json` sidecar — compute, write, read. |
| `internal/schedules` | Adapter over the kit's cron scheduler; supplies the fire function that turns a schedule into a backup job. |
| `internal/fleet` | Builds the snapshot broadcast on `/ws/fleet` (host facts + repos + recent jobs + live jobs). |
| `internal/ws` | The two WebSocket handlers: fleet snapshots and live job-log tails. |
| `internal/router` | Every HTTP handler, plus the outbound secret fetchers and the job event emitter. |

---

## How a backup works

`POST /backup` returns a job id immediately; the work happens on a detached
goroutine. One pass does:

1. **Collect the Drive tree.** One paginated `files.list` sweep fetches every
   non-trashed file with all the metadata the change check needs. If the repo
   names a source folder, files are then filtered to descendants of it — the
   parent graph collected in the same pass resolves each file's path. (The
   `root` alias is resolved to its canonical id first, because Drive never
   serialises `root` into a file's `parents[]`, and comparing against the
   literal string would filter out every file.)
2. **Three-tier skip check, per file.** Tier 1: Drive's `md5Checksum` plus size
   match the manifest. Tier 2 (and the fallback for native Google files, which
   have no md5): `modifiedTime` plus size, and `headRevisionId` when both sides
   have one. Tier 3 is the server's job — the request carries
   `If-Modified-Since` and a `304` means nothing changed. Tiers 1 and 2 cost no
   network at all, so a steady-state run over an unchanged Drive transfers
   almost nothing.
3. **Rotate the live copy** into `_versions/<file_id>/<iso>__<name>` *before*
   downloading, so the previous version survives a crash mid-download.
4. **Resumable download.** See `StreamDownload` below. Native Google files
   (Docs/Sheets/Slides/Drawings) are exported instead — to docx/xlsx/pptx/png
   respectively; Forms, Sites and Maps have no export and are skipped. A single
   retry is attempted on md5 mismatch.
5. **Update the manifest** with the new size, md5, path and version list.
6. **Prune versions** to the repo's `keep_versions`, dropping pruned names from
   the manifest as well as from disk.
7. **Bury vanished files.** Anything in the manifest that the Drive sweep did
   not see is moved to `graveyard/<file_id>/<iso>__<name>` and marked deleted
   in the manifest, rather than being destroyed.
8. **Sweep the graveyard**, dropping entries older than `graveyard_keep_days`.
9. **Optionally pull Drive revisions** into `_drive_revisions/<file_id>/<rev>`
   when the repo sets `pull_drive_revisions`. Best-effort; a failure logs and
   does not fail the job. Native files are skipped.
10. **Save the manifest atomically**, then **recompute `stats.json`** on a
    detached goroutine so the disk crawl never delays the job's completion
    event.

Progress is persisted and broadcast every 5 changed files, so a watching UI
sees a live counter without SQLite being hammered.

**Restore** (`POST /restore`) is pure local copy, no network: it reads either
the live mirror or a named version out of `_versions/`, and writes into a
scratch directory or an allowlisted custom path.

---

## The mirror layout

```
<GDRIVE_BACKUP_ROOT>/
  .agent/
    state.db                       SQLite: repos, jobs, pending_events
    schedules.json                 Scheduler's disk cache (atomically written)
  <account_id>/
    manifest.json                  Per-file state: path, md5, size, modifiedTime,
    manifest.json.bak                versions, deleted_at. .bak is the last good copy
    stats.json                     Cached aggregate footprint, rewritten per backup
    files/<drive path>             The live mirror, mirroring Drive's folder tree
    _versions/<file_id>/<iso>__<name>
    graveyard/<file_id>/<iso>__<name>
    _drive_revisions/<file_id>/<revision_id>
  restore-<uuid>/                  Scratch restore target (target_mode=scratch)
```

Two things worth knowing about this layout:

- **`.part` files are expected to survive across runs.** That is precisely what
  makes downloads resumable: the next attempt stats the `.part`, sends
  `Range: bytes=N-`, re-hashes the bytes already on disk so the in-band md5
  still covers the whole file, and appends. Do not "clean up" stray `.part`
  files as a maintenance task — you are throwing away completed transfer work.
  (They are gitignored, but that is about the repo, not the mirror.)
- **Everything lives under one root**, including the state database. That is
  deliberate: the container needs exactly one bind-mount.

---

## The adaptive limiter

Every outbound Drive API call passes through `AdaptiveLimiter`, which is two
independent gates:

- a **rate limiter** (`x/time/rate`) capping requests per second, with a
  one-second burst budget, and
- a **weighted semaphore** (`x/sync/semaphore`) capping simultaneous in-flight
  requests, so HTTP/2 multiplexing cannot open an unbounded number of streams
  in the same wall-clock second.

Both are needed: RPS alone permits a burst of N requests inside one second,
and concurrency alone permits an unbounded sustained rate.

On a `429` from Drive, **both ceilings halve**, with a floor of 1. A background
goroutine then ramps both back up by +1 every 60 seconds for as long as no
`429` has landed in the trailing window, until it reaches the configured
ceilings.

The implementation detail that matters: `rate.Limiter` supports `SetLimit` in
place, but a semaphore's capacity is immutable — so the semaphore lives behind
an `atomic.Pointer` and is *replaced* on resize. A caller releases against the
semaphore it acquired from, captured in its release closure, not against
whatever is current. Releasing on the wrong instance would over-grant capacity
and let the effective ceiling drift upward forever.

---

## HTTP API

All routes are unauthenticated — see the security section. Bodies are JSON.

### Health

| Route | Behaviour |
|---|---|
| `GET`/`HEAD` `/health/live` | Always `200 {"status":"live"}`. Touches no dependency. |
| `GET`/`HEAD` `/health/ready` | Pings SQLite. `200 {"status":"ready"}`, or `503 {"status":"not_ready","error":…}`. |

`HEAD` aliases exist because container healthcheck tools (`wget --spider`,
`curl -I`) issue `HEAD`, and gin does not route `HEAD` to a `GET` handler.

### Repos

| Route | Behaviour |
|---|---|
| `POST /repos/init` | Create or update a repo binding. `200 {"id":…,"status":"ready"}`. |
| `POST /repos/bind` | Identical to `/repos/init` — an idempotent rebind, same body. |
| `GET /repos` | All bindings. |
| `GET /repos/:id` | One binding, or `404`. |
| `GET /repos/:id/stats` | Cached mirror stats. **`204` when no backup has completed yet** (no `stats.json` sidecar) — that is the "never backed up" signal, not an error. `404` if the repo is unknown. |
| `DELETE /repos/:id` | `204`. Drops the binding only; **on-disk data is left alone** deliberately. |

`/repos/init` body — `id` and `account_id` are required:

| Field | Default | Meaning |
|---|---|---|
| `id` | — | Repo id, chosen by the controller |
| `account_id` | — | Which Google account's credentials to fetch |
| `source_drive_folder_id` | whole Drive | Restrict the backup to this folder's subtree |
| `source_drive_label` | `""` | Display label only |
| `dest_local_path` | `<backup root>/<account_id>` | Informational; the runner always uses the derived path |
| `keep_versions` | `10` | Rotated versions kept per file |
| `graveyard_keep_days` | `30` | Retention for files that vanished from Drive |
| `conditional_get` | `false` | **Stored but never read** — see "Known gaps" |
| `pull_drive_revisions` | `false` | Also mirror each file's Drive revision history |
| `filter_include_mime` | `[]` | **Stored but never read** — see "Known gaps" |
| `filter_exclude_paths` | `[]` | **Stored but never read** — see "Known gaps" |

### Credentials and schedules

| Route | Behaviour |
|---|---|
| `POST /internal/credentials/:id/invalidate` | Drops the cached OAuth token source for that account so the next job re-fetches the bundle. `200`. |
| `POST /schedules/refresh` | Pull schedules from the controller immediately instead of waiting for the 5-minute tick. `200 {"refreshed":true}`; `502` if the pull fails; `503` if the scheduler is not initialised. |

### Jobs

| Route | Behaviour |
|---|---|
| `POST /backup` | Body `{"repo_id":…, "schedule_id":…, "trigger_key":…}` (only `repo_id` required). `202 {"job_id":…}`. |
| `POST /restore` | See below. `202 {"job_id":…}`, or `400`. |
| `POST /jobs/:id/cancel` | `200 {"cancelled":…}`, or `404` if the job is not currently running. Cancellation is cooperative — it cancels the job context, checked between files. |
| `GET /jobs` | The 100 most recent job rows, newest first. |
| `GET /jobs/:id` | One job, or `404`. |

`POST /restore` body:

| Field | Meaning |
|---|---|
| `repo_id` | Required |
| `version` | `""`/`"current"` for the live mirror, or an ISO timestamp prefix matching a `_versions/` entry |
| `file_id` | Optional — restore one file instead of the whole manifest |
| `target_mode` | `""`/`"scratch"` (default) writes to `<backup root>/restore-<uuid>/`; `"custom"` uses `custom_path` |
| `custom_path` | Required for `target_mode=custom`. **Must be absolute and must sit inside `GDRIVE_RESTORE_ALLOWED_ROOTS`**, or the request is rejected with `400` before any IO happens |
| `trigger_key` | Optional provenance label |

### Files and versions

| Route | Behaviour |
|---|---|
| `GET /snapshots/:repo/:revision/files` | The mirrored file tree as `{"files":[…]}`. `:revision` of `current` (or empty) lists files present in the live mirror; any other value filters to files having a version whose name starts with it. |
| `GET /revisions/:repo/:file_id` | `{"revisions":[{"timestamp","size","sanitized_name"}]}`, newest first — the rotated version history for one file. |
| `GET /files/:repo/:version/*path` | Streams one file with `Content-Disposition: attachment`. **`*path` is the Drive file id**, not a filesystem path — Drive names contain slashes and other characters that break routing. A trailing `/raw` is stripped. Ranges, `Content-Length` and `If-Modified-Since` come from `http.ServeFile`, so browsers get a real progress bar. |

### Drive folder picker

`GET /drive/folders?account_id=…&q=…&parent=…` proxies a server-side
`files.list` restricted to folders, so a UI can browse Drive without doing
OAuth in the browser. `account_id` is required (`400` otherwise); `q` is a name
substring; `parent` restricts to one folder's children. At most 100 entries,
returned as `{"folders":[{"id","name"}]}`. Drive errors surface as `502`.

This route uses a *separate* credential fetch (`secrets-for-browse`) because it
must work before any repo binding exists — otherwise you could never create the
first repo for an account.

### WebSockets

| Route | Behaviour |
|---|---|
| `GET /ws/fleet` | Pushes `{"type":"snapshot","data":{…}}` frames: host facts, all repos, the 50 most recent jobs, and every in-flight job with live counters. An initial frame is sent immediately on connect, then on every job state change, plus a 30s liveness rebuild. Delivery is conflating latest-wins, so a slow client skips intermediate frames but never misses the newest one. |
| `GET /ws/jobs/:id/logs` | Flushes the job's log ring as scrollback, then tails new lines. `404` before upgrade if the job is unknown or already cleaned up. |

Both send a server ping every 30s. Both accept any `Origin`.

> **Empty-list quirk.** `GET /repos` and `GET /jobs` marshal a nil Go slice, so
> an empty result is JSON `null`, not `[]`. A client doing `data.map(...)`
> straight off the response will throw on a fresh install. Guard for it.

---

## Configuration

Everything comes from the environment. There are no flags (beyond `--help` and
`--version`) and no config file.

### Required

| Variable | Meaning |
|---|---|
| `GDRIVE_NODE_TOKEN` | The bearer token attached to **outbound** calls to the controller. Required unless supplied via `BEARER_TOKEN_FILE`. |
| `CONTROL_CENTER_URL` | Controller base URL, e.g. `https://controller.example.com:1443`. Trailing slashes are trimmed. Its host stays the HTTP `Host` header and TLS SNI even when the dial target is rewritten. |
| `SITE_ID` | Short site label (e.g. `site-a`) stamped onto outbound job events so the controller need not infer it from the connection. |
| `NODE_NAME` | This host's name, e.g. `nas`. It is the `:node` segment in the controller's `/api/gdrive/:node/*` routes and the `?node=` in the schedule pull. |

### Optional

| Variable | Default | Meaning |
|---|---|---|
| `BEARER_TOKEN_FILE` | unset | Path to a file holding the bearer token. **Takes precedence over `GDRIVE_NODE_TOKEN`** when set; an unreadable path is a fatal startup error. |
| `TRAEFIK_DOCKER_DNS` | unset | Docker-network DNS name to dial *instead of* the host in `CONTROL_CENTER_URL`, keeping the original `Host`/SNI. Lets a host-local reverse proxy route and attach a client certificate, so the agent never depends on the host resolving the public hostname. Empty means direct dial. |
| `TRAEFIK_DIAL_PORT` | `1443` | Port used with `TRAEFIK_DOCKER_DNS`. |
| `GDRIVE_BACKUP_ROOT` | `/mnt/backups/gdrive` | Root of the mirror. Everything, including the state DB, lives under it. |
| `GDRIVE_MAX_RPS` | `10` | Drive requests/second ceiling. Must be `> 0`. |
| `GDRIVE_MAX_CONCURRENT` | `8` | Simultaneous in-flight Drive requests. Must be `> 0`. |
| `GDRIVE_RESTORE_ALLOWED_ROOTS` | the backup root | CSV of absolute prefixes a `target_mode=custom` restore may write into. The default means a fresh install can only restore back under the backup root — writing anywhere else is an explicit opt-in. |
| `LISTEN_ADDR` | `:8080` | Bind address. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. Structured `log/slog` output. |

Invalid or unparseable numeric values fall back to the default rather than
failing; missing *required* variables are reported together in one error and
the process exits non-zero.

> **`LOCAL_TLS_DOMAIN` was removed.** It used to be validated as *required* and
> was then never read by any code path — a variable you had to set for the
> process to boot, which then did nothing. Nothing in this repo consumes it, so
> rather than invent a use for it, it is gone. Setting it is now harmless; it
> is simply ignored.

---

## Mounts

**`GDRIVE_BACKUP_ROOT` is the only bind-mount the agent requires.** The mirror,
the manifests, the stats sidecars, the SQLite database and the scheduler's disk
cache all live under it — deliberately, so there is never a second mount to
forget, and so the agent's operational state cannot drift away from the data it
describes.

Optionally, mount a root-owned mode-`0600` file containing the bearer token and
point `BEARER_TOKEN_FILE` at it. **The container runs as root specifically so
that this file can stay `0600`** — a non-root uid would force you to widen its
permissions on the host, which is the opposite of what a secret file wants.

---

## Controller contract

The agent is not standalone: it expects a controller implementing five
endpoints, all reached at `CONTROL_CENTER_URL`. If you adopt this agent you
will need to provide them.

| Call | Purpose | Bearer? |
|---|---|---|
| `POST /api/gdrive/jobs/:id/event` | Job lifecycle events (`started`, `completed`, `failed`, `cancelled`) as a **flat** JSON object carrying `job_id` at the top level, plus site, node, counters and timings. Delivered by the durable outbox: persisted to SQLite first, POSTed immediately, and retried every 30s until accepted. | yes |
| `POST /api/gdrive/reconcile` | On startup and every 5 minutes, `{site, node, snapshot_at, jobs}` carrying this node's authoritative job set — everything non-terminal plus terminal jobs from the last 24h. The safety net for a terminal event that was lost or arrived out of order and would otherwise leave a controller row stuck `running`. | yes |
| `GET /api/gdrive/schedules?node=<node>` | The schedule list for this node. Pulled at startup and every 5 minutes, cached to disk, and evaluated locally — **the controller is not in the firing path**, so neither a controller outage nor an agent restart can make this host miss a window. | **no** |
| `GET /api/gdrive/accounts/:id/secrets-for-node/:node` | The OAuth bundle for a backup/restore job. The controller is expected to check that a repo binding exists before vending. | yes |
| `GET /api/gdrive/accounts/:id/secrets-for-browse/:node` | The same bundle for the folder picker, without the repo-binding requirement — a new account with zero repos still needs to browse Drive to create its first one. | yes |

Note the asymmetry: the schedule pull carries **no** `Authorization` header,
because it goes over the plain client. Either that endpoint must not require
the bearer, or the pull will fail.

**OAuth bundles are vended per job and held in memory only.** The agent never
persists a `refresh_token` or `client_secret` to disk; the controller decrypts
them, hands them over, and the agent caches the derived token source in memory
until `/internal/credentials/:id/invalidate` drops it or the process exits.

---

## Build and run

```bash
go build ./...
go vet ./...
go test ./...
```

`go build ./...` works out of the box provided you can fetch
`github.com/Daniel-dev22/agent-kit-go`.

Container builds are less convenient, honestly: `agent-kit-go` is currently a
**private** module, and the Dockerfile fetches it through a buildx secret
declared `required=true`, so a bare `docker build` fails rather than silently
falling back to an anonymous fetch. You need a GitHub token with read access:

```bash
GITHUB_TOKEN=… docker buildx build \
  --secret id=ghtoken,env=GITHUB_TOKEN \
  --build-arg VERSION="$(git describe --tags --always)" \
  -t gdrive-agent:dev .
```

The image is `golang:1.26` → `alpine:3.21` and runs as root — see
[Mounts](#mounts) for why. `CGO_ENABLED=0` costs nothing here because
`modernc.org/sqlite` is pure Go.

Running it directly:

```bash
GDRIVE_NODE_TOKEN=…                                      \
CONTROL_CENTER_URL=https://controller.example.com:1443   \
SITE_ID=site-a                                           \
NODE_NAME=nas                                            \
GDRIVE_BACKUP_ROOT=/mnt/storage/gdrive                   \
./gdrive-agent
```

`./gdrive-agent --help` prints the full env-var and route contract.

---

## Testing

**Be aware of what is covered here: almost nothing.**

There is exactly **one** test file — `internal/drivestore/stats_test.go`, which
covers computing, writing and reading the stats sidecar. That is the whole
suite.

Everything else is untested, including all of the parts you would most want a
test on:

- `internal/job/runner.go` — ~1000 lines containing the entire backup and
  restore state machine, the three-tier change check, path resolution from
  Drive's parent graph, version rotation, graveyard burial, and the restore
  path allowlist check.
- `internal/drive/limiter.go` — the halve-on-429 and ramp-back-up logic,
  including the semaphore-swap correctness argument above.
- `internal/drivestore/download.go` — resume, re-hash-on-resume, md5 mismatch,
  the `304` path, and the fsync/rename durability sequence.
- `internal/db/sqlite.go` — the store, the orphan sweep, and the subquery
  timestamp parsing that exists because of a driver quirk.
- All HTTP handlers, both WebSocket handlers, config validation, and the
  scheduler adapter.

This is the single best place to contribute. The pure functions are the easy
wins — `canSkip`, `buildDownloadURL`, `defaultExportMime`, `isPathAllowed`,
`fileWalker.pathOf` and `descendsFrom` are all directly testable with no
network and no disk.

CI (`.github/workflows/ci.yml`) runs `go vet` and `go test -count=1` on Go 1.26.

---

## Known gaps

Documented rather than hidden:

- **`conditional_get`, `filter_include_mime` and `filter_exclude_paths` are
  inert.** All three are accepted by `/repos/init`, stored in SQLite and echoed
  back on reads — and no code path consults any of them. Conditional GET is in
  fact always on (tier 3 of the change check), so that flag is merely
  redundant, but the two filter fields silently do nothing: set an exclude
  path and every matching file is still downloaded.
- **`repos.last_backup_at` / `last_backup_job_id` columns are never written.**
  Both values are derived on read from the `jobs` table instead, which is the
  actual source of truth. The columns remain in the schema.
- **A stale `schedules` table** may exist in databases created by older builds.
  Nothing reads or writes it; the schedule cache is the kit's JSON file on
  disk. New databases no longer create it.
- **Prune schedules are accepted and skipped.** The scheduler logs a warning
  when a schedule's action is `prune`, because `Runner` has no `StartPrune`.
- **Native Google file revisions are not pulled**, only binary files' — native
  revisions require a different export flow.

---

## Related projects

- **[agent-kit-go](https://github.com/Daniel-dev22/agent-kit-go)** — the shared
  module this agent is built on. Used here for `logging`, `transport` (the
  pooled client, the dial rewriter and the bearer round-tripper), `storage`
  (manifest, versions, graveyard, path sanitisation, atomic writes),
  `eventoutbox`, `jobstore`, `reconcile`, `fleet` and `scheduler`.
- Sibling agents following the same shape:
  [build-agent](https://github.com/Daniel-dev22/build-agent),
  [docker-agent](https://github.com/Daniel-dev22/docker-agent),
  [duplicacy-agent-api](https://github.com/Daniel-dev22/duplicacy-agent-api),
  [filemesh-agent](https://github.com/Daniel-dev22/filemesh-agent).

---

## License

MIT — see [LICENSE](LICENSE).
