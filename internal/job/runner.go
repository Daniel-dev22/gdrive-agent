package job

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"

	"github.com/Daniel-dev22/agent-kit-go/storage"

	"github.com/Daniel-dev22/gdrive-agent/internal/db"
	gdrive "github.com/Daniel-dev22/gdrive-agent/internal/drive"
	"github.com/Daniel-dev22/gdrive-agent/internal/drivestore"
)

// Action enumerates the supported job actions.
type Action string

const (
	ActionBackup  Action = "backup"
	ActionRestore Action = "restore"
)

// State enumerates job lifecycle states. Maps 1:1 to the gdrive_jobs table
// state column on the router side.
type State string

const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

// EventEmitter is the function the runner calls on every state transition.
// In production wiring it posts to /api/gdrive/jobs/:id/event on the router
// over the mTLS tunnel. Stubbed-out in tests.
type EventEmitter func(ctx context.Context, jobID string, event string, payload map[string]any)

// SecretsFetcher returns the per-account oauth+client bundle for the given
// account_id. The runner calls it once at job start; if a job lives long
// enough for the refresh_token to rotate, the agent's router-driven
// cache-invalidate handler drops the cached drive.Service so the NEXT job
// picks up the new bundle.
type SecretsFetcher func(ctx context.Context, accountID string) (gdrive.SecretBundle, error)

// Runner owns the job lifecycle. One Runner per agent process. Jobs run
// asynchronously; the Start* methods return immediately with a job_id.
type Runner struct {
	store    *db.Store
	factory  *gdrive.ClientFactory
	limiter  *gdrive.AdaptiveLimiter
	backupRT string // BACKUP_ROOT
	emitter  EventEmitter
	fetcher  SecretsFetcher
	allowed  []string // restore allowlist prefixes
	onChange func()   // callback fired on state transitions so the fleet hub coalesces a broadcast

	mu      sync.RWMutex
	jobs    map[string]*JobHandle // id → in-memory handle (logs, cancel)
	cancels map[string]context.CancelFunc
}

// JobHandle is the live state of one job — logs ring + cancellation hook.
type JobHandle struct {
	ID   string
	Logs *LogBuffer
	Row  db.JobRow
}

// Config wires the runner up at startup.
type Config struct {
	Store               *db.Store
	Factory             *gdrive.ClientFactory
	Limiter             *gdrive.AdaptiveLimiter
	BackupRoot          string
	RestoreAllowedRoots []string
	EventEmitter        EventEmitter
	SecretsFetcher      SecretsFetcher
	OnChange            func()
}

// New constructs a Runner.
func New(cfg Config) *Runner {
	if cfg.EventEmitter == nil {
		cfg.EventEmitter = func(ctx context.Context, jobID string, event string, payload map[string]any) {}
	}
	if cfg.OnChange == nil {
		cfg.OnChange = func() {}
	}
	return &Runner{
		store:    cfg.Store,
		factory:  cfg.Factory,
		limiter:  cfg.Limiter,
		backupRT: cfg.BackupRoot,
		emitter:  cfg.EventEmitter,
		fetcher:  cfg.SecretsFetcher,
		allowed:  cfg.RestoreAllowedRoots,
		onChange: cfg.OnChange,
		jobs:     map[string]*JobHandle{},
		cancels:  map[string]context.CancelFunc{},
	}
}

// BackupRequest is the input to StartBackup.
type BackupRequest struct {
	RepoID     string
	ScheduleID string // optional — set by scheduler-triggered runs
	TriggerKey string // optional — "manual" | "schedule"
}

// StartBackup launches an async backup job and returns its ID immediately.
func (r *Runner) StartBackup(ctx context.Context, req BackupRequest) (string, error) {
	repo, err := r.store.GetRepo(ctx, req.RepoID)
	if err != nil {
		return "", fmt.Errorf("get repo %s: %w", req.RepoID, err)
	}
	jobID := uuid.NewString()
	row := db.JobRow{
		ID:         jobID,
		RepoID:     repo.ID,
		AccountID:  repo.AccountID,
		Action:     string(ActionBackup),
		State:      string(StatePending),
		StartedAt:  time.Now().UTC(),
		ScheduleID: req.ScheduleID,
		TriggerKey: req.TriggerKey,
	}
	if err := r.store.UpsertJob(ctx, row); err != nil {
		return "", fmt.Errorf("persist job: %w", err)
	}

	jobCtx, cancel := context.WithCancel(context.Background()) // detach from request ctx — handler returns immediately
	h := &JobHandle{ID: jobID, Logs: NewLogBuffer(), Row: row}
	r.mu.Lock()
	r.jobs[jobID] = h
	r.cancels[jobID] = cancel
	r.mu.Unlock()

	go r.runBackup(jobCtx, h, repo)
	return jobID, nil
}

// runBackup executes the §F.6 backup algorithm. State transitions are
// persisted to SQLite and broadcast via the event emitter and onChange.
func (r *Runner) runBackup(ctx context.Context, h *JobHandle, repo db.RepoRow) {
	r.markRunning(ctx, h)
	defer r.cleanup(h.ID)

	r.logInfo(h, "backup starting", "repo_id", repo.ID)

	bundle, err := r.fetcher(ctx, repo.AccountID)
	if err != nil {
		r.markFailed(ctx, h, fmt.Errorf("fetch secrets: %w", err))
		return
	}
	svc, err := r.factory.ForBundle(ctx, bundle)
	if err != nil {
		r.markFailed(ctx, h, fmt.Errorf("drive.NewService: %w", err))
		return
	}

	accountRoot := filepath.Join(r.backupRT, repo.AccountID)
	if err := os.MkdirAll(filepath.Join(accountRoot, "files"), 0o755); err != nil {
		r.markFailed(ctx, h, fmt.Errorf("mkdir files: %w", err))
		return
	}
	manifestStore, err := storage.NewManifestStore(r.backupRT, repo.AccountID)
	if err != nil {
		r.markFailed(ctx, h, fmt.Errorf("open manifest: %w", err))
		return
	}
	manifestStore.SetAccountID(repo.AccountID)
	versions := storage.NewVersionLayout(accountRoot)
	grave := storage.NewGraveyard(accountRoot)

	// Build path-resolver from Drive's parent graph. Paginate files.list
	// once collecting everything we need (incl. directories) before
	// downloading anything — keeps the path graph consistent.
	walker, err := r.collectFiles(ctx, svc, repo, h)
	if err != nil {
		r.markFailed(ctx, h, fmt.Errorf("collect files: %w", err))
		return
	}
	// Now that the in-scope file count is known, stamp it on the row so
	// the fleet snapshot can broadcast a denominator for progress percent.
	// Fire onChange so the UI's progress bar transitions from
	// indeterminate ("listing…") to determinate as soon as we know.
	h.Row.TotalFiles = len(walker.files)
	r.onChange()

	prevManifest := manifestStore.Snapshot()
	seenIDs := map[string]bool{}

	// Pre-build TokenSource auth header once per job — avoids re-acquiring
	// the OAuth token on every file download. The shared HTTP/2 transport
	// in factory.HTTPClient() is what multiplexes the concurrent file
	// streams.
	authHeader, err := buildAuthHeader(ctx, r.factory, bundle)
	if err != nil {
		r.markFailed(ctx, h, fmt.Errorf("build auth header: %w", err))
		return
	}

	for _, file := range walker.files {
		if ctx.Err() != nil {
			r.markCancelled(ctx, h)
			return
		}
		seenIDs[file.Id] = true
		h.Row.FilesSeen++

		path := walker.pathOf(file)
		if path == "" {
			r.logWarn(h, "skip: no resolvable path", "file_id", file.Id, "name", file.Name)
			continue
		}
		liveAbs := filepath.Join(accountRoot, "files", storage.SanitizePath(path))

		// 3-tier idempotency.
		prev, hadPrev := prevManifest[file.Id]
		if hadPrev && r.canSkip(file, prev) {
			// Already-current entry; re-stamp manifest in case other
			// metadata (path) changed.
			updated := buildManifestEntry(file, path, prev.Versions, prev.VersionCount)
			manifestStore.Set(file.Id, updated)
			h.Row.FilesSkippedMeta++
			continue
		}

		// Decide between download vs export-as (Sheets/Docs/Slides).
		downloadURL, ifModSince, isNative := r.buildDownloadURL(file, prev)
		if downloadURL == "" {
			r.logWarn(h, "skip: no download URL", "file_id", file.Id, "mime", file.MimeType)
			continue
		}

		// Ensure parent dirs for liveAbs exist.
		if err := os.MkdirAll(filepath.Dir(liveAbs), 0o755); err != nil {
			r.logWarn(h, "mkdir parent failed", "path", liveAbs, "err", err.Error())
			continue
		}

		// Rotate existing live file into _versions/<id>/<iso>__<name>
		// BEFORE the new download so the old version survives a mid-
		// download crash.
		var rotated string
		if hadPrev {
			rotName, rotErr := versions.Rotate(file.Id, liveAbs, file.Name, time.Now().UTC())
			if rotErr != nil {
				r.logWarn(h, "rotate failed", "file_id", file.Id, "err", rotErr.Error())
			} else {
				rotated = rotName
			}
		}

		expectedMD5 := ""
		if !isNative {
			expectedMD5 = file.Md5Checksum
		}
		release, lerr := r.limiter.Wait(ctx)
		if lerr != nil {
			r.markCancelled(ctx, h)
			return
		}
		res, notMod, dlErr := drivestore.StreamDownload(ctx, drivestore.DownloadReq{
			URL:             downloadURL,
			AuthHeader:      authHeader,
			IfModifiedSince: ifModSince,
			DestPath:        liveAbs,
			ExpectedMD5:     expectedMD5,
			HTTPClient:      r.factory.HTTPClient(),
		})
		release()
		h.Row.APICalls++

		if dlErr != nil {
			r.limiter.ReportError(dlErr)
			// Single retry on md5 mismatch (drivestore.ErrMD5Mismatch).
			if errors.Is(dlErr, drivestore.ErrMD5Mismatch) {
				r.logWarn(h, "md5 mismatch, retrying", "file_id", file.Id)
				release2, lerr := r.limiter.Wait(ctx)
				if lerr != nil {
					r.markCancelled(ctx, h)
					return
				}
				res, notMod, dlErr = drivestore.StreamDownload(ctx, drivestore.DownloadReq{
					URL:             downloadURL,
					AuthHeader:      authHeader,
					IfModifiedSince: ifModSince,
					DestPath:        liveAbs,
					ExpectedMD5:     expectedMD5,
					HTTPClient:      r.factory.HTTPClient(),
				})
				release2()
				h.Row.APICalls++
			}
			if dlErr != nil {
				r.logError(h, "download failed", "file_id", file.Id, "err", dlErr.Error())
				continue
			}
		}
		if notMod {
			// Tier-3 304: refresh manifest timestamp/metadata only.
			updated := buildManifestEntry(file, path, prev.Versions, prev.VersionCount)
			manifestStore.Set(file.Id, updated)
			h.Row.FilesSkippedNotModified++
			continue
		}

		if hadPrev {
			h.Row.FilesVersioned++
		} else {
			h.Row.FilesNew++
		}
		h.Row.FilesChanged++
		h.Row.BytesDownloaded += res.Size

		// Append rotated entry to manifest versions; prune to keep N.
		newVersions := prev.Versions
		if rotated != "" {
			newVersions = append(newVersions, rotated)
		}
		if repo.KeepVersions > 0 {
			pruned, perr := versions.Prune(file.Id, repo.KeepVersions)
			if perr != nil {
				r.logWarn(h, "prune versions", "file_id", file.Id, "err", perr.Error())
			}
			// Drop pruned names from manifest list.
			if len(pruned) > 0 {
				prunedSet := map[string]struct{}{}
				for _, p := range pruned {
					prunedSet[p] = struct{}{}
				}
				filtered := newVersions[:0]
				for _, v := range newVersions {
					if _, ok := prunedSet[v]; !ok {
						filtered = append(filtered, v)
					}
				}
				newVersions = filtered
			}
		}

		entry := buildManifestEntry(file, path, newVersions, len(newVersions))
		if !isNative && res.MD5Hex != "" {
			entry.MD5 = res.MD5Hex
		}
		entry.Size = res.Size
		manifestStore.Set(file.Id, entry)

		r.logInfo(h, "downloaded", "file_id", file.Id, "path", path, "bytes", fmt.Sprintf("%d", res.Size))

		// Persist + broadcast every 5 files so the UI's progress bar feels
		// live without thrashing SQLite or the fleet hub on bursty
		// downloads (with 8 concurrent downloaders this is ~once per
		// second on average).
		if h.Row.FilesChanged%5 == 0 {
			_ = r.store.UpsertJob(ctx, h.Row)
			r.onChange()
		}
	}

	// Pass 2: files in manifest but missing from Drive → bury.
	now := time.Now().UTC()
	for fileID, mf := range prevManifest {
		if seenIDs[fileID] {
			continue
		}
		if mf.DeletedAt != nil {
			// Already buried on a previous run. Leave alone.
			continue
		}
		liveAbs := filepath.Join(accountRoot, "files", storage.SanitizePath(mf.Path))
		if _, err := grave.Bury(fileID, liveAbs, filepath.Base(mf.Path), now); err != nil {
			r.logWarn(h, "bury failed", "file_id", fileID, "err", err.Error())
			continue
		}
		manifestStore.MarkDeleted(fileID, now)
		h.Row.FilesDeleted++
		r.logInfo(h, "buried", "file_id", fileID, "path", mf.Path)
	}

	// Pass 3: graveyard retention sweep.
	if repo.GraveyardKeepDays > 0 {
		pruned, gerr := grave.Sweep(repo.GraveyardKeepDays, now)
		if gerr != nil {
			r.logWarn(h, "graveyard sweep", "err", gerr.Error())
		}
		if pruned > 0 {
			r.logInfo(h, "graveyard swept", "pruned", fmt.Sprintf("%d", pruned))
		}
	}

	// Optional: pull Drive revisions per file when configured.
	if repo.PullDriveRevisions {
		r.pullRevisions(ctx, svc, h, walker.files, accountRoot, authHeader)
	}

	// Persist manifest. Atomic — survives crash.
	if err := manifestStore.Save(); err != nil {
		r.markFailed(ctx, h, fmt.Errorf("save manifest: %w", err))
		return
	}

	r.markCompleted(ctx, h)

	// Refresh the cached stats sidecar from the just-written mirror. These
	// aggregates only change when a backup runs, so we compute them once here
	// and serve them O(1) from <accountRoot>/stats.json instead of re-deriving
	// from the manifest + crawling the disk on every UI request. Detached from
	// the job's context (the crawl must not block job teardown / the completed
	// event / WS) and run in its own goroutine; a race against a later job is
	// benign — it's recomputed next run.
	go r.refreshRepoStats(repo.AccountID, accountRoot, manifestStore.Snapshot())
}

// refreshRepoStats recomputes and persists the per-account stats sidecar. Best
// effort: any failure is logged once and swallowed (the cache is derived data,
// not authoritative). Wrapped in a recover so a crawl panic never takes down
// the agent.
func (r *Runner) refreshRepoStats(accountID, accountRoot string, manifest map[string]storage.ManifestFile) {
	defer func() {
		if rec := recover(); rec != nil {
			slog.Error("repo stats refresh panicked", "account_id", accountID, "panic", rec)
		}
	}()
	stats, err := drivestore.ComputeRepoStats(accountID, accountRoot, manifest)
	if err != nil {
		slog.Warn("repo stats compute failed", "account_id", accountID, "err", err.Error())
		return
	}
	if err := drivestore.WriteRepoStats(accountRoot, stats); err != nil {
		slog.Warn("repo stats write failed", "account_id", accountID, "err", err.Error())
		return
	}
	slog.Info("repo stats refreshed",
		"account_id", accountID,
		"current_files", stats.CurrentFiles,
		"total_disk_bytes", stats.TotalDiskBytes)
}

// canSkip applies the 3-tier check (plan §F.4.3). Tier 1+2 are pure
// metadata; tier 3 (If-Modified-Since 304) is handled inside StreamDownload.
func (r *Runner) canSkip(file *drive.File, prev storage.ManifestFile) bool {
	if file.Md5Checksum != "" {
		// Binary file — md5 + size is authoritative.
		if file.Md5Checksum == prev.MD5 && file.Size == prev.Size {
			return true
		}
	}
	// Tier 2 / native fallback: modifiedTime + size + (HeadRevisionId|version)
	if prev.ModifiedTime.IsZero() || file.ModifiedTime == "" {
		return false
	}
	prevTime := prev.ModifiedTime.UTC().Format(time.RFC3339)
	if prevTime == file.ModifiedTime && file.Size == prev.Size {
		// For natives also compare head revision id when both sides have one.
		if file.HeadRevisionId != "" && prev.HeadRevisionID != "" {
			return file.HeadRevisionId == prev.HeadRevisionID
		}
		return true
	}
	return false
}

// buildDownloadURL returns (url, ifModifiedSince, isNative).
//
//   - For binary files: GET /drive/v3/files/<id>?alt=media&supportsAllDrives=true
//   - For native files: GET /drive/v3/files/<id>/export?mimeType=<exportMime>
//
// If a manifest entry exists, ifModifiedSince is the prev.ModifiedTime so
// Drive can short-circuit with a 304.
func (r *Runner) buildDownloadURL(file *drive.File, prev storage.ManifestFile) (urlStr, ifModSince string, isNative bool) {
	if !prev.ModifiedTime.IsZero() {
		ifModSince = prev.ModifiedTime.UTC().Format(http.TimeFormat)
	}
	if isNativeMime(file.MimeType) {
		isNative = true
		export := defaultExportMime(file.MimeType)
		if export == "" {
			return "", "", true
		}
		v := url.Values{}
		v.Set("mimeType", export)
		urlStr = fmt.Sprintf("https://www.googleapis.com/drive/v3/files/%s/export?%s",
			url.PathEscape(file.Id), v.Encode())
		return urlStr, ifModSince, true
	}
	urlStr = fmt.Sprintf("https://www.googleapis.com/drive/v3/files/%s?alt=media&supportsAllDrives=true",
		url.PathEscape(file.Id))
	return urlStr, ifModSince, false
}

// pullRevisions iterates each file and downloads any new Drive revisions
// into _drive_revisions/<fileId>/<revisionId>. Best-effort; failures are
// logged but don't fail the job.
func (r *Runner) pullRevisions(ctx context.Context, svc *drive.Service, h *JobHandle, files []*drive.File, accountRoot, authHeader string) {
	for _, f := range files {
		if ctx.Err() != nil {
			return
		}
		if isNativeMime(f.MimeType) {
			// Native revisions can be exported but it's a different flow;
			// skip in v1.
			continue
		}
		release, lerr := r.limiter.Wait(ctx)
		if lerr != nil {
			return
		}
		revs, err := gdrive.RevisionList(ctx, svc, f.Id)
		release()
		h.Row.APICalls++
		if err != nil {
			r.limiter.ReportError(err)
			r.logWarn(h, "revisions list", "file_id", f.Id, "err", err.Error())
			continue
		}
		for _, rev := range revs {
			revDest := filepath.Join(accountRoot, "_drive_revisions", f.Id, rev.Id)
			if _, err := os.Stat(revDest); err == nil {
				continue // already have it
			}
			urlStr := fmt.Sprintf("https://www.googleapis.com/drive/v3/files/%s/revisions/%s?alt=media",
				url.PathEscape(f.Id), url.PathEscape(rev.Id))
			release2, lerr := r.limiter.Wait(ctx)
			if lerr != nil {
				return
			}
			_, _, derr := drivestore.StreamDownload(ctx, drivestore.DownloadReq{
				URL:         urlStr,
				AuthHeader:  authHeader,
				DestPath:    revDest,
				ExpectedMD5: rev.Md5Checksum,
				HTTPClient:  r.factory.HTTPClient(),
			})
			release2()
			h.Row.APICalls++
			if derr != nil {
				r.limiter.ReportError(derr)
				r.logWarn(h, "revision download", "file_id", f.Id, "rev_id", rev.Id, "err", derr.Error())
			}
		}
	}
}

// collectFiles paginates files.list once with all fields the idempotency
// check needs. Returns a walker that holds the slice and a path-resolver.
type fileWalker struct {
	files     []*drive.File
	parentMap map[string]*drive.File // id → file
	rootID    string                 // optional — repo root if set
}

func (w *fileWalker) pathOf(f *drive.File) string {
	if f == nil {
		return ""
	}
	parts := []string{f.Name}
	cur := f
	for {
		if len(cur.Parents) == 0 {
			break
		}
		parent, ok := w.parentMap[cur.Parents[0]]
		if !ok {
			break
		}
		if w.rootID != "" && parent.Id == w.rootID {
			break
		}
		parts = append([]string{parent.Name}, parts...)
		cur = parent
	}
	return strings.Join(parts, "/")
}

func (r *Runner) collectFiles(ctx context.Context, svc *drive.Service, repo db.RepoRow, h *JobHandle) (*fileWalker, error) {
	// Drive's "root" alias works in Q() / Get() but NEVER appears in a
	// file's parents[] — Drive serializes parents as canonical folder IDs.
	// descendsFrom() compares against parent IDs, so if we leave rootID as
	// the literal "root" string every file gets filtered out and the
	// backup walks zero files. Resolve to the canonical ID once up-front
	// so the descendant filter works. One extra API call per job.
	rootID := repo.SourceFolderID
	if rootID == "root" {
		release, lerr := r.limiter.Wait(ctx)
		if lerr != nil {
			return nil, lerr
		}
		rootFolder, err := svc.Files.Get("root").Fields("id").SupportsAllDrives(true).Context(ctx).Do()
		release()
		h.Row.APICalls++
		if err != nil {
			return nil, fmt.Errorf("resolve drive root alias: %w", err)
		}
		rootID = rootFolder.Id
		r.logInfo(h, "resolved root alias", "canonical_id", rootID)
	}
	w := &fileWalker{parentMap: map[string]*drive.File{}, rootID: rootID}
	query := "trashed=false"
	if rootID != "" {
		// Scope to the configured folder subtree. files.list doesn't filter
		// recursively by parent — we do a BFS using parents in a single
		// listing pass, then filter to descendants of the root.
	}
	pageToken := ""
	for {
		release, lerr := r.limiter.Wait(ctx)
		if lerr != nil {
			return nil, lerr
		}
		call := svc.Files.List().
			Q(query).
			Fields("files(id,name,mimeType,parents,modifiedTime,md5Checksum,size,headRevisionId,version,trashed),nextPageToken").
			PageSize(1000).
			Spaces("drive").
			SupportsAllDrives(true).
			IncludeItemsFromAllDrives(true).
			Context(ctx)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		resp, err := call.Do()
		release()
		h.Row.APICalls++
		if err != nil {
			r.limiter.ReportError(err)
			return nil, err
		}
		for _, f := range resp.Files {
			w.parentMap[f.Id] = f
		}
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	// Materialize the in-scope file slice. If a root folder is configured,
	// keep only files whose parent chain hits that folder. Folders are
	// retained in parentMap for path resolution but never downloaded.
	for _, f := range w.parentMap {
		if isFolder(f.MimeType) {
			continue
		}
		if rootID != "" && !w.descendsFrom(f, rootID) {
			continue
		}
		w.files = append(w.files, f)
	}
	return w, nil
}

func (w *fileWalker) descendsFrom(f *drive.File, ancestorID string) bool {
	visited := map[string]struct{}{}
	cur := f
	for {
		if len(cur.Parents) == 0 {
			return false
		}
		if _, seen := visited[cur.Id]; seen {
			return false // cycle defense
		}
		visited[cur.Id] = struct{}{}
		for _, pid := range cur.Parents {
			if pid == ancestorID {
				return true
			}
		}
		parent, ok := w.parentMap[cur.Parents[0]]
		if !ok {
			return false
		}
		cur = parent
	}
}

// RestoreRequest is the input to StartRestore.
type RestoreRequest struct {
	RepoID     string
	Version    string // ISO timestamp from _versions/, or "current"
	FileID     string // optional — single-file restore; empty means all files at that version
	TargetMode string // "scratch" | "custom"
	CustomPath string // absolute path when TargetMode=custom
	TriggerKey string
}

// StartRestore launches an async restore job and returns its ID.
func (r *Runner) StartRestore(ctx context.Context, req RestoreRequest) (string, error) {
	repo, err := r.store.GetRepo(ctx, req.RepoID)
	if err != nil {
		return "", fmt.Errorf("get repo: %w", err)
	}
	var dst string
	switch req.TargetMode {
	case "", "scratch":
		dst = filepath.Join(r.backupRT, "restore-"+uuid.NewString())
	case "custom":
		clean := filepath.Clean(req.CustomPath)
		if !filepath.IsAbs(clean) {
			return "", errors.New("restore: custom path must be absolute")
		}
		if !r.isPathAllowed(clean) {
			return "", fmt.Errorf("restore: path not in allowlist: %s", clean)
		}
		dst = clean
	default:
		return "", fmt.Errorf("restore: unknown target_mode %q", req.TargetMode)
	}

	jobID := uuid.NewString()
	row := db.JobRow{
		ID: jobID, RepoID: repo.ID, AccountID: repo.AccountID,
		Action: string(ActionRestore), State: string(StatePending),
		StartedAt: time.Now().UTC(), TriggerKey: req.TriggerKey,
	}
	if err := r.store.UpsertJob(ctx, row); err != nil {
		return "", err
	}

	jobCtx, cancel := context.WithCancel(context.Background())
	h := &JobHandle{ID: jobID, Logs: NewLogBuffer(), Row: row}
	r.mu.Lock()
	r.jobs[jobID] = h
	r.cancels[jobID] = cancel
	r.mu.Unlock()

	go r.runRestore(jobCtx, h, repo, req, dst)
	return jobID, nil
}

// runRestore copies versions/<fileId>/<iso>__<name> or live files/<path>
// into the target directory. No network IO — pure local copy.
func (r *Runner) runRestore(ctx context.Context, h *JobHandle, repo db.RepoRow, req RestoreRequest, dst string) {
	r.markRunning(ctx, h)
	defer r.cleanup(h.ID)

	r.logInfo(h, "restore starting", "version", req.Version, "target", dst)

	if err := os.MkdirAll(dst, 0o700); err != nil {
		r.markFailed(ctx, h, fmt.Errorf("mkdir target: %w", err))
		return
	}

	accountRoot := filepath.Join(r.backupRT, repo.AccountID)
	manifestStore, err := storage.NewManifestStore(r.backupRT, repo.AccountID)
	if err != nil {
		r.markFailed(ctx, h, fmt.Errorf("open manifest: %w", err))
		return
	}
	snap := manifestStore.Snapshot()
	versions := storage.NewVersionLayout(accountRoot)

	// Single-file vs full-repo restore.
	candidates := map[string]storage.ManifestFile{}
	if req.FileID != "" {
		if mf, ok := snap[req.FileID]; ok {
			candidates[req.FileID] = mf
		}
	} else {
		for k, v := range snap {
			candidates[k] = v
		}
	}

	for fileID, mf := range candidates {
		if ctx.Err() != nil {
			r.markCancelled(ctx, h)
			return
		}
		var src string
		if req.Version == "" || req.Version == "current" {
			src = filepath.Join(accountRoot, "files", storage.SanitizePath(mf.Path))
		} else {
			// Find the named version directory entry. The version string
			// we accept is the ISO prefix; we match the first entry whose
			// prefix is the ISO.
			vers, _ := versions.ListVersions(fileID)
			for _, v := range vers {
				if strings.HasPrefix(v, req.Version) {
					src = versions.VersionPath(fileID, v)
					break
				}
			}
			if src == "" {
				r.logWarn(h, "no version found", "file_id", fileID, "wanted", req.Version)
				continue
			}
		}
		dstPath := filepath.Join(dst, storage.SanitizePath(mf.Path))
		if err := copyFile(src, dstPath); err != nil {
			r.logError(h, "copy", "file_id", fileID, "err", err.Error())
			continue
		}
		h.Row.FilesChanged++
	}
	r.markCompleted(ctx, h)
}

func (r *Runner) isPathAllowed(p string) bool {
	for _, prefix := range r.allowed {
		clean := filepath.Clean(prefix)
		if p == clean || strings.HasPrefix(p, clean+string(os.PathSeparator)) {
			return true
		}
	}
	return false
}

// CancelJob signals a running job. No-op if the job is unknown or already
// terminal.
func (r *Runner) CancelJob(id string) bool {
	r.mu.Lock()
	cancel, ok := r.cancels[id]
	r.mu.Unlock()
	if !ok {
		return false
	}
	cancel()
	return true
}

// Job returns the in-memory handle (logs buffer + state) for one running job.
// Returns nil if the job isn't currently in-flight (terminated jobs are
// still queryable via the SQLite store).
func (r *Runner) Job(id string) *JobHandle {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.jobs[id]
}

// ListLive returns a snapshot of live job handles. Used by the fleet builder.
func (r *Runner) ListLive() []*JobHandle {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*JobHandle, 0, len(r.jobs))
	for _, h := range r.jobs {
		out = append(out, h)
	}
	return out
}

// SnapshotJobs returns the most recent N persisted job rows.
func (r *Runner) SnapshotJobs(ctx context.Context, limit int) ([]db.JobRow, error) {
	return r.store.ListJobs(ctx, limit)
}

// ----- helpers -----

func (r *Runner) markRunning(ctx context.Context, h *JobHandle) {
	h.Row.State = string(StateRunning)
	_ = r.store.UpsertJob(ctx, h.Row)
	r.emitter(ctx, h.ID, "started", nil)
	r.onChange()
}

func (r *Runner) markCompleted(ctx context.Context, h *JobHandle) {
	now := time.Now().UTC()
	h.Row.State = string(StateCompleted)
	h.Row.CompletedAt = &now
	zero := 0
	h.Row.ExitCode = &zero
	_ = r.store.UpsertJob(ctx, h.Row)
	r.logInfo(h, "completed",
		"files_seen", fmt.Sprintf("%d", h.Row.FilesSeen),
		"files_new", fmt.Sprintf("%d", h.Row.FilesNew),
		"files_versioned", fmt.Sprintf("%d", h.Row.FilesVersioned),
		"files_skipped_meta", fmt.Sprintf("%d", h.Row.FilesSkippedMeta),
		"files_skipped_not_modified", fmt.Sprintf("%d", h.Row.FilesSkippedNotModified),
		"files_deleted", fmt.Sprintf("%d", h.Row.FilesDeleted),
		"bytes", fmt.Sprintf("%d", h.Row.BytesDownloaded))
	r.emitter(ctx, h.ID, "completed", map[string]any{
		"files_seen":                 h.Row.FilesSeen,
		"files_changed":              h.Row.FilesChanged,
		"files_new":                  h.Row.FilesNew,
		"files_versioned":            h.Row.FilesVersioned,
		"files_skipped_meta":         h.Row.FilesSkippedMeta,
		"files_skipped_not_modified": h.Row.FilesSkippedNotModified,
		"files_deleted":              h.Row.FilesDeleted,
		"bytes_downloaded":           h.Row.BytesDownloaded,
		"api_calls":                  h.Row.APICalls,
	})
	r.onChange()
	h.Logs.Close()
}

func (r *Runner) markFailed(ctx context.Context, h *JobHandle, err error) {
	now := time.Now().UTC()
	h.Row.State = string(StateFailed)
	h.Row.CompletedAt = &now
	one := 1
	h.Row.ExitCode = &one
	h.Row.Error = err.Error()
	_ = r.store.UpsertJob(ctx, h.Row)
	r.logError(h, "failed", "err", err.Error())
	r.emitter(ctx, h.ID, "failed", map[string]any{"error": err.Error()})
	r.onChange()
	h.Logs.Close()
}

func (r *Runner) markCancelled(ctx context.Context, h *JobHandle) {
	now := time.Now().UTC()
	h.Row.State = string(StateCancelled)
	h.Row.CompletedAt = &now
	one := 1
	h.Row.ExitCode = &one
	_ = r.store.UpsertJob(ctx, h.Row)
	r.logWarn(h, "cancelled")
	r.emitter(ctx, h.ID, "cancelled", nil)
	r.onChange()
	h.Logs.Close()
}

func (r *Runner) cleanup(jobID string) {
	r.mu.Lock()
	delete(r.cancels, jobID)
	// Leave r.jobs[id] populated for a beat so concurrent GETs against
	// the WS log endpoint still find the handle. A cleanup ticker would
	// be overkill here — the handle is small, ~10KB at worst, and the
	// fleet snapshot only includes terminal jobs from the DB anyway.
	r.mu.Unlock()
}

func (r *Runner) logInfo(h *JobHandle, msg string, kv ...string)  { r.logAt(h, "info", msg, kv...) }
func (r *Runner) logWarn(h *JobHandle, msg string, kv ...string)  { r.logAt(h, "warn", msg, kv...) }
func (r *Runner) logError(h *JobHandle, msg string, kv ...string) { r.logAt(h, "error", msg, kv...) }

func (r *Runner) logAt(h *JobHandle, level, msg string, kv ...string) {
	field, value := "", ""
	if len(kv) >= 2 {
		field, value = kv[0], kv[1]
	}
	l := LogLine{JobID: h.ID, Time: time.Now().UTC(), Level: level, Message: msg, Field: field, Value: value}
	h.Logs.Append(l)
	switch level {
	case "warn":
		slog.Warn("job", "job_id", h.ID, "msg", msg)
	case "error":
		slog.Error("job", "job_id", h.ID, "msg", msg)
	default:
		slog.Info("job", "job_id", h.ID, "msg", msg)
	}
}

// buildManifestEntry converts a Drive File into a ManifestFile, preserving
// the rotated-version list passed in (the walker computes the new list).
func buildManifestEntry(f *drive.File, path string, vers []string, versionCount int) storage.ManifestFile {
	out := storage.ManifestFile{
		Path:           path,
		MD5:            f.Md5Checksum,
		Size:           f.Size,
		HeadRevisionID: f.HeadRevisionId,
		Version:        f.Version,
		MimeType:       f.MimeType,
		Versions:       vers,
		VersionCount:   versionCount,
	}
	if t, err := time.Parse(time.RFC3339, f.ModifiedTime); err == nil {
		out.ModifiedTime = t
	}
	return out
}

// buildAuthHeader fetches a fresh OAuth access token and returns the
// "Bearer <token>" header value. Called once per job so we don't pay the
// token-source mutex on every download.
//
// The factory parameter is unused today — kept in the signature so a
// future change that shares the cached TokenSource cleanly slots in
// without breaking call sites.
func buildAuthHeader(ctx context.Context, factory *gdrive.ClientFactory, b gdrive.SecretBundle) (string, error) {
	_ = factory
	tok, err := tokenForBundle(ctx, b)
	if err != nil {
		return "", err
	}
	return "Bearer " + tok.AccessToken, nil
}

// tokenForBundle returns a fresh oauth2.Token without exposing the factory's
// internal source cache. The factory's cached ReuseTokenSource handles
// refresh in steady state; this path duplicates a small amount of work for
// the side-channel where the storage layer needs a header string.
func tokenForBundle(ctx context.Context, b gdrive.SecretBundle) (*oauth2.Token, error) {
	scopes := b.Scopes
	if len(scopes) == 0 {
		scopes = []string{drive.DriveReadonlyScope}
	}
	cfg := &oauth2.Config{
		ClientID:     b.ClientID,
		ClientSecret: b.ClientSecret,
		Endpoint:     google.Endpoint,
		Scopes:       scopes,
	}
	tok := &oauth2.Token{RefreshToken: b.RefreshToken, AccessToken: b.AccessToken, TokenType: "Bearer"}
	if b.TokenExpiry != "" {
		if t, err := time.Parse(time.RFC3339, b.TokenExpiry); err == nil {
			tok.Expiry = t
		}
	}
	ts := oauth2.ReuseTokenSource(tok, cfg.TokenSource(ctx, tok))
	return ts.Token()
}

// copyFile streams src → dst with the same buffer pool that downloads use.
// Atomic — writes to dst+".part" then renames.
func copyFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".part"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := copyPooled(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}
