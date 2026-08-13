package router

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Daniel-dev22/agent-kit-go/eventoutbox"
	kitsched "github.com/Daniel-dev22/agent-kit-go/scheduler"
	"github.com/Daniel-dev22/agent-kit-go/storage"

	"github.com/Daniel-dev22/gdrive-agent/internal/config"
	"github.com/Daniel-dev22/gdrive-agent/internal/db"
	"github.com/Daniel-dev22/gdrive-agent/internal/drive"
	"github.com/Daniel-dev22/gdrive-agent/internal/drivestore"
	"github.com/Daniel-dev22/gdrive-agent/internal/fleet"
	"github.com/Daniel-dev22/gdrive-agent/internal/job"
)

// App bundles every component the HTTP handlers need. One App per agent
// process — wired in cmd/gdrive-agent/main.go.
type App struct {
	Cfg     config.Config
	Store   *db.Store
	Factory *drive.ClientFactory
	Runner  *job.Runner
	Fleet   *fleet.Hub

	// Scheduler is the per-host cron scheduler from agent-kit-go. Pulled
	// from <CONTROL_CENTER_URL>/api/gdrive/schedules?node=<self>, cached
	// to disk, and ticked locally — the controller is NOT in the firing
	// path, so a controller outage cannot make this host miss a window.
	Scheduler *kitsched.Scheduler

	// SecretsFetcher returns the per-account credentials bundle for JOB
	// execution. In production this calls the controller's
	// /api/gdrive/accounts/:id/secrets-for-node/:node endpoint over the mTLS
	// tunnel; the controller checks for an existing repo binding before
	// vending. For tests it can return a static bundle.
	SecretsFetcher func(ctx context.Context, accountID string) (drive.SecretBundle, error)

	// BrowseSecretsFetcher returns the per-account credentials bundle for
	// the FOLDER PICKER, which needs Drive credentials before any repo row
	// exists (first-repo bootstrap). Calls
	// /api/gdrive/accounts/:id/secrets-for-browse/:node — same bearer auth,
	// same bundle shape, no repo-binding requirement.
	BrowseSecretsFetcher func(ctx context.Context, accountID string) (drive.SecretBundle, error)

	// RouterClient is the HTTP client used for outbound calls back to the
	// controller. Pooled, HTTP/2 enabled, no per-request alloc.
	RouterClient *http.Client

	// Outbox is the durable, restart-surviving queue for job lifecycle
	// events POSTed to the controller. Constructed in
	// cmd/gdrive-agent/main.go and shared with the EventEmitter so a
	// transient controller outage or an agent restart no longer drops
	// events (the previous fire-and-forget POST did). The Outbox owns
	// delivery + retries; EventEmitter only marshals + Enqueues.
	Outbox *eventoutbox.Outbox
}

// RegisterRoutes mounts the agent's whole HTTP surface.
//
// SECURITY: none of these routes is authenticated. The `auth` group below is
// a bare gin group with NO middleware — the name is historical. The agent
// assumes an mTLS-terminating reverse proxy in front of it does the
// authentication; the bearer token it holds is used only in the OUTBOUND
// direction (agent → controller, see secretsFetcher). Anything that can
// reach this listener can read every mirrored file and start jobs.
func (a *App) RegisterRoutes(r *gin.Engine) {
	// Health endpoints — no auth. HEAD aliases registered because container
	// healthcheck tools (`wget --spider`, `curl -I`) issue HEAD; gin doesn't
	// auto-route HEAD to GET handlers.
	r.GET("/health/live", a.handleLive)
	r.HEAD("/health/live", a.handleLive)
	r.GET("/health/ready", a.handleReady)
	r.HEAD("/health/ready", a.handleReady)

	// NOTE: no middleware — see the SECURITY note above.
	auth := r.Group("/")

	// Repo lifecycle.
	auth.POST("/repos/init", a.handleReposInit)
	auth.POST("/repos/bind", a.handleReposBind)
	auth.GET("/repos", a.handleListRepos)
	auth.GET("/repos/:id", a.handleGetRepo)
	auth.GET("/repos/:id/stats", a.handleRepoStats)
	auth.DELETE("/repos/:id", a.handleDeleteRepo)

	// Cache busts + schedule nudges.
	auth.POST("/internal/credentials/:id/invalidate", a.handleInvalidateCredential)
	auth.POST("/schedules/refresh", a.handleSchedulesRefresh)

	// Job lifecycle.
	auth.POST("/backup", a.handleBackup)
	auth.POST("/restore", a.handleRestore)
	auth.POST("/jobs/:id/cancel", a.handleCancelJob)
	auth.GET("/jobs", a.handleListJobs)
	auth.GET("/jobs/:id", a.handleGetJob)

	// Live-mirror file listing + per-file revision history + file streaming.
	// gdrive has no atomic snapshots; the picker browses the live tree and
	// drills into each file's rotated revision history.
	auth.GET("/snapshots/:repo/:revision/files", a.handleSnapshotFiles)
	auth.GET("/revisions/:repo/:file_id", a.handleFileRevisions)
	auth.GET("/files/:repo/:version/*path", a.handleStreamFile)

	// Drive folder picker (proxies a server-side Drive call so the UI
	// doesn't need OAuth in the browser).
	auth.GET("/drive/folders", a.handleListFolders)
}

// ----- health -----

func (a *App) handleLive(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "live"})
}

// handleReady reports ready iff the DB is reachable. It performs an actual
// SQLite ping rather than returning a constant — if the backing disk fell
// away we want whatever fronts this agent to know.
func (a *App) handleReady(c *gin.Context) {
	if err := a.Store.DB.PingContext(c.Request.Context()); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// ----- repos -----

// reposInitReq is what the router POSTs on /repos/init. The agent stores the
// binding in SQLite and (optionally) pre-warms the drive.Service.
type reposInitReq struct {
	ID                 string   `json:"id" binding:"required"`
	AccountID          string   `json:"account_id" binding:"required"`
	SourceFolderID     string   `json:"source_drive_folder_id"`
	SourceLabel        string   `json:"source_drive_label"`
	DestLocalPath      string   `json:"dest_local_path"`
	KeepVersions       int      `json:"keep_versions"`
	GraveyardKeepDays  int      `json:"graveyard_keep_days"`
	ConditionalGet     bool     `json:"conditional_get"`
	PullDriveRevisions bool     `json:"pull_drive_revisions"`
	FilterIncludeMime  []string `json:"filter_include_mime"`
	FilterExcludePaths []string `json:"filter_exclude_paths"`
}

func (a *App) handleReposInit(c *gin.Context) {
	var req reposInitReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	row := reqToRepoRow(req, a.Cfg.BackupRoot)
	if err := a.Store.UpsertRepo(c.Request.Context(), row); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	a.Fleet.Trigger()
	c.JSON(http.StatusOK, gin.H{"id": row.ID, "status": "ready"})
}

func (a *App) handleReposBind(c *gin.Context) {
	// /repos/bind is the same shape as /repos/init — idempotent rebind.
	a.handleReposInit(c)
}

func (a *App) handleListRepos(c *gin.Context) {
	rows, err := a.Store.ListRepos(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, rows)
}

func (a *App) handleGetRepo(c *gin.Context) {
	row, err := a.Store.GetRepo(c.Request.Context(), c.Param("id"))
	if err != nil {
		if db.IsNoRows(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, row)
}

// handleRepoStats serves the cached aggregate stats for a repo's account from
// the stats.json sidecar, computed once at the end of each backup. 204 when no
// backup has completed yet (no sidecar) — the UI falls back to its "no backup
// yet" state. Stats are per-account; repos sharing an account see the same row.
func (a *App) handleRepoStats(c *gin.Context) {
	repo, err := a.Store.GetRepo(c.Request.Context(), c.Param("id"))
	if err != nil {
		if db.IsNoRows(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	accountRoot := filepath.Join(a.Cfg.BackupRoot, repo.AccountID)
	stats, ok, err := drivestore.ReadRepoStats(accountRoot)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.Status(http.StatusNoContent)
		return
	}
	c.JSON(http.StatusOK, stats)
}

func (a *App) handleDeleteRepo(c *gin.Context) {
	if err := a.Store.DeleteRepo(c.Request.Context(), c.Param("id")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	a.Fleet.Trigger()
	c.Status(http.StatusNoContent)
}

func reqToRepoRow(req reposInitReq, backupRoot string) db.RepoRow {
	dst := req.DestLocalPath
	if dst == "" {
		dst = filepath.Join(backupRoot, req.AccountID)
	}
	keep := req.KeepVersions
	if keep == 0 {
		keep = 10
	}
	grave := req.GraveyardKeepDays
	if grave == 0 {
		grave = 30
	}
	return db.RepoRow{
		ID:                 req.ID,
		AccountID:          req.AccountID,
		SourceFolderID:     req.SourceFolderID,
		SourceLabel:        req.SourceLabel,
		DestLocalPath:      dst,
		KeepVersions:       keep,
		GraveyardKeepDays:  grave,
		ConditionalGet:     req.ConditionalGet,
		PullDriveRevisions: req.PullDriveRevisions,
		FilterIncludeMime:  strings.Join(req.FilterIncludeMime, ","),
		FilterExcludePaths: strings.Join(req.FilterExcludePaths, ","),
	}
}

// ----- cache bust -----

func (a *App) handleInvalidateCredential(c *gin.Context) {
	a.Factory.Invalidate(c.Param("id"))
	c.JSON(http.StatusOK, gin.H{"invalidated": c.Param("id")})
}

// handleSchedulesRefresh forces an early reconcile pull from
// <CONTROL_CENTER_URL>/api/gdrive/schedules?node=<self>. Used by the
// controller after a schedule write so the agent doesn't have to wait
// for the 5-min reconcile tick.
func (a *App) handleSchedulesRefresh(c *gin.Context) {
	if a.Scheduler == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "scheduler not initialized"})
		return
	}
	if err := a.Scheduler.Pull(c.Request.Context()); err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"refreshed": true})
}

// ----- jobs -----

type backupReq struct {
	RepoID     string `json:"repo_id" binding:"required"`
	ScheduleID string `json:"schedule_id,omitempty"`
	TriggerKey string `json:"trigger_key,omitempty"`
}

func (a *App) handleBackup(c *gin.Context) {
	var req backupReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	jobID, err := a.Runner.StartBackup(c.Request.Context(), job.BackupRequest{
		RepoID:     req.RepoID,
		ScheduleID: req.ScheduleID,
		TriggerKey: req.TriggerKey,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	a.Fleet.Trigger()
	c.JSON(http.StatusAccepted, gin.H{"job_id": jobID})
}

type restoreReq struct {
	RepoID     string `json:"repo_id" binding:"required"`
	Version    string `json:"version,omitempty"`
	FileID     string `json:"file_id,omitempty"`
	TargetMode string `json:"target_mode,omitempty"`
	CustomPath string `json:"custom_path,omitempty"`
	TriggerKey string `json:"trigger_key,omitempty"`
}

func (a *App) handleRestore(c *gin.Context) {
	var req restoreReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	jobID, err := a.Runner.StartRestore(c.Request.Context(), job.RestoreRequest{
		RepoID:     req.RepoID,
		Version:    req.Version,
		FileID:     req.FileID,
		TargetMode: req.TargetMode,
		CustomPath: req.CustomPath,
		TriggerKey: req.TriggerKey,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	a.Fleet.Trigger()
	c.JSON(http.StatusAccepted, gin.H{"job_id": jobID})
}

func (a *App) handleCancelJob(c *gin.Context) {
	ok := a.Runner.CancelJob(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not running"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"cancelled": c.Param("id")})
}

func (a *App) handleListJobs(c *gin.Context) {
	rows, err := a.Runner.SnapshotJobs(c.Request.Context(), 100)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, rows)
}

func (a *App) handleGetJob(c *gin.Context) {
	row, err := a.Store.GetJob(c.Request.Context(), c.Param("id"))
	if err != nil {
		if db.IsNoRows(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, row)
}

// ----- snapshots / file stream -----

// handleSnapshotFiles returns the live-mirror file tree. The legacy
// /snapshots/:repo aggregate-version endpoint was dropped — gdrive isn't
// snapshot-based, so the picker browses the current tree (here) and drills
// into per-file revisions via handleFileRevisions. The `:revision` URL
// segment is preserved for the "current" path; non-current values filter
// to files that have a matching prefix in _versions/<id>/ (rarely used
// directly by the UI now).
func (a *App) handleSnapshotFiles(c *gin.Context) {
	repoID := c.Param("repo")
	rev := c.Param("revision")
	repo, err := a.Store.GetRepo(c.Request.Context(), repoID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
		return
	}
	accountRoot := filepath.Join(a.Cfg.BackupRoot, repo.AccountID)
	ms, _ := storage.NewManifestStore(a.Cfg.BackupRoot, repo.AccountID)
	snap := ms.Snapshot()
	type entry struct {
		FileID        string `json:"file_id"`
		Path          string `json:"path"`
		Size          int64  `json:"size"`
		Mime          string `json:"mime_type"`
		HasFile       bool   `json:"has_file"`
		RevisionCount int    `json:"revision_count"`
	}
	out := []entry{}
	for fid, mf := range snap {
		hasFile := false
		switch {
		case rev == "" || rev == "current":
			liveAbs := filepath.Join(accountRoot, "files", storage.SanitizePath(mf.Path))
			if _, err := os.Stat(liveAbs); err == nil {
				hasFile = true
			}
		default:
			dir := filepath.Join(accountRoot, "_versions", fid)
			ents, _ := os.ReadDir(dir)
			for _, e := range ents {
				if strings.HasPrefix(e.Name(), rev) {
					hasFile = true
					break
				}
			}
		}
		if !hasFile {
			continue
		}
		out = append(out, entry{
			FileID:        fid,
			Path:          mf.Path,
			Size:          mf.Size,
			Mime:          mf.MimeType,
			HasFile:       true,
			RevisionCount: mf.VersionCount,
		})
	}
	c.JSON(http.StatusOK, gin.H{"files": out})
}

// handleFileRevisions returns the rotated version history for a single file.
// Each entry is one row in _versions/<file_id>/<iso>__<name>. Newest first.
// Sized via os.Stat; rows where Stat fails are skipped.
func (a *App) handleFileRevisions(c *gin.Context) {
	repoID := c.Param("repo")
	fileID := c.Param("file_id")
	repo, err := a.Store.GetRepo(c.Request.Context(), repoID)
	if err != nil {
		if db.IsNoRows(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	accountRoot := filepath.Join(a.Cfg.BackupRoot, repo.AccountID)
	layout := storage.NewVersionLayout(accountRoot)
	names, err := layout.ListVersions(fileID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	type revisionRow struct {
		Timestamp     string `json:"timestamp"`
		Size          int64  `json:"size"`
		SanitizedName string `json:"sanitized_name"`
	}
	out := make([]revisionRow, 0, len(names))
	for _, n := range names {
		iso, display, ok := storage.SplitVersionName(n)
		if !ok {
			continue
		}
		info, err := os.Stat(layout.VersionPath(fileID, n))
		if err != nil {
			continue
		}
		out = append(out, revisionRow{Timestamp: iso, Size: info.Size(), SanitizedName: display})
	}
	slices.SortFunc(out, func(a, b revisionRow) int { return cmp.Compare(b.Timestamp, a.Timestamp) })
	c.JSON(http.StatusOK, gin.H{"revisions": out})
}

// handleStreamFile streams one file version to the caller with
// Content-Disposition: attachment. Range support inherited from
// http.ServeFile. This route is UNAUTHENTICATED like every other one — see
// the SECURITY note on RegisterRoutes. URL params:
//
//	:repo     repo_id
//	:version  "current" | <iso>
//	*path     remaining URL path is the FILE ID (we addressed by id because
//	          the human-readable path on Drive may contain slashes/special
//	          chars that break URL routing).
//
// Browser shows a real progress bar because we set Content-Length via
// http.ServeFile.
func (a *App) handleStreamFile(c *gin.Context) {
	repoID := c.Param("repo")
	version := c.Param("version")
	pathParam := strings.TrimPrefix(c.Param("path"), "/")
	// Frontend appends a `/raw` suffix so the download URL reads "/.../FILEID/raw".
	// Gin's *path catch-all greedily includes that suffix; strip it so the
	// manifest lookup runs with the bare file ID. Leaving room for future
	// per-file metadata suffixes (/meta, /etag) following the same pattern.
	pathParam = strings.TrimSuffix(pathParam, "/raw")
	if pathParam == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "file id required in path"})
		return
	}
	fileID := pathParam // by convention; future: split for nested resource access

	repo, err := a.Store.GetRepo(c.Request.Context(), repoID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
		return
	}
	accountRoot := filepath.Join(a.Cfg.BackupRoot, repo.AccountID)
	ms, _ := storage.NewManifestStore(a.Cfg.BackupRoot, repo.AccountID)
	mf, ok := ms.Get(fileID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "file not in manifest"})
		return
	}
	var src string
	if version == "" || version == "current" {
		src = filepath.Join(accountRoot, "files", storage.SanitizePath(mf.Path))
	} else {
		dir := filepath.Join(accountRoot, "_versions", fileID)
		ents, _ := os.ReadDir(dir)
		for _, e := range ents {
			if strings.HasPrefix(e.Name(), version) {
				src = filepath.Join(dir, e.Name())
				break
			}
		}
		if src == "" {
			c.JSON(http.StatusNotFound, gin.H{"error": "version not found"})
			return
		}
	}
	info, err := os.Stat(src)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "file missing on disk"})
		return
	}
	display := storage.SanitizeName(filepath.Base(mf.Path))
	c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, display))
	if mf.MimeType != "" {
		c.Header("Content-Type", mf.MimeType)
	}
	// http.ServeFile handles Range, Content-Length, ETag and If-Modified-Since
	// for free. Keeps the agent's stream-download "blazing-efficient" line
	// honest end-to-end.
	c.Header("Last-Modified", info.ModTime().UTC().Format(http.TimeFormat))
	http.ServeFile(c.Writer, c.Request, src)
}

// ----- drive folder picker -----

type folderEntry struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// handleListFolders proxies Drive's files.list filtered to folders. Query
// params:
//
//	q          — substring to search for in folder name (optional)
//	account_id — required: which account to authenticate as
//	parent     — optional: filter to children of this folder id
//
// Returns at most 100 entries to keep the picker UI responsive.
func (a *App) handleListFolders(c *gin.Context) {
	accountID := c.Query("account_id")
	if accountID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "account_id required"})
		return
	}
	q := c.Query("q")
	parent := c.Query("parent")

	bundle, err := a.BrowseSecretsFetcher(c.Request.Context(), accountID)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "fetch secrets: " + err.Error()})
		return
	}
	svc, err := a.Factory.ForBundle(c.Request.Context(), bundle)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	query := "mimeType = 'application/vnd.google-apps.folder' and trashed = false"
	if parent != "" {
		query += fmt.Sprintf(" and '%s' in parents", strings.ReplaceAll(parent, "'", "\\'"))
	}
	if q != "" {
		// Escape single quotes in user input.
		safe := strings.ReplaceAll(q, "'", "\\'")
		query += fmt.Sprintf(" and name contains '%s'", safe)
	}
	resp, err := svc.Files.List().
		Q(query).
		Fields("files(id,name)").
		PageSize(100).
		SupportsAllDrives(true).
		IncludeItemsFromAllDrives(true).
		Context(c.Request.Context()).
		Do()
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	out := make([]folderEntry, 0, len(resp.Files))
	for _, f := range resp.Files {
		out = append(out, folderEntry{ID: f.Id, Name: f.Name})
	}
	// Wrap in {folders: [...]} rather than returning a bare array — the
	// wrapper leaves room for pagination fields later without breaking
	// clients that already destructure `.folders`.
	c.JSON(http.StatusOK, gin.H{"folders": out})
}

// ----- router event-emitter helper -----

// EventEmitter returns a job.EventEmitter that enqueues lifecycle events into
// the durable Outbox; the Outbox POSTs them to the controller at
// /api/gdrive/jobs/:id/event (bearer attached at the transport layer) and
// retries on failure so events survive a transient controller outage or an
// agent restart.
//
// Wire shape: a FLAT object (job_id, site, node, action, state, …). The
// controller upserts its own job row from these fields and requires job_id at
// the TOP level — wrapping the payload in a nested
// {event, emitted_at, payload} envelope silently drops every event with no row
// written, which is exactly what an earlier version of this did. The agent
// looks the live JobHandle up via Runner.Job() to fill in the metadata the
// runner callback's narrow signature doesn't pass through.
func (a *App) EventEmitter() job.EventEmitter {
	return func(ctx context.Context, jobID string, event string, extra map[string]any) {
		flat := map[string]any{
			"job_id":     jobID,
			"site":       a.Cfg.SiteID,
			"node":       a.Cfg.NodeName,
			"event":      event,
			"emitted_at": time.Now().UTC().Format(time.RFC3339Nano),
		}
		if h := a.Runner.Job(jobID); h != nil {
			flat["repo_id"] = h.Row.RepoID
			flat["account_id"] = h.Row.AccountID
			flat["action"] = h.Row.Action
			flat["state"] = h.Row.State
			flat["started_at"] = h.Row.StartedAt
			if h.Row.CompletedAt != nil {
				flat["completed_at"] = *h.Row.CompletedAt
			}
			if h.Row.ExitCode != nil {
				flat["exit_code"] = *h.Row.ExitCode
			}
			if h.Row.Error != "" {
				flat["error"] = h.Row.Error
			}
			flat["schedule_id"] = h.Row.ScheduleID
			flat["trigger_key"] = h.Row.TriggerKey
			flat["files_seen"] = h.Row.FilesSeen
			flat["files_changed"] = h.Row.FilesChanged
			flat["files_new"] = h.Row.FilesNew
			flat["files_versioned"] = h.Row.FilesVersioned
			flat["files_skipped_meta"] = h.Row.FilesSkippedMeta
			flat["files_skipped_not_modified"] = h.Row.FilesSkippedNotModified
			flat["files_deleted"] = h.Row.FilesDeleted
			flat["bytes_downloaded"] = h.Row.BytesDownloaded
			flat["api_calls"] = h.Row.APICalls
		}
		// Merge extras from the runner (markCompleted/markFailed pass
		// e.g. final metrics or the error message). These override the
		// JobRow snapshot when both are present — the runner's view is
		// authoritative at the moment of emission.
		for k, v := range extra {
			flat[k] = v
		}
		body, err := json.Marshal(flat)
		if err != nil {
			slog.Warn("event emitter: marshal", "error", err, "job_id", jobID, "event", event)
			return
		}
		if a.Outbox == nil {
			// Should never happen in production (wired at startup); guard so a
			// misconfigured test build degrades to a logged drop, not a panic.
			slog.Warn("event emitter: outbox not initialized; event dropped", "job_id", jobID, "event", event)
			return
		}
		// The Outbox owns durability + delivery + retries from here. It POSTs
		// the payload bytes to urlFor(jobID) with Content-Type application/json
		// and the bearer attached at the transport layer.
		a.Outbox.Enqueue(jobID, event, body)
	}
}

// DefaultSecretsFetcher returns a SecretsFetcher for the job-run path
// (the controller enforces a repo binding before vending).
func DefaultSecretsFetcher(cfg config.Config, client *http.Client) func(ctx context.Context, accountID string) (drive.SecretBundle, error) {
	return secretsFetcher(cfg, client, "secrets-for-node")
}

// DefaultBrowseSecretsFetcher returns a SecretsFetcher for the folder-picker
// path (account-existence check only — no repo binding required so a new
// account with zero repos can still browse Drive when creating its first repo).
func DefaultBrowseSecretsFetcher(cfg config.Config, client *http.Client) func(ctx context.Context, accountID string) (drive.SecretBundle, error) {
	return secretsFetcher(cfg, client, "secrets-for-browse")
}

// secretsFetcher is the shared implementation behind DefaultSecretsFetcher
// and DefaultBrowseSecretsFetcher, and the one place the OUTBOUND bearer is
// attached. The two paths return identical bundles — only the
// controller-side authorization predicate differs.
func secretsFetcher(cfg config.Config, client *http.Client, pathSegment string) func(ctx context.Context, accountID string) (drive.SecretBundle, error) {
	return func(ctx context.Context, accountID string) (drive.SecretBundle, error) {
		u := fmt.Sprintf("%s/api/gdrive/accounts/%s/%s/%s",
			cfg.ControlCenterURL, url.PathEscape(accountID), pathSegment, url.PathEscape(cfg.NodeName))
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return drive.SecretBundle{}, err
		}
		req.Header.Set("Authorization", "Bearer "+cfg.NodeToken)
		resp, err := client.Do(req)
		if err != nil {
			return drive.SecretBundle{}, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			return drive.SecretBundle{}, fmt.Errorf("secrets vend HTTP %d: %s", resp.StatusCode, string(body))
		}
		var b drive.SecretBundle
		if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
			return drive.SecretBundle{}, fmt.Errorf("decode bundle: %w", err)
		}
		return b, nil
	}
}
