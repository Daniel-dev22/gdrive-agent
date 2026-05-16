package router

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"

	kitsched "github.com/Daniel-dev22/agent-kit-go/scheduler"

	"github.com/Daniel-dev22/gdrive-agent/internal/config"
	"github.com/Daniel-dev22/gdrive-agent/internal/db"
	"github.com/Daniel-dev22/gdrive-agent/internal/drive"
	"github.com/Daniel-dev22/gdrive-agent/internal/fleet"
	"github.com/Daniel-dev22/gdrive-agent/internal/job"
	"github.com/Daniel-dev22/gdrive-agent/internal/storage"
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
	// to disk, and ticked locally — controller scheduler-api + EDA
	// are NOT in the firing path (mirrors duplicacy-agent).
	Scheduler *kitsched.Scheduler

	// SecretsFetcher returns the per-account credentials bundle for JOB
	// execution. In production this calls the router's
	// /api/gdrive/accounts/:id/secrets-for-node/:node endpoint over the mTLS
	// tunnel; the router checks for an existing repo binding before vending.
	// For tests it can return a static bundle.
	SecretsFetcher func(ctx context.Context, accountID string) (drive.SecretBundle, error)

	// BrowseSecretsFetcher returns the per-account credentials bundle for
	// the FOLDER PICKER, which needs Drive credentials before any repo row
	// exists (first-repo bootstrap). Calls
	// /api/gdrive/accounts/:id/secrets-for-browse/:node — same bearer auth,
	// same bundle shape, no repo-binding requirement.
	BrowseSecretsFetcher func(ctx context.Context, accountID string) (drive.SecretBundle, error)

	// RouterClient is the HTTP client used for outbound calls back to the
	// controller router. Pooled, HTTP/2 enabled, no per-request alloc.
	RouterClient *http.Client
}

// RegisterRoutes mounts every endpoint listed in plan §F.2. Inbound auth
// terminates at the mTLS tunnel (per-site client cert verified by the host
// Traefik); application-layer bearer auth is reserved for the reverse
// direction (agent → controller calls in handleSecrets / outbound code).
func (a *App) RegisterRoutes(r *gin.Engine) {
	// Health endpoints — no auth. HEAD aliases registered because container
	// healthcheck tools (`wget --spider`, `curl -I`) issue HEAD; gin doesn't
	// auto-route HEAD to GET handlers.
	r.GET("/health/live", a.handleLive)
	r.HEAD("/health/live", a.handleLive)
	r.GET("/health/ready", a.handleReady)
	r.HEAD("/health/ready", a.handleReady)

	auth := r.Group("/")

	// Repo lifecycle.
	auth.POST("/repos/init", a.handleReposInit)
	auth.POST("/repos/bind", a.handleReposBind)
	auth.GET("/repos", a.handleListRepos)
	auth.GET("/repos/:id", a.handleGetRepo)
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

	// Snapshots + file streaming.
	auth.GET("/snapshots/:repo", a.handleListSnapshots)
	auth.GET("/snapshots/:repo/:revision/files", a.handleSnapshotFiles)
	auth.GET("/files/:repo/:version/*path", a.handleStreamFile)

	// Drive folder picker (proxies a server-side Drive call so the UI
	// doesn't need OAuth in the browser).
	auth.GET("/drive/folders", a.handleListFolders)
}

// ----- health -----

func (a *App) handleLive(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "live"})
}

// handleReady reports ready iff the DB is reachable. The router test by
// performing an actual SQLite ping; if the disk fell off we want the load
// balancer to know.
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
// controller router after a CRUD write so the agent doesn't have
// to wait for the 5-min reconcile tick.
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

// handleListSnapshots returns the set of distinct version timestamps across
// all files in a repo. UI uses this to render the "pick a version" picker.
func (a *App) handleListSnapshots(c *gin.Context) {
	repoID := c.Param("repo")
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
	ms, err := storage.NewManifestStore(a.Cfg.BackupRoot, repo.AccountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	snap := ms.Snapshot()
	// Distinct ISO timestamps across every file's _versions/<id>/<iso>__...
	// Always include "current" at the top.
	seen := map[string]struct{}{}
	out := []map[string]any{{"version": "current", "label": "Current (live mirror)"}}
	for fileID := range snap {
		dir := filepath.Join(accountRoot, "_versions", fileID)
		ents, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range ents {
			iso, _, ok := storage.SplitVersionName(e.Name())
			if !ok {
				continue
			}
			if _, dup := seen[iso]; dup {
				continue
			}
			seen[iso] = struct{}{}
			out = append(out, map[string]any{"version": iso, "label": iso})
		}
	}
	c.JSON(http.StatusOK, out)
}

// handleSnapshotFiles returns the file tree at a chosen version. "Current"
// uses the manifest directly; an ISO version filters to files that have a
// matching prefix in _versions/<id>/.
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
		FileID  string `json:"file_id"`
		Path    string `json:"path"`
		Size    int64  `json:"size"`
		Mime    string `json:"mime_type"`
		HasFile bool   `json:"has_file"`
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
		out = append(out, entry{FileID: fid, Path: mf.Path, Size: mf.Size, Mime: mf.MimeType, HasFile: true})
	}
	c.JSON(http.StatusOK, out)
}

// handleStreamFile streams one file version to the caller (router → browser)
// with Content-Disposition: attachment. Range support inherited from
// http.ServeFile. URL params:
//
//   :repo     repo_id
//   :version  "current" | <iso>
//   *path     remaining URL path is the FILE ID (we addressed by id because
//             the human-readable path on Drive may contain slashes/special
//             chars that break URL routing).
//
// Browser shows a real progress bar because we set Content-Length via
// http.ServeFile.
func (a *App) handleStreamFile(c *gin.Context) {
	repoID := c.Param("repo")
	version := c.Param("version")
	pathParam := strings.TrimPrefix(c.Param("path"), "/")
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
//   q          — substring to search for in folder name (optional)
//   account_id — required: which account to authenticate as
//   parent     — optional: filter to children of this folder id
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
	// Wrap in {folders: [...]} — the frontend's gdrive-api.ts client expects
	// `r.folders` (mirrors the duplicacy picker's shape). Returning a bare
	// array makes `.map` crash on `undefined` in RepoForm.
	c.JSON(http.StatusOK, gin.H{"folders": out})
}

// ----- router event-emitter helper -----

// EventEmitter returns a job.EventEmitter that POSTs lifecycle events to
// the controller router at /api/gdrive/jobs/:id/event. Bearer-authed
// with the agent's node token. Pooled HTTP/2 client.
func (a *App) EventEmitter() job.EventEmitter {
	return func(ctx context.Context, jobID string, event string, payload map[string]any) {
		body, _ := json.Marshal(map[string]any{
			"event":     event,
			"emitted_at": time.Now().UTC().Format(time.RFC3339Nano),
			"payload":   payload,
		})
		u := fmt.Sprintf("%s/api/gdrive/jobs/%s/event", a.Cfg.ControlCenterURL, url.PathEscape(jobID))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(string(body)))
		if err != nil {
			log.Warn().Err(err).Msg("event emitter: build req")
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+a.Cfg.NodeToken)
		resp, err := a.RouterClient.Do(req)
		if err != nil {
			log.Warn().Err(err).Str("job_id", jobID).Str("event", event).Msg("event emitter: POST failed")
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			log.Warn().Int("status", resp.StatusCode).Str("job_id", jobID).Msg("event emitter: non-2xx")
		}
	}
}

// DefaultSecretsFetcher returns a SecretsFetcher for the job-run path
// (router enforces a repo binding before vending).
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
// and DefaultBrowseSecretsFetcher. The two paths return identical bundles —
// only the router-side authorization predicate differs.
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

