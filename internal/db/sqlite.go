package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"
	_ "modernc.org/sqlite" // pure-Go sqlite driver — no CGO required
)

// Store wraps a single-writer SQLite database used for job persistence and
// repo metadata. modernc.org/sqlite is pure Go (cgo-free); we deliberately
// constrain the connection pool to a single writer because SQLite's WAL
// mode supports many concurrent readers but only one writer at a time.
// Multiple writer connections would deadlock under contention.
//
// The agent uses this DB for:
//   - jobs table: every backup/restore run with its state machine
//   - repos table: account_id + folder_id bindings agent learned from /repos/init
//   - schedules cache: pulled from router on /schedules/refresh
//
// Manifest files (per-account, on disk) are NOT in this DB — they live next
// to the data they describe. SQLite is reserved for ops the agent's HTTP
// endpoints serve to the router/UI.
type Store struct {
	DB *sql.DB
}

// Open creates or opens the SQLite file at the given path. Parent dir is
// created if missing. WAL mode + a small busy_timeout cover concurrent
// reads safely while keeping crash-recovery cheap.
func Open(ctx context.Context, path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir db dir: %w", err)
	}
	// _journal=WAL: writers don't block readers.
	// _busy_timeout=5000ms: avoid SQLITE_BUSY on transient lock contention.
	// _txlock=immediate: tx acquires write lock up front, prevents deadlock
	//   between two read-then-upgrade transactions.
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_txlock=immediate", path)
	d, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open sqlite: %w", err)
	}
	// Single-writer constraint: cap connections so concurrent goroutines
	// queue at the driver layer rather than deadlocking inside SQLite.
	d.SetMaxOpenConns(1)
	d.SetMaxIdleConns(1)
	d.SetConnMaxLifetime(0) // never expire — single conn

	if err := d.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	s := &Store{DB: d}
	if err := s.migrate(ctx); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	log.Info().Str("path", path).Msg("sqlite store ready")
	return s, nil
}

// Close flushes and closes the DB. Safe to call multiple times.
func (s *Store) Close() error {
	if s == nil || s.DB == nil {
		return nil
	}
	return s.DB.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		// jobs — one row per backup/restore lifecycle
		`CREATE TABLE IF NOT EXISTS jobs (
			id TEXT PRIMARY KEY,
			repo_id TEXT NOT NULL,
			account_id TEXT NOT NULL,
			action TEXT NOT NULL,
			state TEXT NOT NULL,
			started_at TIMESTAMP NOT NULL,
			completed_at TIMESTAMP,
			exit_code INTEGER,
			error TEXT,
			files_seen INTEGER DEFAULT 0,
			files_changed INTEGER DEFAULT 0,
			bytes_downloaded INTEGER DEFAULT 0,
			api_calls INTEGER DEFAULT 0,
			schedule_id TEXT,
			trigger_key TEXT,
			updated_at TIMESTAMP NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_repo ON jobs(repo_id, started_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)`,

		// repos — what the agent knows after /repos/init
		`CREATE TABLE IF NOT EXISTS repos (
			id TEXT PRIMARY KEY,
			account_id TEXT NOT NULL,
			source_folder_id TEXT,
			source_label TEXT,
			dest_local_path TEXT NOT NULL,
			keep_versions INTEGER DEFAULT 10,
			graveyard_keep_days INTEGER DEFAULT 30,
			conditional_get INTEGER DEFAULT 1,
			pull_drive_revisions INTEGER DEFAULT 0,
			filter_include_mime TEXT,
			filter_exclude_paths TEXT,
			last_backup_at TIMESTAMP,
			last_backup_job_id TEXT,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_repos_account ON repos(account_id)`,

		// schedules cache — agent re-pulls from router on /schedules/refresh
		`CREATE TABLE IF NOT EXISTS schedules (
			id TEXT PRIMARY KEY,
			repo_id TEXT NOT NULL,
			action TEXT NOT NULL,
			schedule_json TEXT NOT NULL,
			miss_policy TEXT,
			enabled INTEGER NOT NULL DEFAULT 1,
			params_json TEXT,
			version INTEGER NOT NULL DEFAULT 1,
			refreshed_at TIMESTAMP NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_schedules_repo ON schedules(repo_id)`,
	}
	for _, q := range stmts {
		if _, err := s.DB.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("migrate %q: %w", q, err)
		}
	}
	return nil
}

// UpsertRepo inserts or updates a repo binding. Called by /repos/init and
// /repos/bind handlers — both are idempotent.
func (s *Store) UpsertRepo(ctx context.Context, r RepoRow) error {
	now := time.Now().UTC()
	if r.CreatedAt.IsZero() {
		r.CreatedAt = now
	}
	r.UpdatedAt = now
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO repos (
			id, account_id, source_folder_id, source_label, dest_local_path,
			keep_versions, graveyard_keep_days, conditional_get, pull_drive_revisions,
			filter_include_mime, filter_exclude_paths,
			created_at, updated_at
		) VALUES (
			?, ?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?,
			?, ?
		)
		ON CONFLICT(id) DO UPDATE SET
			account_id = excluded.account_id,
			source_folder_id = excluded.source_folder_id,
			source_label = excluded.source_label,
			dest_local_path = excluded.dest_local_path,
			keep_versions = excluded.keep_versions,
			graveyard_keep_days = excluded.graveyard_keep_days,
			conditional_get = excluded.conditional_get,
			pull_drive_revisions = excluded.pull_drive_revisions,
			filter_include_mime = excluded.filter_include_mime,
			filter_exclude_paths = excluded.filter_exclude_paths,
			updated_at = excluded.updated_at
	`,
		r.ID, r.AccountID, r.SourceFolderID, r.SourceLabel, r.DestLocalPath,
		r.KeepVersions, r.GraveyardKeepDays, boolToInt(r.ConditionalGet), boolToInt(r.PullDriveRevisions),
		r.FilterIncludeMime, r.FilterExcludePaths,
		r.CreatedAt, r.UpdatedAt,
	)
	return err
}

// GetRepo loads one repo by id. ErrNoRows means absent (caller should 404).
func (s *Store) GetRepo(ctx context.Context, id string) (RepoRow, error) {
	row := s.DB.QueryRowContext(ctx, `
		SELECT id, account_id, source_folder_id, source_label, dest_local_path,
		       keep_versions, graveyard_keep_days, conditional_get, pull_drive_revisions,
		       filter_include_mime, filter_exclude_paths,
		       last_backup_at, last_backup_job_id, created_at, updated_at
		FROM repos WHERE id = ?
	`, id)
	var r RepoRow
	var cond, pull int
	var lastBackupAt sql.NullTime
	var lastJob sql.NullString
	var fInc, fExc sql.NullString
	var srcLabel, srcFolder sql.NullString
	err := row.Scan(&r.ID, &r.AccountID, &srcFolder, &srcLabel, &r.DestLocalPath,
		&r.KeepVersions, &r.GraveyardKeepDays, &cond, &pull,
		&fInc, &fExc,
		&lastBackupAt, &lastJob, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		return r, err
	}
	r.ConditionalGet = cond != 0
	r.PullDriveRevisions = pull != 0
	r.SourceFolderID = srcFolder.String
	r.SourceLabel = srcLabel.String
	r.FilterIncludeMime = fInc.String
	r.FilterExcludePaths = fExc.String
	if lastBackupAt.Valid {
		r.LastBackupAt = &lastBackupAt.Time
	}
	if lastJob.Valid {
		r.LastBackupJobID = lastJob.String
	}
	return r, nil
}

// ListRepos returns all repo rows. Used by the fleet snapshot builder.
func (s *Store) ListRepos(ctx context.Context) ([]RepoRow, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, account_id, source_folder_id, source_label, dest_local_path,
		       keep_versions, graveyard_keep_days, conditional_get, pull_drive_revisions,
		       filter_include_mime, filter_exclude_paths,
		       last_backup_at, last_backup_job_id, created_at, updated_at
		FROM repos ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RepoRow
	for rows.Next() {
		var r RepoRow
		var cond, pull int
		var lastBackupAt sql.NullTime
		var lastJob sql.NullString
		var fInc, fExc sql.NullString
		var srcLabel, srcFolder sql.NullString
		if err := rows.Scan(&r.ID, &r.AccountID, &srcFolder, &srcLabel, &r.DestLocalPath,
			&r.KeepVersions, &r.GraveyardKeepDays, &cond, &pull,
			&fInc, &fExc,
			&lastBackupAt, &lastJob, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		r.ConditionalGet = cond != 0
		r.PullDriveRevisions = pull != 0
		r.SourceFolderID = srcFolder.String
		r.SourceLabel = srcLabel.String
		r.FilterIncludeMime = fInc.String
		r.FilterExcludePaths = fExc.String
		if lastBackupAt.Valid {
			r.LastBackupAt = &lastBackupAt.Time
		}
		if lastJob.Valid {
			r.LastBackupJobID = lastJob.String
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// DeleteRepo removes a repo binding. Does NOT delete its on-disk backup —
// that's the operator's call.
func (s *Store) DeleteRepo(ctx context.Context, id string) error {
	_, err := s.DB.ExecContext(ctx, `DELETE FROM repos WHERE id = ?`, id)
	return err
}

// MarkOrphanedJobsFailed flips every job still recorded as running or
// pending into "failed" with a fixed error message. Called once at agent
// startup — a JobHandle/goroutine is conceptually tied to a runtime
// instance and never survives a process restart, so any "running" row
// in SQLite after boot is an orphan from a prior crash/redeploy.
// Without this sweep the UI keeps surfacing those rows in activeJobs
// forever, and operator clicks (Cancel, View Log) 404 because the
// runtime has no in-memory handle.
//
// Returns the number of rows remediated for logging.
func (s *Store) MarkOrphanedJobsFailed(ctx context.Context) (int, error) {
	now := time.Now().UTC()
	res, err := s.DB.ExecContext(ctx, `
		UPDATE jobs
		   SET state        = 'failed',
		       completed_at = ?,
		       exit_code    = 1,
		       error        = 'agent restarted while job was in flight',
		       updated_at   = ?
		 WHERE state IN ('running','pending')`,
		now, now)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// UpsertJob inserts or replaces a job row. Called from the runner on every
// state transition so the agent's snapshot always reflects truth.
func (s *Store) UpsertJob(ctx context.Context, j JobRow) error {
	j.UpdatedAt = time.Now().UTC()
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO jobs (
			id, repo_id, account_id, action, state, started_at, completed_at,
			exit_code, error, files_seen, files_changed, bytes_downloaded, api_calls,
			schedule_id, trigger_key, updated_at
		) VALUES (
			?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?,
			?, ?, ?
		)
		ON CONFLICT(id) DO UPDATE SET
			state = excluded.state,
			completed_at = excluded.completed_at,
			exit_code = excluded.exit_code,
			error = excluded.error,
			files_seen = excluded.files_seen,
			files_changed = excluded.files_changed,
			bytes_downloaded = excluded.bytes_downloaded,
			api_calls = excluded.api_calls,
			updated_at = excluded.updated_at
	`,
		j.ID, j.RepoID, j.AccountID, j.Action, j.State, j.StartedAt, j.CompletedAt,
		j.ExitCode, j.Error, j.FilesSeen, j.FilesChanged, j.BytesDownloaded, j.APICalls,
		j.ScheduleID, j.TriggerKey, j.UpdatedAt,
	)
	return err
}

// GetJob loads one job by id. ErrNoRows means absent.
func (s *Store) GetJob(ctx context.Context, id string) (JobRow, error) {
	row := s.DB.QueryRowContext(ctx, `
		SELECT id, repo_id, account_id, action, state, started_at, completed_at,
		       exit_code, error, files_seen, files_changed, bytes_downloaded, api_calls,
		       schedule_id, trigger_key, updated_at
		FROM jobs WHERE id = ?
	`, id)
	return scanJob(row.Scan)
}

// ListJobs returns the N most recent jobs (any repo, any state) ordered
// newest first. Used by the fleet snapshot.
func (s *Store) ListJobs(ctx context.Context, limit int) ([]JobRow, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, repo_id, account_id, action, state, started_at, completed_at,
		       exit_code, error, files_seen, files_changed, bytes_downloaded, api_calls,
		       schedule_id, trigger_key, updated_at
		FROM jobs ORDER BY started_at DESC LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []JobRow
	for rows.Next() {
		j, err := scanJob(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func scanJob(scan func(...any) error) (JobRow, error) {
	var j JobRow
	var completedAt sql.NullTime
	var exit sql.NullInt64
	var errStr, schedID, trig sql.NullString
	err := scan(&j.ID, &j.RepoID, &j.AccountID, &j.Action, &j.State, &j.StartedAt, &completedAt,
		&exit, &errStr, &j.FilesSeen, &j.FilesChanged, &j.BytesDownloaded, &j.APICalls,
		&schedID, &trig, &j.UpdatedAt)
	if err != nil {
		return j, err
	}
	if completedAt.Valid {
		j.CompletedAt = &completedAt.Time
	}
	if exit.Valid {
		v := int(exit.Int64)
		j.ExitCode = &v
	}
	j.Error = errStr.String
	j.ScheduleID = schedID.String
	j.TriggerKey = trig.String
	return j, nil
}

// IsNoRows is a convenience for the "not found" case.
func IsNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// RepoRow is the SQLite row shape for repos. JSON tags match the wire
// shape the controller frontend's Repo interface expects
// (gdrive-api.ts) so /repos, /ws/fleet, and /api/gdrive/<node>/* return
// snake_case keys the UI can consume without an extra mapping layer.
type RepoRow struct {
	ID                 string     `json:"id"`
	AccountID          string     `json:"account_id"`
	SourceFolderID     string     `json:"source_drive_folder_id"`
	SourceLabel        string     `json:"source_drive_label"`
	DestLocalPath      string     `json:"dest_local_path"`
	KeepVersions       int        `json:"keep_versions"`
	GraveyardKeepDays  int        `json:"graveyard_keep_days"`
	ConditionalGet     bool       `json:"conditional_get"`
	PullDriveRevisions bool       `json:"pull_drive_revisions"`
	FilterIncludeMime  string     `json:"filter_include_mime"` // CSV
	FilterExcludePaths string     `json:"filter_exclude_paths"` // CSV
	LastBackupAt       *time.Time `json:"last_backup_at,omitempty"`
	LastBackupJobID    string     `json:"last_backup_job_id,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

// JobRow is the SQLite row shape for jobs. JSON tags match the wire
// shape the controller frontend's Job interface expects
// (gdrive-api.ts) — the useGdriveFleet hook filters activeJobs by
// j.state === 'running', so without snake_case keys here the UI sees
// every state field as undefined and renders zero active jobs.
type JobRow struct {
	ID              string     `json:"id"`
	RepoID          string     `json:"repo_id"`
	AccountID       string     `json:"account_id"`
	Action          string     `json:"action"`
	State           string     `json:"state"`
	StartedAt       time.Time  `json:"started_at"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	ExitCode        *int       `json:"exit_code,omitempty"`
	Error           string     `json:"error,omitempty"`
	FilesSeen       int        `json:"files_seen"`
	FilesChanged    int        `json:"files_changed"`
	BytesDownloaded int64      `json:"bytes_downloaded"`
	APICalls        int        `json:"api_calls"`
	// TotalFiles is the in-scope file count known after collectFiles
	// finishes paging Drive's files.list. Used by the UI to compute
	// progress percent (FilesChanged/TotalFiles). Transient — not
	// persisted to SQLite (no schema column), only ever populated on
	// in-flight JobHandles via the runner.
	TotalFiles      int        `json:"total_files"`
	ScheduleID      string     `json:"schedule_id,omitempty"`
	TriggerKey      string     `json:"trigger_key,omitempty"`
	UpdatedAt       time.Time  `json:"updated_at"`
}
