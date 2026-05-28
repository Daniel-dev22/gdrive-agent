package fleet

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"time"

	kitfleet "github.com/Daniel-dev22/agent-kit-go/fleet"
	"github.com/Daniel-dev22/gdrive-agent/internal/db"
	"github.com/Daniel-dev22/gdrive-agent/internal/job"
)

// HostInfo carries static facts about the host. Computed once at hub
// creation; emitted with every snapshot for UI panels that show node
// identity + sizing.
type HostInfo struct {
	Hostname   string `json:"hostname"`
	NumCPU     int    `json:"num_cpu"`
	SiteID     string `json:"site_id"`
	NodeName   string `json:"node_name"`
	BackupRoot string `json:"backup_root"`
}

// Snapshot is the unit broadcast on /ws/fleet.
type Snapshot struct {
	Host  HostInfo     `json:"host"`
	Repos []db.RepoRow `json:"repos"`
	Jobs  []db.JobRow  `json:"jobs"`
	Live  []LiveJob    `json:"live_jobs"`
	Time  time.Time    `json:"time"`
}

// LiveJob is the snapshot-friendly subset of an in-flight job. Detailed
// log lines stream separately via /ws/jobs/:id/logs — the fleet WS only
// needs to convey "what's running right now" for the dashboard tile.
//
// TotalFiles is the in-scope file count discovered by collectFiles().
// Zero until the file-listing phase finishes (Drive files.list paging
// can take 10-30s for large drives). The frontend derives progress
// percent from FilesChanged/TotalFiles; renders an indeterminate bar
// while TotalFiles is 0.
type LiveJob struct {
	ID                      string    `json:"id"`
	RepoID                  string    `json:"repo_id"`
	AccountID               string    `json:"account_id"`
	Action                  string    `json:"action"`
	State                   string    `json:"state"`
	FilesSeen               int       `json:"files_seen"`
	FilesChanged            int       `json:"files_changed"`
	FilesNew                int       `json:"files_new"`
	FilesVersioned          int       `json:"files_versioned"`
	FilesSkippedMeta        int       `json:"files_skipped_meta"`
	FilesSkippedNotModified int       `json:"files_skipped_not_modified"`
	FilesDeleted            int       `json:"files_deleted"`
	BytesDownloaded         int64     `json:"bytes_downloaded"`
	APICalls                int       `json:"api_calls"`
	TotalFiles              int       `json:"total_files"`
	StartedAt               time.Time `json:"started_at"`
}

// Builder is the deps bundle the snapshot collector uses. The runner is
// the source of truth for live jobs; the SQLite store is the source of
// truth for persisted repos + recent terminal jobs.
type Builder struct {
	Store      *db.Store
	Runner     *job.Runner
	SiteID     string
	NodeName   string
	BackupRoot string
}

// Build collects a fresh Snapshot. Pure cache reads — never spawn a Drive
// API call from here. The fleet broadcaster fires this on a 5s tick plus
// every onChange callback from the runner, so end-to-end latency from
// "job state changed" to "browser saw it" is sub-second.
func (b *Builder) Build(ctx context.Context) (Snapshot, error) {
	hn, _ := os.Hostname()
	host := HostInfo{
		Hostname:   hn,
		NumCPU:     runtime.NumCPU(),
		SiteID:     b.SiteID,
		NodeName:   b.NodeName,
		BackupRoot: b.BackupRoot,
	}
	repos, err := b.Store.ListRepos(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	// Always recompute dest_local_path from the live BACKUP_ROOT — the
	// SQLite value can be stale after the operator moves the backup pool
	// (the row's path was captured at /repos/init time and never updated).
	// The runner itself uses filepath.Join(BACKUP_ROOT, AccountID), so
	// rewriting the wire shape keeps the UI in sync with reality.
	for i := range repos {
		repos[i].DestLocalPath = filepath.Join(b.BackupRoot, repos[i].AccountID)
	}
	jobs, err := b.Runner.SnapshotJobs(ctx, 50)
	if err != nil {
		return Snapshot{}, err
	}
	live := []LiveJob{}
	for _, h := range b.Runner.ListLive() {
		live = append(live, LiveJob{
			ID:                      h.ID,
			RepoID:                  h.Row.RepoID,
			AccountID:               h.Row.AccountID,
			Action:                  h.Row.Action,
			State:                   h.Row.State,
			FilesSeen:               h.Row.FilesSeen,
			FilesChanged:            h.Row.FilesChanged,
			FilesNew:                h.Row.FilesNew,
			FilesVersioned:          h.Row.FilesVersioned,
			FilesSkippedMeta:        h.Row.FilesSkippedMeta,
			FilesSkippedNotModified: h.Row.FilesSkippedNotModified,
			FilesDeleted:            h.Row.FilesDeleted,
			BytesDownloaded:         h.Row.BytesDownloaded,
			APICalls:                h.Row.APICalls,
			TotalFiles:              h.Row.TotalFiles,
			StartedAt:               h.Row.StartedAt,
		})
	}
	return Snapshot{
		Host:  host,
		Repos: repos,
		Jobs:  jobs,
		Live:  live,
		Time:  time.Now().UTC(),
	}, nil
}

// Hub is the shared agent-kit-go coalescing snapshot broadcaster, specialized
// to this agent's Snapshot payload. The hand-rolled hub that used to live here
// now comes from the kit so duplicacy-agent-api and gdrive-agent share ONE
// correct implementation (guarded close, slow-consumer drop, no self-
// unregister). See agent-kit-go/fleet.
type Hub = kitfleet.Hub[Snapshot]

// NewHub builds the fleet hub from a Builder, rebuilding every 5s for liveness
// in addition to on-demand Trigger() calls from runner state transitions.
func NewHub(builder *Builder) *Hub {
	return kitfleet.NewHub(builder.Build, 5*time.Second)
}
