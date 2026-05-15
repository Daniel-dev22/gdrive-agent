package fleet

import (
	"context"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/Daniel-dev22/gdrive-agent/internal/db"
	"github.com/Daniel-dev22/gdrive-agent/internal/job"
)

// HostInfo carries static facts about the host. Computed once at hub
// creation; emitted with every snapshot for UI panels that show node
// identity + sizing.
type HostInfo struct {
	Hostname    string `json:"hostname"`
	NumCPU      int    `json:"num_cpu"`
	SiteID      string `json:"site_id"`
	NodeName    string `json:"node_name"`
	BackupRoot  string `json:"backup_root"`
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
type LiveJob struct {
	ID              string `json:"id"`
	RepoID          string `json:"repo_id"`
	AccountID       string `json:"account_id"`
	Action          string `json:"action"`
	State           string `json:"state"`
	FilesSeen       int    `json:"files_seen"`
	FilesChanged    int    `json:"files_changed"`
	BytesDownloaded int64  `json:"bytes_downloaded"`
	APICalls        int    `json:"api_calls"`
	StartedAt       time.Time `json:"started_at"`
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
	jobs, err := b.Runner.SnapshotJobs(ctx, 50)
	if err != nil {
		return Snapshot{}, err
	}
	live := []LiveJob{}
	for _, h := range b.Runner.ListLive() {
		live = append(live, LiveJob{
			ID:              h.ID,
			RepoID:          h.Row.RepoID,
			AccountID:       h.Row.AccountID,
			Action:          h.Row.Action,
			State:           h.Row.State,
			FilesSeen:       h.Row.FilesSeen,
			FilesChanged:    h.Row.FilesChanged,
			BytesDownloaded: h.Row.BytesDownloaded,
			APICalls:        h.Row.APICalls,
			StartedAt:       h.Row.StartedAt,
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

// Hub coalesces snapshot triggers onto a single broadcast goroutine.
// Producers (runner state transitions, 5s tick) call Trigger(); the hub
// builds at most one snapshot per coalescing window. Pattern mirrors
// duplicacy-agent-api's fleetHub.
type Hub struct {
	builder *Builder

	mu     sync.RWMutex
	subs   map[chan Snapshot]struct{}

	trigger chan struct{} // buffered 1 — coalescing
}

func NewHub(builder *Builder) *Hub {
	return &Hub{
		builder: builder,
		subs:    map[chan Snapshot]struct{}{},
		trigger: make(chan struct{}, 1),
	}
}

// Trigger asks the hub to compute and broadcast a fresh snapshot. Non-
// blocking — duplicate triggers within the build window are dropped.
func (h *Hub) Trigger() {
	select {
	case h.trigger <- struct{}{}:
	default:
	}
}

// Run is the broadcaster loop. Cancel the context to terminate.
func (h *Hub) Run(ctx context.Context) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-h.trigger:
		}
		snap, err := h.builder.Build(ctx)
		if err != nil {
			continue
		}
		h.broadcast(snap)
	}
}

func (h *Hub) broadcast(snap Snapshot) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.subs {
		select {
		case c <- snap:
		default:
			// Slow consumer — drop. WS layer will detect via close on
			// disconnect, no extra cleanup needed here.
		}
	}
}

// Subscribe returns a channel + unsubscribe. Caller MUST call unsubscribe
// (typically in a defer) or the hub will retain a reference.
func (h *Hub) Subscribe() (chan Snapshot, func()) {
	c := make(chan Snapshot, 4)
	h.mu.Lock()
	h.subs[c] = struct{}{}
	h.mu.Unlock()
	return c, func() {
		h.mu.Lock()
		if _, ok := h.subs[c]; ok {
			delete(h.subs, c)
			close(c)
		}
		h.mu.Unlock()
	}
}

// SnapshotNow builds one snapshot synchronously without broadcasting. Used
// to greet new WS subscribers so they don't have to wait up to 5s for
// first paint.
func (h *Hub) SnapshotNow(ctx context.Context) (Snapshot, error) {
	return h.builder.Build(ctx)
}
