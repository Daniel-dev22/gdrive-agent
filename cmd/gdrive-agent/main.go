package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Daniel-dev22/agent-kit-go/eventoutbox"
	"github.com/Daniel-dev22/agent-kit-go/jobstore"
	"github.com/Daniel-dev22/agent-kit-go/logging"
	"github.com/Daniel-dev22/agent-kit-go/memlimit"
	"github.com/Daniel-dev22/agent-kit-go/reconcile"
	"github.com/Daniel-dev22/agent-kit-go/transport"
	"github.com/gin-gonic/gin"

	"github.com/Daniel-dev22/gdrive-agent/internal/config"
	"github.com/Daniel-dev22/gdrive-agent/internal/db"
	"github.com/Daniel-dev22/gdrive-agent/internal/drive"
	"github.com/Daniel-dev22/gdrive-agent/internal/fleet"
	"github.com/Daniel-dev22/gdrive-agent/internal/job"
	"github.com/Daniel-dev22/gdrive-agent/internal/router"
	"github.com/Daniel-dev22/gdrive-agent/internal/schedules"
	"github.com/Daniel-dev22/gdrive-agent/internal/ws"
)

// version is overridden via -ldflags=-X at build time. Kept here so
// --help and the /health/ready response can include it.
var version = "dev"

func main() {
	helpFlag := flag.Bool("help", false, "print help and exit")
	versionFlag := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *helpFlag {
		printHelp()
		return
	}
	if *versionFlag {
		fmt.Println(version)
		return
	}

	setupLogging()

	cfg := config.MustLoad()
	slog.Info("gdrive-agent starting",
		"site", cfg.SiteID,
		"node", cfg.NodeName,
		"backup_root", cfg.BackupRoot,
		"max_rps", cfg.MaxRPS,
		"max_concurrent", cfg.MaxConcurrent)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Before the database opens or any server starts; the refresher stops with ctx.
	anon := setupMemLimit(ctx)

	// SQLite store under <backup_root>/.agent/state.db so the agent's
	// operational state lives alongside the data it describes — no extra
	// host bind-mount required.
	dbPath := filepath.Join(cfg.BackupRoot, ".agent", "state.db")
	store, err := db.Open(ctx, dbPath)
	if err != nil {
		slog.Error("open sqlite", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	// Sweep orphaned running/pending jobs left over from a prior crash
	// or redeploy. A JobHandle + goroutine is runtime-bound; SQLite rows
	// stuck in "running" after a fresh boot are by definition dead. If
	// we leave them as-is they show up in the UI's activeJobs forever,
	// and operator clicks (Cancel, View Log) 404 because the runtime
	// has no in-memory handle to dispatch to.
	if n, err := store.MarkOrphanedJobsFailed(ctx); err != nil {
		slog.Warn("orphan job sweep failed", "error", err)
	} else if n > 0 {
		slog.Info("marked orphaned jobs as failed (boot remediation)", "count", n)
	}

	factory := drive.NewClientFactory()
	limiter := drive.NewAdaptiveLimiter(cfg.MaxRPS, cfg.MaxConcurrent)
	limiter.StartRamper(ctx)

	// Pooled HTTP client for outbound calls back to the controller. The host
	// from CONTROL_CENTER_URL is preserved as the Host header so the local
	// reverse proxy can match its routing rule; when TRAEFIK_DOCKER_DNS is set
	// the dial target is rewritten to that container so we never have to
	// depend on the host's DNS resolving the public hostname correctly.
	// Shared with duplicacy-agent-api via agent-kit-go/transport (canonical
	// pool knobs were taken from gdrive: KeepAlive 30s, IdleConnTimeout 90s,
	// ForceAttemptHTTP2, dial 5s, MaxIdleConns 20, per-host 10, client 30s).
	// No bearer here — the plain client; the event Outbox client below wraps
	// this transport to add the bearer.
	routerHTTP := transport.NewRouterClient(transport.RouterClientConfig{
		TraefikDockerDNS: cfg.TraefikDockerDNS,
		TraefikDialPort:  cfg.TraefikDialPort,
	})

	app := &router.App{
		Cfg:          cfg,
		Store:        store,
		Factory:      factory,
		RouterClient: routerHTTP,
		Anon:         anon,
	}
	app.SecretsFetcher = router.DefaultSecretsFetcher(cfg, routerHTTP)
	app.BrowseSecretsFetcher = router.DefaultBrowseSecretsFetcher(cfg, routerHTTP)

	// Durable outbound event queue (agent-kit-go). Job lifecycle events used
	// to be pushed fire-and-forget from the EventEmitter, so a transient
	// controller outage or an agent restart could leave the controller's job
	// row stuck "running". The Outbox persists each event to a
	// pending_events table on the same SQLite handle, fires an immediate
	// best-effort POST, and retries the backlog every 30s. It POSTs to the
	// same /api/gdrive/jobs/:id/event endpoint with the same flat JSON body;
	// the bearer is attached by outboxHTTP's transport-layer round-tripper so
	// the request the router receives is identical to the old per-request one.
	// The Outbox client reuses routerHTTP's pooled, dial-rewritten transport
	// (same connection pool + Traefik behavior) wrapped in the shared bearer
	// round-tripper from agent-kit-go/transport, and the same Timeout — this is
	// byte-for-byte the old local newOutboxClient/bearerAuthRoundTripper pair.
	outboxHTTP := &http.Client{
		Timeout:   routerHTTP.Timeout,
		Transport: transport.NewBearerRoundTripper(routerHTTP.Transport, cfg.NodeToken),
	}
	urlFor := func(id string) string {
		return cfg.ControlCenterURL + "/api/gdrive/jobs/" + url.PathEscape(id) + "/event"
	}
	outbox, err := eventoutbox.New(store.DB, outboxHTTP, urlFor)
	if err != nil {
		slog.Error("eventoutbox init", "error", err)
		os.Exit(1)
	}
	app.Outbox = outbox
	outbox.Start(ctx)
	defer outbox.Close()

	// Two-phase construction: the runner's OnChange callback wants to
	// point at hub.Trigger, but the hub's Builder wants to point at the
	// runner. We solve the cycle by deferring the OnChange wiring via a
	// closure that captures `hub` by pointer; assignment to hub happens
	// before any runner method runs so there's no read-after-nil hazard.
	var hub *fleet.Hub
	runner := job.New(job.Config{
		Store:               store,
		Factory:             factory,
		Limiter:             limiter,
		BackupRoot:          cfg.BackupRoot,
		RestoreAllowedRoots: cfg.RestoreAllowedRoots,
		SecretsFetcher:      app.SecretsFetcher,
		EventEmitter:        app.EventEmitter(),
		OnChange:            func() { hub.Trigger() },
	})
	app.Runner = runner

	builder := &fleet.Builder{
		Store:      store,
		Runner:     runner,
		SiteID:     cfg.SiteID,
		NodeName:   cfg.NodeName,
		BackupRoot: cfg.BackupRoot,
	}
	hub = fleet.NewHub(builder)
	app.Fleet = hub

	go hub.Run(ctx)

	// Per-host cron scheduler (agent-kit-go). Pulls schedules from
	// <CONTROL_CENTER_URL>/api/gdrive/schedules?node=<self> every 5 min,
	// fires matching schedules every minute via runner.StartBackup. Disk
	// cache survives pod restart so a restart in the middle of a fire
	// minute does not double-fire and a long outage does not flood with
	// catch-up runs (MissPolicy enforced by the kit).
	sched, err := schedules.New(cfg, routerHTTP, runner)
	if err != nil {
		slog.Error("scheduler init", "error", err)
		os.Exit(1)
	}
	app.Scheduler = sched
	sched.Start(ctx)
	defer sched.Stop()

	// Declarative job-state reconcile: on startup + every 5 min, POST this
	// node's full authoritative job set (non-terminal + last-24h terminals,
	// from the local jobs table) to /api/gdrive/reconcile so the controller
	// converges its gdrive_jobs rows. Safety net for a lost/out-of-order
	// terminal event that would otherwise strand a row 'running'. Reuses the
	// bearer-wrapped outbox client; stateless POST.
	go reconcile.Run(ctx, reconcile.Config{
		Client:   outboxHTTP,
		URL:      cfg.ControlCenterURL + "/api/gdrive/reconcile",
		Site:     cfg.SiteID,
		Node:     cfg.NodeName,
		Interval: 5 * time.Minute,
		Snapshot: func(sctx context.Context) ([]jobstore.JobRef, error) {
			return jobstore.ListForReconcile(sctx, store.DB, jobstore.ListOptions{
				Table:                   "jobs",
				ExitCodeColumn:          "exit_code",
				RecentTerminalPredicate: "completed_at >= ?",
				RecentTerminalArg:       time.Now().Add(-24 * time.Hour).UTC(),
			})
		},
	})

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(loggerMiddleware())

	app.RegisterRoutes(r)

	// WS endpoints. Like every other route, these carry NO application-layer
	// authentication — the agent assumes an mTLS-terminating reverse proxy in
	// front of it. Both upgraders also accept any Origin. See the security
	// section of README.md before exposing this listener directly.
	r.GET("/ws/fleet", ws.FleetHandler(hub))
	r.GET("/ws/jobs/:id/logs", ws.JobLogHandler(runner))

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout — streaming downloads of multi-GB files would
		// time out spuriously. Per-connection idle is bounded by the
		// HTTP/2 transport on the read side.
	}
	go func() {
		slog.Info("HTTP server listening", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("shutdown signal received")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP shutdown error", "error", err)
	}
}

func setupLogging() {
	logging.Setup(os.Getenv("LOG_LEVEL"))
}

// setupMemLimit derives the Go soft memory limit from the container cgroup
// limit (85%) so the GC tightens before the kernel OOM-killer acts, and starts
// the cgroup anon-memory sampler exposed on /health/ready. It runs before any
// server or worker starts. An invalid configuration is fatal (the agent would
// otherwise run uncapped by the Go runtime under a hard cgroup cap); an
// environmental failure is logged and retried by the refresher.
func setupMemLimit(ctx context.Context) *memlimit.PeakAnon {
	if _, err := memlimit.Apply(ctx, slog.Default()); err != nil {
		if errors.Is(err, memlimit.ErrInvalidConfig) {
			slog.Error("memlimit: invalid configuration", "error", err)
			os.Exit(1)
		}
		slog.Warn("memlimit: not applied", "error", err)
	}
	return memlimit.StartPeakAnon(ctx, slog.Default(), memlimit.PeakOptions{})
}

func loggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		slog.Info("http",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"dur", time.Since(start))
	}
}

func printHelp() {
	fmt.Println(`gdrive-agent — single-host Google Drive backup agent

Usage: gdrive-agent [--help|--version]

WARNING: no route is authenticated at the application layer. The agent
expects an mTLS-terminating reverse proxy in front of it. See README.md.

Required env vars:
  GDRIVE_NODE_TOKEN              Bearer token attached to OUTBOUND calls to the controller
                                 (or supply it via BEARER_TOKEN_FILE instead)
  CONTROL_CENTER_URL             Controller base URL, e.g. https://controller.example.com:1443
  SITE_ID                        Site label stamped on outbound events, e.g. site-a
  NODE_NAME                      This host's name in the controller's /api/gdrive/:node/* route, e.g. nas

Optional env vars:
  BEARER_TOKEN_FILE              Path to a file holding the bearer token; takes precedence
                                 over GDRIVE_NODE_TOKEN when set
  TRAEFIK_DOCKER_DNS             e.g. traefik — rewrite the outbound dial target to this
                                 container, keeping the Host header / SNI. Empty = direct dial
  TRAEFIK_DIAL_PORT              default 1443 — port used with TRAEFIK_DOCKER_DNS
  GDRIVE_BACKUP_ROOT             default /mnt/backups/gdrive
  GDRIVE_MAX_RPS                 default 10
  GDRIVE_MAX_CONCURRENT          default 8
  GDRIVE_RESTORE_ALLOWED_ROOTS   CSV of absolute prefixes a custom restore may write into;
                                 default = backup root
  LISTEN_ADDR                    default :8080
  LOG_LEVEL                      debug|info|warn|error (default info)

Endpoints:
  GET    /health/live                                 Liveness (HEAD alias too)
  GET    /health/ready                                Readiness — pings SQLite (HEAD alias too)
  POST   /repos/init                                  Bootstrap repo binding
  POST   /repos/bind                                  Idempotent rebind (same body as init)
  GET    /repos                                       List bound repos
  GET    /repos/:id                                   One repo
  GET    /repos/:id/stats                             Cached mirror stats (204 if never backed up)
  DELETE /repos/:id                                   Drop the binding (leaves data on disk)
  POST   /internal/credentials/:id/invalidate         Drop the cached OAuth token source
  POST   /schedules/refresh                           Pull schedules from the controller now
  POST   /backup                                      Start backup job
  POST   /restore                                     Start restore job
  POST   /jobs/:id/cancel                             Cancel a running job
  GET    /jobs                                        100 most recent jobs
  GET    /jobs/:id                                    One job
  GET    /snapshots/:repo/:revision/files             Mirrored file tree
  GET    /revisions/:repo/:file_id                    Rotated version history for one file
  GET    /files/:repo/:version/*path                  Stream a mirrored file (path = file id)
  GET    /drive/folders?q=&account_id=&parent=        Drive folder picker
  GET    /ws/fleet                                    Snapshot push WS
  GET    /ws/jobs/:id/logs                            Live log tail WS`)
}
