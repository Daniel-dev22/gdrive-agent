package main

import (
	"context"
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

	// Pooled HTTP client for outbound calls back to the router. The URL host
	// ("controller-api.{domain}") is preserved as the Host header so the
	// local docker Traefik can match its routing rule; when TRAEFIK_DOCKER_DNS
	// is set the dial target is rewritten to that container so we never have
	// to depend on the host's DNS resolving the public hostname correctly.
	// Shared with duplicacy-agent-api via agent-kit-go/transport (canonical
	// pool knobs were taken from gdrive: KeepAlive 30s, IdleConnTimeout 90s,
	// ForceAttemptHTTP2, dial 5s, MaxIdleConns 20, per-host 10, client 30s).
	// No bearer here — the plain router client; the event Outbox client below
	// wraps this transport to add the bearer.
	routerHTTP := transport.NewRouterClient(transport.RouterClientConfig{
		TraefikDockerDNS: cfg.TraefikDockerDNS,
		TraefikDialPort:  cfg.TraefikDialPort,
	})

	app := &router.App{
		Cfg:          cfg,
		Store:        store,
		Factory:      factory,
		RouterClient: routerHTTP,
	}
	app.SecretsFetcher = router.DefaultSecretsFetcher(cfg, routerHTTP)
	app.BrowseSecretsFetcher = router.DefaultBrowseSecretsFetcher(cfg, routerHTTP)

	// Durable outbound event queue (agent-kit-go). Job lifecycle events used
	// to be pushed fire-and-forget from the EventEmitter, so a transient
	// controller outage or an agent restart could leave a central
	// gdrive_jobs row stuck "running". The Outbox persists each event to a
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

	// WS endpoints. Inbound auth is the mTLS tunnel terminated by the host
	// Traefik (per-site client cert); no application-layer bearer is required
	// on this direction.
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
	fmt.Println(`gdrive-agent — Control Center Google Drive backup agent

Usage: gdrive-agent [--help|--version]

Required env vars:
  GDRIVE_NODE_TOKEN              Bearer token the agent attaches on outbound calls to controller
  LOCAL_TLS_DOMAIN               e.g. example.com
  CONTROL_CENTER_URL             e.g. https://controller-api.example.com:1443
  TRAEFIK_DOCKER_DNS             e.g. traefik (rewrite dial target to local docker Traefik; mirrors duplicacy)
  TRAEFIK_DIAL_PORT              default 1443
  SITE_ID                        kd | ng
  NODE_NAME                      e.g. nas

Optional env vars:
  GDRIVE_BACKUP_ROOT             default /mnt/backups/gdrive
  GDRIVE_MAX_RPS                 default 10
  GDRIVE_MAX_CONCURRENT          default 8
  GDRIVE_RESTORE_ALLOWED_ROOTS   CSV; default = backup root
  LISTEN_ADDR                    default :8080
  LOG_LEVEL                      debug|info|warn|error (default info)

Endpoints:
  POST   /repos/init                                  Bootstrap repo binding
  POST   /repos/bind                                  Idempotent rebind
  GET    /repos                                       List bound repos
  GET    /repos/:id
  DELETE /repos/:id
  POST   /internal/credentials/:id/invalidate         Cache bust
  POST   /schedules/refresh                           Schedule re-pull nudge
  POST   /backup                                      Start backup job
  POST   /restore                                     Start restore job
  POST   /jobs/:id/cancel
  GET    /jobs
  GET    /jobs/:id
  GET    /snapshots/:repo                             Version list
  GET    /snapshots/:repo/:revision/files             File tree at revision
  GET    /files/:repo/:version/*path                  Stream a file version
  GET    /drive/folders?q=&account_id=&parent=        Drive folder picker
  GET    /ws/fleet                                    Snapshot push WS
  GET    /ws/jobs/:id/logs                            Live log tail WS
  GET    /health/live  /health/ready                  Probes (no auth)`)
}
