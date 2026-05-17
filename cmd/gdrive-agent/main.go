package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

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
	log.Info().
		Str("site", cfg.SiteID).
		Str("node", cfg.NodeName).
		Str("backup_root", cfg.BackupRoot).
		Float64("max_rps", cfg.MaxRPS).
		Int("max_concurrent", cfg.MaxConcurrent).
		Msg("gdrive-agent starting")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// SQLite store under <backup_root>/.agent/state.db so the agent's
	// operational state lives alongside the data it describes — no extra
	// host bind-mount required.
	dbPath := filepath.Join(cfg.BackupRoot, ".agent", "state.db")
	store, err := db.Open(ctx, dbPath)
	if err != nil {
		log.Fatal().Err(err).Msg("open sqlite")
	}
	defer store.Close()

	// Sweep orphaned running/pending jobs left over from a prior crash
	// or redeploy. A JobHandle + goroutine is runtime-bound; SQLite rows
	// stuck in "running" after a fresh boot are by definition dead. If
	// we leave them as-is they show up in the UI's activeJobs forever,
	// and operator clicks (Cancel, View Log) 404 because the runtime
	// has no in-memory handle to dispatch to.
	if n, err := store.MarkOrphanedJobsFailed(ctx); err != nil {
		log.Warn().Err(err).Msg("orphan job sweep failed")
	} else if n > 0 {
		log.Info().Int("count", n).Msg("marked orphaned jobs as failed (boot remediation)")
	}

	factory := drive.NewClientFactory()
	limiter := drive.NewAdaptiveLimiter(cfg.MaxRPS, cfg.MaxConcurrent)
	limiter.StartRamper(ctx)

	// Pooled HTTP client for outbound calls back to the router. The URL host
	// ("controller-api.{domain}") is preserved as the Host header so the
	// local docker Traefik can match its routing rule; when TRAEFIK_DOCKER_DNS
	// is set the dial target is rewritten to that container so we never have
	// to depend on the host's DNS resolving the public hostname correctly.
	// Mirrors duplicacy-agent-api/network.go:buildControlCenterClient.
	routerHTTP := newRouterClient(cfg)

	app := &router.App{
		Cfg:          cfg,
		Store:        store,
		Factory:      factory,
		RouterClient: routerHTTP,
	}
	app.SecretsFetcher = router.DefaultSecretsFetcher(cfg, routerHTTP)
	app.BrowseSecretsFetcher = router.DefaultBrowseSecretsFetcher(cfg, routerHTTP)

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
		log.Fatal().Err(err).Msg("scheduler init")
	}
	app.Scheduler = sched
	sched.Start(ctx)
	defer sched.Stop()

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
		log.Info().Str("addr", cfg.ListenAddr).Msg("HTTP server listening")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("HTTP server error")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info().Msg("shutdown signal received")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("HTTP shutdown error")
	}
}

// newRouterClient builds the pooled HTTP client used for /api/* calls back
// to the controller router. Two operating modes — same shape as
// duplicacy-agent-api/network.go:buildControlCenterClient:
//
//   - **Direct mode** (cfg.TraefikDockerDNS == ""): the URL host is dialed
//     normally. Used when the agent can resolve the public hostname itself
//     (e.g. tests, k3s-node hosts dialing cluster DNS).
//
//   - **Traefik-rewrite mode** (cfg.TraefikDockerDNS != ""): DialContext
//     ignores the URL's host:port and dials <TraefikDockerDNS>:<TraefikDialPort>
//     instead — the local docker Traefik container, resolved via the docker
//     network's embedded DNS. The HTTP Host header and TLS SNI stay as the
//     URL host (e.g. "controller-api.example.com:1443") so Traefik
//     matches the inbound rule and forwards into the cluster over its
//     site-specific mTLS ServersTransport.
func newRouterClient(cfg config.Config) *http.Client {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        20,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	}

	if cfg.TraefikDockerDNS != "" {
		traefikTarget := net.JoinHostPort(cfg.TraefikDockerDNS, cfg.TraefikDialPort)
		transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, traefikTarget)
		}
	} else {
		transport.DialContext = dialer.DialContext
	}

	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
	}
}

func setupLogging() {
	level := strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL")))
	zerolog.TimeFieldFormat = time.RFC3339
	switch level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
}

func loggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info().
			Str("method", c.Request.Method).
			Str("path", c.Request.URL.Path).
			Int("status", c.Writer.Status()).
			Dur("dur", time.Since(start)).
			Msg("http")
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
