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

	"github.com/Daniel-dev22/gdrive-agent/internal/auth"
	"github.com/Daniel-dev22/gdrive-agent/internal/config"
	"github.com/Daniel-dev22/gdrive-agent/internal/db"
	"github.com/Daniel-dev22/gdrive-agent/internal/drive"
	"github.com/Daniel-dev22/gdrive-agent/internal/fleet"
	"github.com/Daniel-dev22/gdrive-agent/internal/job"
	"github.com/Daniel-dev22/gdrive-agent/internal/router"
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

	factory := drive.NewClientFactory()
	limiter := drive.NewAdaptiveLimiter(cfg.MaxRPS, cfg.MaxConcurrent)
	limiter.StartRamper(ctx)

	// Pooled HTTP client for outbound calls back to the router. Plain HTTPS
	// — the NAS's local Traefik handles cert pinning at the host edge, the
	// agent doesn't need to know.
	routerHTTP := newRouterClient()

	app := &router.App{
		Cfg:          cfg,
		Store:        store,
		Factory:      factory,
		RouterClient: routerHTTP,
	}
	app.SecretsFetcher = router.DefaultSecretsFetcher(cfg, routerHTTP)

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

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(loggerMiddleware())

	bearer := auth.RequireBearer(cfg.NodeToken)
	app.RegisterRoutes(r, bearer)

	// WS endpoints — both require bearer auth via Sec-WebSocket-Protocol or
	// query string is NOT used; the standard Authorization header is sent
	// by the router-side proxy, so the normal bearer middleware works.
	r.GET("/ws/fleet", bearer, ws.FleetHandler(hub))
	r.GET("/ws/jobs/:id/logs", bearer, ws.JobLogHandler(runner))

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
// to the controller router. Standard internal-service preset: short
// dial, long keepalive, no global timeout (long-running secrets-vend calls
// must not be clipped).
func newRouterClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			ForceAttemptHTTP2:   true,
			MaxIdleConns:        20,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
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
  GDRIVE_NODE_TOKEN              Bearer token presented by controller router
  LOCAL_TLS_DOMAIN               e.g. example.com
  ROUTER_BASE_URL                e.g. https://controller.example.com
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
