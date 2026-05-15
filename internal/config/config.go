package config

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

// Config captures every operator-tunable parameter for the agent. All values
// arrive via environment variables — no flag parsing, no config files. The
// ansible role that deploys the agent renders these into the docker-compose
// env block.
type Config struct {
	// NodeToken is the plaintext bearer presented by the router on every
	// authenticated HTTP call. The router stores the SHA-256 of this in the
	// gdrive_node_tokens table; the agent compares the incoming header to
	// this plaintext via crypto/subtle.ConstantTimeCompare.
	NodeToken string

	// LocalTLSDomain feeds the agent's redirect-URI computation and the host
	// header that local Traefik expects to see. e.g. "example.com".
	LocalTLSDomain string

	// RouterBaseURL is the absolute URL the agent posts job events and pulls
	// secret bundles from. e.g. "https://controller.example.com".
	RouterBaseURL string

	// BackupRoot is the on-disk root for every backed-up account's mirror.
	// Default /mnt/backups/gdrive. The host bind-mount provided by the
	// docker-compose unit must point here.
	BackupRoot string

	// SiteID identifies which Control Center site this agent serves. One of
	// "kd" | "ng". Stamped onto job-event posts so the router doesn't have
	// to infer it from the incoming hostname.
	SiteID string

	// NodeName is the short name the router uses to address this agent in
	// the /api/gdrive/:node/* route. Typically "nas".
	NodeName string

	// MaxRPS is the steady-state ceiling for Drive API requests/sec across
	// all jobs. AdaptiveLimiter starts here, halves on 429, ramps back up.
	MaxRPS float64

	// MaxConcurrent caps in-flight Drive requests across all jobs (separate
	// from RPS so we don't burst N requests in the same wallclock second).
	MaxConcurrent int

	// RestoreAllowedRoots is the CSV allowlist of absolute path prefixes a
	// "custom" restore target may write into. Defaults to BackupRoot alone.
	// Validated by job/runner.go before any IO happens.
	RestoreAllowedRoots []string

	// ListenAddr is where Gin binds. Internal — never set by operators.
	ListenAddr string

	// LogLevel — debug | info | warn | error. Default info.
	LogLevel string
}

// Load reads the environment and validates required values. Missing required
// variables cause a fatal log (process exits non-zero) rather than running
// half-configured.
func Load() (Config, error) {
	cfg := Config{
		NodeToken:      strings.TrimSpace(os.Getenv("GDRIVE_NODE_TOKEN")),
		LocalTLSDomain: strings.TrimSpace(os.Getenv("LOCAL_TLS_DOMAIN")),
		RouterBaseURL:  strings.TrimRight(strings.TrimSpace(os.Getenv("ROUTER_BASE_URL")), "/"),
		BackupRoot:     getEnv("GDRIVE_BACKUP_ROOT", "/mnt/backups/gdrive"),
		SiteID:         strings.TrimSpace(os.Getenv("SITE_ID")),
		NodeName:       strings.TrimSpace(os.Getenv("NODE_NAME")),
		MaxRPS:         getFloat("GDRIVE_MAX_RPS", 10),
		MaxConcurrent:  getInt("GDRIVE_MAX_CONCURRENT", 8),
		ListenAddr:     getEnv("LISTEN_ADDR", ":8080"),
		LogLevel:       getEnv("LOG_LEVEL", "info"),
	}

	// Restore allowlist defaults to the backup root if unset. This means a
	// fresh install will only allow restores back under /mnt/backups/gdrive
	// — operators must explicitly opt-in to writing anywhere else.
	allowed := strings.TrimSpace(os.Getenv("GDRIVE_RESTORE_ALLOWED_ROOTS"))
	if allowed == "" {
		cfg.RestoreAllowedRoots = []string{cfg.BackupRoot}
	} else {
		for _, p := range strings.Split(allowed, ",") {
			if p = strings.TrimSpace(p); p != "" {
				cfg.RestoreAllowedRoots = append(cfg.RestoreAllowedRoots, p)
			}
		}
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// MustLoad calls Load and fatally exits the process on error. Used in main.
func MustLoad() Config {
	cfg, err := Load()
	if err != nil {
		log.Fatal().Err(err).Msg("config load failed")
	}
	return cfg
}

func (c Config) validate() error {
	missing := []string{}
	if c.NodeToken == "" {
		missing = append(missing, "GDRIVE_NODE_TOKEN")
	}
	if c.LocalTLSDomain == "" {
		missing = append(missing, "LOCAL_TLS_DOMAIN")
	}
	if c.RouterBaseURL == "" {
		missing = append(missing, "ROUTER_BASE_URL")
	}
	if c.SiteID == "" {
		missing = append(missing, "SITE_ID")
	}
	if c.NodeName == "" {
		missing = append(missing, "NODE_NAME")
	}
	if c.MaxRPS <= 0 {
		return errors.New("GDRIVE_MAX_RPS must be > 0")
	}
	if c.MaxConcurrent <= 0 {
		return errors.New("GDRIVE_MAX_CONCURRENT must be > 0")
	}
	if len(missing) > 0 {
		return errors.New("missing required env vars: " + strings.Join(missing, ", "))
	}
	return nil
}

func getEnv(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func getFloat(k string, def float64) float64 {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func getInt(k string, def int) int {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
