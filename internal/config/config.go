package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// Config captures every operator-tunable parameter for the agent. All values
// arrive via environment variables — no flag parsing, no config files. The
// deployment automation renders these into the container's env block.
type Config struct {
	// NodeToken is the plaintext bearer the agent attaches to its OUTBOUND
	// calls to the controller. The controller stores the SHA-256 of this and
	// verifies agent calls against it. Note the direction: inbound auth on the
	// agent is the mTLS-terminating proxy in front of it, so this token is
	// never checked on requests the agent receives.
	NodeToken string

	// ControlCenterURL is the absolute base URL the agent posts job events to
	// and pulls secret bundles from — e.g.
	// "https://controller.example.com:1443". Its host stays as the HTTP Host
	// header / TLS SNI on every outbound call even when TraefikDockerDNS
	// rewrites the dial target, so a fronting proxy can route on it.
	ControlCenterURL string

	// TraefikDockerDNS is the docker-network DNS name to dial INSTEAD of the
	// hostname in ControlCenterURL. When set (typically "traefik"), the HTTP
	// client rewrites every dial target to <TraefikDockerDNS>:<TraefikDialPort>
	// while keeping the URL's authority as the Host header / TLS SNI. That
	// lets the host's local reverse proxy match the controller hostname and
	// forward it on over its own client-certificate transport, so the agent
	// never depends on the host resolving the public hostname itself. Empty
	// disables the rewrite (direct dial).
	TraefikDockerDNS string

	// TraefikDialPort is the port on the resolved Traefik IP. Default "1443".
	TraefikDialPort string

	// BackupRoot is the on-disk root for every backed-up account's mirror.
	// Default /mnt/backups/gdrive. The host bind-mount provided by the
	// docker-compose unit must point here.
	BackupRoot string

	// SiteID identifies which site this agent serves — an opaque short label
	// (e.g. site-a). Stamped onto job-event posts so the controller doesn't
	// have to infer it from the incoming hostname.
	SiteID string

	// NodeName is the short name the controller uses to address this agent in
	// its /api/gdrive/:node/* route. Typically the host's role, e.g. "nas".
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
	nodeToken, err := loadNodeToken()
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		NodeToken:        nodeToken,
		ControlCenterURL: strings.TrimRight(strings.TrimSpace(os.Getenv("CONTROL_CENTER_URL")), "/"),
		TraefikDockerDNS: strings.TrimSpace(os.Getenv("TRAEFIK_DOCKER_DNS")),
		TraefikDialPort:  getEnv("TRAEFIK_DIAL_PORT", "1443"),
		BackupRoot:       getEnv("GDRIVE_BACKUP_ROOT", "/mnt/backups/gdrive"),
		SiteID:           strings.TrimSpace(os.Getenv("SITE_ID")),
		NodeName:         strings.TrimSpace(os.Getenv("NODE_NAME")),
		MaxRPS:           getFloat("GDRIVE_MAX_RPS", 10),
		MaxConcurrent:    getInt("GDRIVE_MAX_CONCURRENT", 8),
		ListenAddr:       getEnv("LISTEN_ADDR", ":8080"),
		LogLevel:         getEnv("LOG_LEVEL", "info"),
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
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}
	return cfg
}

func (c Config) validate() error {
	missing := []string{}
	if c.NodeToken == "" {
		missing = append(missing, "GDRIVE_NODE_TOKEN")
	}
	if c.ControlCenterURL == "" {
		missing = append(missing, "CONTROL_CENTER_URL")
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

// loadNodeToken reads the bearer token from BEARER_TOKEN_FILE if set,
// otherwise from GDRIVE_NODE_TOKEN env. The file path is the production
// shape (a mode-0600 bind-mount written by the deployment automation); the
// env-var form exists for local dev/test.
func loadNodeToken() (string, error) {
	if path := strings.TrimSpace(os.Getenv("BEARER_TOKEN_FILE")); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read BEARER_TOKEN_FILE=%s: %w", path, err)
		}
		return strings.TrimSpace(string(raw)), nil
	}
	return strings.TrimSpace(os.Getenv("GDRIVE_NODE_TOKEN")), nil
}

func getInt(k string, def int) int {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
