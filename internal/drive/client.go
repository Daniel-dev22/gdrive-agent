package drive

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

// SecretBundle is the deserialized payload the agent fetches from the router
// at /api/gdrive/accounts/:id/secrets-for-node/:node. The router decrypts
// the per-account refresh_token + client_secret out of the DB before
// handing it over the mTLS tunnel; the agent holds it only in memory.
type SecretBundle struct {
	AccountID    string   `json:"account_id"`
	GoogleEmail  string   `json:"google_email"`
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret"`
	RefreshToken string   `json:"refresh_token"`
	AccessToken  string   `json:"access_token,omitempty"`
	TokenExpiry  string   `json:"token_expires_at,omitempty"` // RFC3339; advisory
	Scopes       []string `json:"scopes"`
}

// ClientFactory builds drive.Service instances backed by a shared HTTP/2
// transport. Reusing one *http.Client across every Service means one TLS
// session and one h2 connection pool per Google endpoint — the foundation
// of the "blazing-fast streaming downloads" pattern.
//
// The factory caches per-account TokenSource so refresh_token → access_token
// exchanges happen at most once per access-token lifetime per account, not
// once per Drive call.
type ClientFactory struct {
	httpClient *http.Client

	mu      sync.Mutex
	sources map[string]oauth2.TokenSource // accountID → live source
	configs map[string]*oauth2.Config     // accountID → exchange config
}

// NewClientFactory wires up the shared transport. The factory does not
// know about the AdaptiveLimiter — that lives at the call site (job/runner)
// because the limiter is per-job-pool, while the HTTP client is global.
func NewClientFactory() *ClientFactory {
	tr := &http.Transport{
		// Force HTTP/2 implicitly via ForceAttemptHTTP2. Go's stdlib enables
		// h2 on HTTPS by default, but ForceAttemptHTTP2=true makes the
		// behavior explicit and survives upstream toggling defaults.
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 25,
		IdleConnTimeout:     5 * time.Minute,
		TLSHandshakeTimeout: 10 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	return &ClientFactory{
		httpClient: &http.Client{
			Transport: tr,
			// No client-side timeout — individual Drive calls supply their
			// own context deadlines, and very long downloads (multi-GB)
			// would die under a global 30s timeout. The semaphore in the
			// limiter is the backstop against unbounded concurrency.
		},
		sources: map[string]oauth2.TokenSource{},
		configs: map[string]*oauth2.Config{},
	}
}

// ForBundle returns a drive.Service for the given account, refreshing the
// access token as needed. Cached across calls for the same accountID. On
// secret rotation (refresh_token changed in the router) the caller MUST
// invoke Invalidate(accountID) before the next ForBundle to force re-load.
func (f *ClientFactory) ForBundle(ctx context.Context, b SecretBundle) (*drive.Service, error) {
	if b.AccountID == "" || b.RefreshToken == "" || b.ClientID == "" || b.ClientSecret == "" {
		return nil, errors.New("drive: secret bundle missing required fields")
	}
	ts := f.tokenSource(b)
	httpClient := oauth2.NewClient(
		context.WithValue(ctx, oauth2.HTTPClient, f.httpClient),
		ts,
	)
	svc, err := drive.NewService(ctx,
		option.WithHTTPClient(httpClient),
		option.WithUserAgent("gdrive-agent/1.0"),
	)
	if err != nil {
		return nil, fmt.Errorf("drive.NewService: %w", err)
	}
	return svc, nil
}

// tokenSource returns a TokenSource cached per accountID. oauth2.ReuseTokenSource
// caches the access_token in memory until expiry, then transparently calls
// the underlying refresh source. The RefreshToken stays put; only the
// access_token rotates.
func (f *ClientFactory) tokenSource(b SecretBundle) oauth2.TokenSource {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ts, ok := f.sources[b.AccountID]; ok {
		return ts
	}

	scopes := b.Scopes
	if len(scopes) == 0 {
		scopes = []string{drive.DriveReadonlyScope}
	}
	cfg := &oauth2.Config{
		ClientID:     b.ClientID,
		ClientSecret: b.ClientSecret,
		Endpoint:     google.Endpoint,
		Scopes:       scopes,
	}
	tok := &oauth2.Token{
		RefreshToken: b.RefreshToken,
		AccessToken:  b.AccessToken,
		TokenType:    "Bearer",
	}
	// If the router supplied an expiry, parse it so oauth2 doesn't refresh
	// unnecessarily on the first call. Best-effort — bad timestamp just
	// means an extra refresh.
	if b.TokenExpiry != "" {
		if t, err := time.Parse(time.RFC3339, b.TokenExpiry); err == nil {
			tok.Expiry = t
		}
	}
	ts := oauth2.ReuseTokenSource(tok, cfg.TokenSource(context.Background(), tok))
	f.sources[b.AccountID] = ts
	f.configs[b.AccountID] = cfg
	return ts
}

// Invalidate drops the cached TokenSource for accountID. Next ForBundle
// call rebuilds it from a fresh bundle. Used by the router-driven cache-
// bust handler at /internal/credentials/:id/invalidate.
func (f *ClientFactory) Invalidate(accountID string) {
	f.mu.Lock()
	delete(f.sources, accountID)
	delete(f.configs, accountID)
	f.mu.Unlock()
}

// HTTPClient exposes the shared transport so other components (e.g. the
// range-resumable download in storage/atomic) can issue raw HTTP GETs
// against drive.googleapis.com without bypassing the connection pool.
func (f *ClientFactory) HTTPClient() *http.Client {
	return f.httpClient
}
