package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Daniel-dev22/agent-kit-go/transport"
	"github.com/Daniel-dev22/gdrive-agent/internal/config"
	"github.com/Daniel-dev22/gdrive-agent/internal/drive"
	"github.com/Daniel-dev22/gdrive-agent/internal/router"
)

// The secrets vend path: main builds a TOKENLESS router client (newRouterHTTP)
// and the fetcher sets its own Authorization. A redirect to another origin must
// be refused, otherwise the agent would decode the redirect target's JSON as the
// account's Drive credentials.
func TestSecretsFetcherRefusesRedirectedVend(t *testing.T) {
	var evilHits int32
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&evilHits, 1)
		_ = json.NewEncoder(w).Encode(drive.SecretBundle{AccountID: "a1", RefreshToken: "attacker-chosen"})
	}))
	defer evil.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, strings.Replace(evil.URL, "127.0.0.1", "localhost", 1), http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	cfg := config.Config{ControlCenterURL: origin.URL, NodeName: "n1", NodeToken: "s3cret"}
	for name, fetch := range map[string]func(context.Context, string) (drive.SecretBundle, error){
		"secrets-for-node":   router.DefaultSecretsFetcher(cfg, newRouterHTTP(cfg)),
		"secrets-for-browse": router.DefaultBrowseSecretsFetcher(cfg, newRouterHTTP(cfg)),
	} {
		b, err := fetch(context.Background(), "a1")
		if err == nil || b.RefreshToken != "" {
			t.Fatalf("%s: redirected vend accepted: bundle=%+v err=%v", name, b, err)
		}
	}
	if n := atomic.LoadInt32(&evilHits); n != 0 {
		t.Fatalf("redirect target received %d request(s)", n)
	}
}

// Positive control: a non-redirected vend still works through the same client.
func TestSecretsFetcherDirectVendStillWorks(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(drive.SecretBundle{AccountID: "a1", RefreshToken: "rt"})
	}))
	defer srv.Close()
	cfg := config.Config{ControlCenterURL: srv.URL, NodeName: "n1", NodeToken: "s3cret"}
	b, err := router.DefaultSecretsFetcher(cfg, newRouterHTTP(cfg))(context.Background(), "a1")
	if err != nil || b.RefreshToken != "rt" || gotAuth != "Bearer s3cret" {
		t.Fatalf("bundle=%+v err=%v auth=%q", b, err, gotAuth)
	}
}

// The outbox client wraps the router transport in the kit bearer round-tripper
// (same shape as main.go). It carries a transport token, so it relies on the
// round-tripper check rather than the client's CheckRedirect.
func TestOutboxClientRefusesCrossOriginRedirect(t *testing.T) {
	var targetHits int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&targetHits, 1)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	routerHTTP := newRouterHTTP(config.Config{})
	outboxHTTP := &http.Client{
		Timeout:   routerHTTP.Timeout,
		Transport: transport.NewBearerRoundTripper(routerHTTP.Transport, "s3cret"),
	}
	resp, err := outboxHTTP.Post(origin.URL, "text/plain", strings.NewReader("SECRET-BODY"))
	if err == nil {
		resp.Body.Close()
		t.Fatal("cross-origin redirect was followed")
	}
	if n := atomic.LoadInt32(&targetHits); n != 0 {
		t.Fatalf("redirect target received %d request(s)", n)
	}
}
