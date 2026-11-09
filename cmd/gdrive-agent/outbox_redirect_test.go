package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Daniel-dev22/agent-kit-go/transport"
)

// The outbox client wraps the router transport in the kit bearer round-tripper
// (same shape as main). It must not carry the node token to a redirect target
// on another host.
func TestOutboxClientDoesNotLeakBearerOnRedirect(t *testing.T) {
	var got string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	routerHTTP := transport.NewRouterClient(transport.RouterClientConfig{BearerToken: "s3cret"})
	outboxHTTP := &http.Client{
		Timeout:   routerHTTP.Timeout,
		Transport: transport.NewBearerRoundTripper(routerHTTP.Transport, "s3cret"),
	}
	resp, err := outboxHTTP.Post(origin.URL, "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got != "" {
		t.Fatalf("bearer leaked to redirect target: %q", got)
	}
}
