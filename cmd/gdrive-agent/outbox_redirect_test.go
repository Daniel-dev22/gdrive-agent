package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Daniel-dev22/agent-kit-go/transport"
)

// The outbox client wraps the router transport in the kit bearer round-tripper
// (same shape as main). It must refuse a redirect to another origin, both for
// requests that rely on the transport token and for callers that set their own
// Authorization (router handlers.go secretsFetcher): net/http strips the
// caller's header on the foreign hop and the transport would otherwise re-add
// the node token.
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

	routerHTTP := transport.NewRouterClient(transport.RouterClientConfig{BearerToken: "s3cret"})
	outboxHTTP := &http.Client{
		Timeout:   routerHTTP.Timeout,
		Transport: transport.NewBearerRoundTripper(routerHTTP.Transport, "s3cret"),
	}
	for name, setAuth := range map[string]bool{"transport-token": false, "caller-set-authorization": true} {
		req, err := http.NewRequest(http.MethodPost, origin.URL, strings.NewReader("SECRET-BODY"))
		if err != nil {
			t.Fatal(err)
		}
		if setAuth {
			req.Header.Set("Authorization", "Bearer s3cret")
		}
		resp, err := outboxHTTP.Do(req)
		if err == nil {
			resp.Body.Close()
			t.Fatalf("%s: cross-origin redirect was followed", name)
		}
	}
	if n := atomic.LoadInt32(&targetHits); n != 0 {
		t.Fatalf("redirect target received %d request(s)", n)
	}
}
