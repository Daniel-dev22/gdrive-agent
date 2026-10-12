package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/Daniel-dev22/agent-kit-go/memlimit"
	"github.com/gin-gonic/gin"

	"github.com/Daniel-dev22/gdrive-agent/internal/db"
)

func readyBody(t *testing.T, a *App) map[string]any {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	a.handleReady(c)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestReadyReportsAnonOnlyWhenSampled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := db.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	m := readyBody(t, &App{Store: store})
	if _, ok := m["anon_peak_bytes"]; ok {
		t.Fatalf("no sampler: fields must be absent: %v", m)
	}

	// A sampler over a fixture cgroup with a known literal value.
	dir := t.TempDir()
	self := filepath.Join(dir, "self")
	mustWrite(t, self, "0::/\n")
	mustWrite(t, filepath.Join(dir, "memory.stat"), "anon 4096\nfile 1\n")
	anon := memlimit.StartPeakAnon(ctx, nil, memlimit.PeakOptions{Root: dir, SelfCgroup: self})
	m = readyBody(t, &App{Store: store, Anon: anon})
	if m["anon_bytes"] != float64(4096) || m["anon_peak_bytes"] != float64(4096) || m["status"] != "ready" {
		t.Fatalf("got %v", m)
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
