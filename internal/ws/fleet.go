package ws

import (
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/Daniel-dev22/gdrive-agent/internal/fleet"
)

// upgrader is shared across both /ws/* handlers.
//
// SECURITY: CheckOrigin returns true, i.e. ANY origin may open these
// sockets. That is only safe because the agent is meant to sit behind an
// mTLS-terminating reverse proxy that authenticates before anything reaches
// this process. Exposing the listener directly makes both feeds
// world-readable from any web page. See README.md.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// snapshotEnvelope is the wire shape every frame is wrapped in:
// {"type":"snapshot","data":<snap>}. It is deliberately identical to
// duplicacy-agent-api's, so one client can consume both agents' feeds. A
// consumer that exact-matches on `type` silently DROPS an unrecognised
// frame, which shows up as a panel stuck on "unreachable" while the socket
// is in fact connected — so renaming this is a hard break, not a rename.
type snapshotEnvelope struct {
	Type string          `json:"type"`
	Data *fleet.Snapshot `json:"data"`
}

// FleetHandler returns a gin handler that upgrades to WS and streams
// snapshot frames from the hub. One read pump (detect disconnect) and
// one write pump (snapshots + 30s heartbeat) per connection.
func FleetHandler(hub *fleet.Hub) gin.HandlerFunc {
	return func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			slog.Warn("fleet ws upgrade", "error", err)
			return
		}
		ch, unsub := hub.Subscribe()
		defer unsub()

		// Initial snapshot — don't make the client wait for the next tick.
		if snap, err := hub.SnapshotNow(c.Request.Context()); err == nil {
			_ = conn.WriteJSON(snapshotEnvelope{Type: "snapshot", Data: &snap})
		}

		writeMu := sync.Mutex{}
		done := make(chan struct{})

		// Heartbeat / writer pump.
		go func() {
			t := time.NewTicker(30 * time.Second)
			defer t.Stop()
			defer conn.Close()
			for {
				select {
				case <-done:
					return
				case snap, ok := <-ch:
					if !ok {
						return
					}
					writeMu.Lock()
					err := conn.WriteJSON(snapshotEnvelope{Type: "snapshot", Data: &snap})
					writeMu.Unlock()
					if err != nil {
						return
					}
				case <-t.C:
					writeMu.Lock()
					err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second))
					writeMu.Unlock()
					if err != nil {
						return
					}
				}
			}
		}()

		// Read pump (blocks until disconnect).
		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}
}
