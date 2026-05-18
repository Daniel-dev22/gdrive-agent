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

// upgrader is shared across both /ws/* handlers. CheckOrigin returns true
// because inbound auth terminates at the mTLS tunnel before requests reach
// the agent — origin spoofing isn't a meaningful additional surface.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// snapshotEnvelope is the wire shape the frontend's useFleet hook expects:
// {"type":"snapshot","data":<snap>}. Mirrors duplicacy-agent-api's
// fleetEvent — same hook consumes both backends, so the wrapper has to match
// or the frontend silently drops the frame (which manifests as the panel
// showing "unreachable" even though the WS is connected).
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
