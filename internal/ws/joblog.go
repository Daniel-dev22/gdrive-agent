package ws

import (
	"log/slog"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/Daniel-dev22/gdrive-agent/internal/job"
)

// JobLogResolver bridges the WS layer to the runner without introducing
// a circular import. Implemented by *job.Runner via its Job() method, but
// kept as an interface so the WS handler is testable with a stub.
type JobLogResolver interface {
	Job(id string) *job.JobHandle
}

// JobLogHandler streams a job's log ring + tail to a WS subscriber.
// Connection lifecycle:
//
//  1. Upgrade.
//  2. Look up the job by URL :id. If unknown → close with 4404.
//  3. Subscribe → flush scrollback (already-buffered ring lines) → enter
//     write pump that ships each new LogLine.
//  4. Heartbeat ping every 30s.
//  5. Read pump detects disconnect.
//
// When the job ends, the buffer's Subscribe() channel closes and the
// writer pump exits cleanly.
func JobLogHandler(resolver JobLogResolver) gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("id")
		h := resolver.Job(jobID)
		if h == nil {
			// Job unknown or already cleaned up. Returning 404 before
			// upgrade is the cleanest path — no half-open WS.
			c.AbortWithStatusJSON(404, gin.H{"error": "job not found"})
			return
		}
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			slog.Warn("joblog ws upgrade", "error", err)
			return
		}
		snap, ch, unsub := h.Logs.Subscribe()
		defer unsub()

		writeMu := sync.Mutex{}
		done := make(chan struct{})

		go func() {
			t := time.NewTicker(30 * time.Second)
			defer t.Stop()
			defer conn.Close()
			// Flush scrollback first.
			for _, l := range snap {
				writeMu.Lock()
				err := conn.WriteJSON(l)
				writeMu.Unlock()
				if err != nil {
					return
				}
			}
			for {
				select {
				case <-done:
					return
				case l, ok := <-ch:
					if !ok {
						// Job ended — buffer closed.
						return
					}
					writeMu.Lock()
					err := conn.WriteJSON(l)
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

		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}
}
