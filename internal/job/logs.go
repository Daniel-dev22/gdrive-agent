package job

import (
	"sync"
	"time"
)

// LogRingSize bounds memory per running job — the most recent N log lines
// are retained for late-joining WS subscribers. Old lines age out FIFO.
const LogRingSize = 500

// LogLine is one structured log entry from a running job.
type LogLine struct {
	JobID   string    `json:"job_id"`
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`            // info|warn|error
	Message string    `json:"message"`
	Field   string    `json:"field,omitempty"`  // e.g. "files_changed"
	Value   string    `json:"value,omitempty"`  // serialized value
}

// LogBuffer holds the ring of log lines for one job plus a set of live
// subscribers (WS connections). Producers call Append; subscribers
// register via Subscribe and read from the returned channel until the
// job ends and the buffer is Closed.
//
// Slow subscribers don't block producers — a full channel buffer means
// that subscriber's tail drops. WS layer detects the drop and reconnects.
type LogBuffer struct {
	mu      sync.Mutex
	ring    []LogLine
	closed  bool
	subs    map[chan LogLine]struct{}
}

// NewLogBuffer pre-allocates the ring slice at full capacity so steady-
// state appends never resize.
func NewLogBuffer() *LogBuffer {
	return &LogBuffer{
		ring: make([]LogLine, 0, LogRingSize),
		subs: map[chan LogLine]struct{}{},
	}
}

// Append adds a log line to the ring and fans out to subscribers. Drops
// oldest line if ring is full.
func (b *LogBuffer) Append(l LogLine) {
	b.mu.Lock()
	if len(b.ring) >= LogRingSize {
		// Drop oldest. The copy is unfortunate but happens at most once
		// per log line; 500 entries × ~200 bytes is ~100KB of data — well
		// under any sensitivity threshold.
		b.ring = b.ring[1:]
	}
	b.ring = append(b.ring, l)
	subs := make([]chan LogLine, 0, len(b.subs))
	for c := range b.subs {
		subs = append(subs, c)
	}
	b.mu.Unlock()
	for _, c := range subs {
		select {
		case c <- l:
		default:
			// Subscriber falling behind. Don't block the producer.
		}
	}
}

// Subscribe returns a channel that receives every subsequent log line and
// the snapshot of lines already in the ring. Caller must drain promptly
// and unsubscribe when done.
//
// The returned snapshot is delivered separately from the channel so the
// caller can present a "scrollback + tail" UX without race conditions.
func (b *LogBuffer) Subscribe() (snapshot []LogLine, ch chan LogLine, unsubscribe func()) {
	c := make(chan LogLine, 64)
	b.mu.Lock()
	snap := make([]LogLine, len(b.ring))
	copy(snap, b.ring)
	if b.closed {
		// Buffer already closed — return scrollback only, close channel
		// immediately so range-over-channel callers exit cleanly.
		close(c)
		b.mu.Unlock()
		return snap, c, func() {}
	}
	b.subs[c] = struct{}{}
	b.mu.Unlock()

	unsub := func() {
		b.mu.Lock()
		if _, ok := b.subs[c]; ok {
			delete(b.subs, c)
			close(c)
		}
		b.mu.Unlock()
	}
	return snap, c, unsub
}

// Close marks the buffer terminal and closes every live subscriber channel.
// After Close the snapshot is still readable but no new Append delivers.
func (b *LogBuffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for c := range b.subs {
		close(c)
	}
	b.subs = map[chan LogLine]struct{}{}
}

// Snapshot returns the current ring contents without subscribing.
func (b *LogBuffer) Snapshot() []LogLine {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]LogLine, len(b.ring))
	copy(out, b.ring)
	return out
}
