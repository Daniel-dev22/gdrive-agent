package drive

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
	"google.golang.org/api/googleapi"
)

// AdaptiveLimiter gates every outbound Drive API call. Two layers:
//
//  1. RPS via x/time/rate.Limiter — smooth, not bursty.
//  2. Concurrency via x/sync/semaphore.Weighted — bounds simultaneous in-flight
//     requests so HTTP/2 multiplexing doesn't drown the connection.
//
// On 429 (googleapi.Error.Code == 429) both ceilings halve, floor=1. A
// background goroutine ramps both back up by 1/+1 every 60s when no 429
// has fired in the trailing window. The Limiter supports SetLimit() in
// place; the semaphore does not, so we hold it behind atomic.Pointer and
// rebuild it on resize.
//
// Thread-safe: SetLimit is internally locked, atomic.Pointer.Store is
// atomic, sync.Mutex protects the rate/conc current values. Wait/Done
// loads the semaphore once, so a concurrent rebuild won't double-release.
type AdaptiveLimiter struct {
	mu      sync.Mutex // guards curRPS / curConc
	curRPS  float64
	curConc int

	ceilRPS  float64
	ceilConc int

	limiter *rate.Limiter
	sem     atomic.Pointer[semaphore.Weighted]

	last429 atomic.Int64 // unix-nano of most recent 429

	ramper sync.Once
}

// NewAdaptiveLimiter constructs a limiter starting at (rps, conc) and capped
// at those same values. SetCeilings can lift them later if operators tune
// env vars at runtime; for now agent restart is the lever.
func NewAdaptiveLimiter(rps float64, conc int) *AdaptiveLimiter {
	if rps <= 0 {
		rps = 1
	}
	if conc <= 0 {
		conc = 1
	}
	l := &AdaptiveLimiter{
		curRPS:   rps,
		curConc:  conc,
		ceilRPS:  rps,
		ceilConc: conc,
		// Bucket size == ceiling RPS gives us a 1-second burst budget. Sub-
		// second bursts above the rate are unavoidable from the rate
		// algorithm and harmless for our quota usage.
		limiter: rate.NewLimiter(rate.Limit(rps), int(rps)),
	}
	sem := semaphore.NewWeighted(int64(conc))
	l.sem.Store(sem)
	return l
}

// Wait acquires one slot from both the rate limiter and the concurrency
// semaphore. Returns (handle, error). Call handle() (or Done()) to release
// the concurrency slot — rate-limiter tokens don't need releasing.
//
// Cancelled contexts unwind both acquisitions: if the rate-limiter wait
// succeeded but semaphore acquisition was cancelled, the spent rate token
// is simply lost (cheap; tokens regenerate continuously).
func (l *AdaptiveLimiter) Wait(ctx context.Context) (release func(), err error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	sem := l.sem.Load()
	if err := sem.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	released := false
	return func() {
		if released {
			return
		}
		released = true
		// IMPORTANT: release on the same semaphore we acquired against, even
		// if a 429-driven resize swapped a new one into l.sem in between.
		// Releasing on the wrong sem would over-grant capacity and let the
		// new ceiling drift up forever.
		sem.Release(1)
	}, nil
}

// ReportError checks the error from a Drive call. If it's a 429, halve the
// ceilings (floor=1) and update the ramper's last-429 timestamp. Other
// errors are ignored — they don't reflect quota pressure.
func (l *AdaptiveLimiter) ReportError(err error) {
	if err == nil {
		return
	}
	var ge *googleapi.Error
	if !errors.As(err, &ge) {
		return
	}
	if ge.Code != 429 {
		return
	}
	l.last429.Store(time.Now().UnixNano())

	l.mu.Lock()
	defer l.mu.Unlock()
	newRPS := l.curRPS / 2
	if newRPS < 1 {
		newRPS = 1
	}
	newConc := l.curConc / 2
	if newConc < 1 {
		newConc = 1
	}
	if newRPS == l.curRPS && newConc == l.curConc {
		return // already at floor
	}
	slog.Warn("adaptive limiter halving on 429",
		"old_rps", l.curRPS,
		"new_rps", newRPS,
		"old_conc", l.curConc,
		"new_conc", newConc)
	l.curRPS = newRPS
	l.curConc = newConc
	l.applyLocked()
}

// applyLocked must be called with l.mu held. Mutates the rate.Limiter in
// place and rebuilds the semaphore. The old semaphore stays alive long
// enough for in-flight Wait/Done pairs to drain (each retained its own
// reference); new Wait calls land on the rebuilt one.
func (l *AdaptiveLimiter) applyLocked() {
	l.limiter.SetLimit(rate.Limit(l.curRPS))
	l.limiter.SetBurst(maxInt(int(l.curRPS), 1))
	l.sem.Store(semaphore.NewWeighted(int64(l.curConc)))
}

// StartRamper launches the 60s ramp goroutine. Idempotent — calling more
// than once is a no-op via sync.Once.
func (l *AdaptiveLimiter) StartRamper(ctx context.Context) {
	l.ramper.Do(func() {
		go l.rampLoop(ctx)
	})
}

func (l *AdaptiveLimiter) rampLoop(ctx context.Context) {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			last := l.last429.Load()
			if last != 0 && now.UnixNano()-last < int64(60*time.Second) {
				continue // 429 hit within the cooldown window, hold steady
			}
			l.mu.Lock()
			changed := false
			if l.curRPS < l.ceilRPS {
				newRPS := l.curRPS + 1
				if newRPS > l.ceilRPS {
					newRPS = l.ceilRPS
				}
				l.curRPS = newRPS
				changed = true
			}
			if l.curConc < l.ceilConc {
				l.curConc++
				changed = true
			}
			if changed {
				slog.Info("adaptive limiter ramping up", "rps", l.curRPS, "conc", l.curConc)
				l.applyLocked()
			}
			l.mu.Unlock()
		}
	}
}

// Snapshot returns the current rps/conc values for observability.
func (l *AdaptiveLimiter) Snapshot() (rps float64, conc int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.curRPS, l.curConc
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
