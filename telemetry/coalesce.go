package telemetry

import (
	"context"
	"sync"
	"time"

	monitor "github.com/aidenappl/go-monitor"
)

// COALESCE_WINDOW is the most often any one failure mode is reported.
//
// Every failure that can feed itself goes through here. appleby-core ingests its
// own telemetry: when its ClickHouse is down, the event reporting a dropped
// batch lands in the same queue and is dropped with the next one, which reports
// another dropped batch. Coalesced, that loop costs one event a minute per mode
// no matter how hard it spins.
const COALESCE_WINDOW = time.Minute

// COALESCE_SWEEP_EVERY is how often the trailing counts are checked. A storm
// that stops mid-window would otherwise take its suppressed count with it.
const COALESCE_SWEEP_EVERY = 15 * time.Second

type coalesceEntry struct {
	windowStart time.Time
	suppressed  int64

	// The last event actually emitted for this key, replayed with its
	// suppressed count when the window closes. Kept without its stack.
	level string
	name  string
	data  map[string]any
}

var coalescing = struct {
	sync.Mutex
	entries map[string]*coalesceEntry
	now     func() time.Time
}{entries: map[string]*coalesceEntry{}, now: time.Now}

// WarnCoalesced is Warn for a failure mode that can repeat in a tight loop. key
// names the mode — usually the event name, or the name plus the one id that
// makes two occurrences different problems (a notification channel). err may be
// nil.
//
// The emitted event carries `suppressed`: how many occurrences of key were
// swallowed since the previous one. When a window closes with occurrences still
// unreported, the sweeper replays the event once more — carrying the most recent
// occurrence's fields — with that count and `trailing: true`.
func WarnCoalesced(ctx context.Context, key, name string, err error, data map[string]any) {
	coalesced(ctx, key, monitor.LevelWarn, name, err, data, false)
}

// ErrorCoalesced is Error for a failure mode that can repeat in a tight loop.
// See WarnCoalesced.
func ErrorCoalesced(ctx context.Context, key, name string, err error, data map[string]any) {
	coalesced(ctx, key, monitor.LevelError, name, err, data, true)
}

func coalesced(ctx context.Context, key, level, name string, err error, data map[string]any, withStack bool) {
	admitted, suppressed := admit(key)
	if !admitted {
		// Counted, and its fields kept: the trailing replay then describes the
		// failure as it stood when the window CLOSED — the latest batch, the
		// running totals — not as it stood when the window opened. The caller
		// built this map either way, so keeping it costs a lock and a copy.
		rememberLatest(key, withError(data, err, false))
		return
	}
	data = withError(data, err, withStack)
	data["suppressed"] = suppressed
	data["coalesce_window_s"] = int(COALESCE_WINDOW / time.Second)
	// emit adds identity and source to data, so the copy remembered for the
	// trailing replay is taken after it — the replay then names the original
	// call site rather than the sweeper.
	emit(ctx, level, name, data, 3)
	remember(key, level, name, data)
}

// admit reports whether an event for key may be emitted now, and how many were
// suppressed since the last one. It allocates only for a key seen for the first
// time, so a failing hot path pays a mutex and a map lookup per occurrence.
func admit(key string) (bool, int64) {
	coalescing.Lock()
	defer coalescing.Unlock()

	now := coalescing.now()
	entry, ok := coalescing.entries[key]
	if !ok {
		coalescing.entries[key] = &coalesceEntry{windowStart: now}
		return true, 0
	}
	if now.Sub(entry.windowStart) < COALESCE_WINDOW {
		entry.suppressed++
		return false, 0
	}
	suppressed := entry.suppressed
	entry.suppressed = 0
	entry.windowStart = now
	return true, suppressed
}

func remember(key, level, name string, data map[string]any) {
	kept := make(map[string]any, len(data))
	for k, v := range data {
		if k == "stack_trace" {
			continue
		}
		kept[k] = v
	}
	coalescing.Lock()
	if entry, ok := coalescing.entries[key]; ok {
		entry.level, entry.name, entry.data = level, name, kept
	}
	coalescing.Unlock()
}

// rememberLatest overlays a suppressed occurrence's fields onto the event kept
// for the trailing replay. The emitted event's identity and call site stay; its
// point-in-time fields (a batch size, a running total, the error) move forward.
func rememberLatest(key string, latest map[string]any) {
	coalescing.Lock()
	defer coalescing.Unlock()
	entry, ok := coalescing.entries[key]
	if !ok || entry.data == nil {
		return
	}
	for k, v := range latest {
		entry.data[k] = v
	}
}

// SweepCoalesced replays every closed window that still holds suppressed
// occurrences, and forgets keys that have gone quiet. The sweeper goroutine
// calls it; tests call it after moving the clock.
func SweepCoalesced() {
	flushCoalesced(false)
}

// flushCoalesced with force replays every pending count regardless of window —
// Shutdown uses it, so a storm in progress at exit is still accounted for.
func flushCoalesced(force bool) {
	type replay struct {
		level, name string
		data        map[string]any
	}
	var due []replay

	coalescing.Lock()
	now := coalescing.now()
	for key, entry := range coalescing.entries {
		if !force && now.Sub(entry.windowStart) < COALESCE_WINDOW {
			continue
		}
		if entry.suppressed == 0 || entry.name == "" {
			if now.Sub(entry.windowStart) >= COALESCE_WINDOW {
				delete(coalescing.entries, key)
			}
			continue
		}
		data := make(map[string]any, len(entry.data)+2)
		for k, v := range entry.data {
			data[k] = v
		}
		data["suppressed"] = entry.suppressed
		data["trailing"] = true
		due = append(due, replay{level: entry.level, name: entry.name, data: data})
		entry.suppressed = 0
		entry.windowStart = now
	}
	coalescing.Unlock()

	for _, r := range due {
		emit(context.Background(), r.level, r.name, r.data, 2)
	}
}

func runSweeper(ctx context.Context) {
	ticker := time.NewTicker(COALESCE_SWEEP_EVERY)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			SweepCoalesced()
		}
	}
}

// SetClockForTest replaces the clock coalescing reads and clears its state,
// returning a function that restores both. Tests only.
func SetClockForTest(clock func() time.Time) (restore func()) {
	coalescing.Lock()
	prevNow, prevEntries := coalescing.now, coalescing.entries
	coalescing.now, coalescing.entries = clock, map[string]*coalesceEntry{}
	coalescing.Unlock()
	return func() {
		coalescing.Lock()
		coalescing.now, coalescing.entries = prevNow, prevEntries
		coalescing.Unlock()
	}
}
