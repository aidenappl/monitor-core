package services

import (
	"sync"
	"time"
)

// DROP_WINDOW is how far back Queue.RecentDrops looks. /health reports it so a
// "dropping events" badge can mean "dropping now" instead of "dropped at some
// point since boot": the lifetime `dropped` counter never decreases, so after
// one incident (2026-11-06, a full ClickHouse disk) it read as an ongoing
// outage for as long as the process lived.
const DROP_WINDOW = 15 * time.Minute

// dropWindow counts drops in one-minute buckets over DROP_WINDOW. A ring of
// minute buckets rather than a list of timestamps: an overflow drops one event
// at a time on the ingest hot path, so recording must be O(1) and its memory
// fixed however many events are lost.
type dropWindow struct {
	mu      sync.Mutex
	minutes [int(DROP_WINDOW / time.Minute)]int64 // unix minute each bucket holds
	counts  [int(DROP_WINDOW / time.Minute)]int64
	last    time.Time
}

func (w *dropWindow) record(now time.Time, n int64) {
	minute := now.Unix() / 60
	i := int(minute % int64(len(w.minutes)))
	w.mu.Lock()
	if w.minutes[i] != minute {
		w.minutes[i] = minute
		w.counts[i] = 0
	}
	w.counts[i] += n
	w.last = now
	w.mu.Unlock()
}

// recent returns the drops in the last DROP_WINDOW and the time of the most
// recent drop (zero if none since boot).
func (w *dropWindow) recent(now time.Time) (int64, time.Time) {
	oldest := now.Unix()/60 - int64(len(w.minutes)) + 1
	w.mu.Lock()
	defer w.mu.Unlock()
	var total int64
	for i, m := range w.minutes {
		if m >= oldest {
			total += w.counts[i]
		}
	}
	return total, w.last
}
