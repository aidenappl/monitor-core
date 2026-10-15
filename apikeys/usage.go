package apikeys

import (
	"context"
	"sync"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/telemetry"
)

// USAGE_FLUSH_INTERVAL is how often recorded key use is written to
// api_keys.last_used_at. It bounds how stale "last used" can read in the UI.
const USAGE_FLUSH_INTERVAL = time.Minute

// usage holds the newest use of each key id since the last flush.
//
// ─────────────────────────────────────────────────────────────────────────────
// ⚠️ last_used_at EXISTED SINCE MIGRATION 106 AND NOTHING EVER WROTE IT.
//
// The column was selected, serialised and documented, so every key read
// "never used" — including the ones a whole fleet ingests through. The one
// question an operator asks before revoking a key ("is anything still sending
// with this?") had a confident, wrong answer.
//
// Validation runs on every ingest request, so it must not touch MariaDB. It
// records the use in memory under its own mutex (never the cache's), and a
// background loop writes the batch once a minute: one UPDATE per key that was
// used, regardless of how many requests it made.
// ─────────────────────────────────────────────────────────────────────────────
var (
	usageMu sync.Mutex
	usage   = make(map[string]time.Time)
)

// markUsed records that the key with this id authenticated a request now.
func markUsed(id string) {
	now := time.Now().UTC()
	usageMu.Lock()
	usage[id] = now
	usageMu.Unlock()
}

// takeUsage swaps out the pending batch.
func takeUsage() map[string]time.Time {
	usageMu.Lock()
	defer usageMu.Unlock()
	if len(usage) == 0 {
		return nil
	}
	batch := usage
	usage = make(map[string]time.Time, len(batch))
	return batch
}

// flushUsage writes one pending batch. A failed write puts its entries back
// (unless a newer use has arrived meanwhile), so a MariaDB blip delays the
// timestamp rather than losing it.
func flushUsage(ctx context.Context) {
	batch := takeUsage()
	if batch == nil {
		return
	}
	if err := query.TouchAPIKeys(db.SQL, batch); err != nil {
		usageMu.Lock()
		for id, at := range batch {
			if existing, ok := usage[id]; !ok || existing.Before(at) {
				usage[id] = at
			}
		}
		usageMu.Unlock()
		telemetry.WarnCoalesced(ctx, "apikeys.usage.flush.failed", "apikeys.usage.flush.failed", err, map[string]any{
			"dependency": "mariadb",
			"keys":       len(batch),
			"outcome":    "last_used_at is stale until the next successful flush",
		})
	}
}

// runUsageFlusher writes recorded key use until ctx is cancelled, then flushes
// once more so a clean shutdown does not drop the last minute.
func runUsageFlusher(ctx context.Context) {
	ticker := time.NewTicker(USAGE_FLUSH_INTERVAL)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			flushUsage(context.Background())
			return
		case <-ticker.C:
			flushUsage(ctx)
		}
	}
}
