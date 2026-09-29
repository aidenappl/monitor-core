package routes

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/telemetry"
)

// INGEST_SUMMARY_EVERY is how often healthy ingest is reported.
//
// A successful POST /v1/events emits no request event — on appleby-core that
// event would be ingested by the next batch and report itself forever (see
// middleware.LoggingMiddleware). So healthy ingest appears in Monitor only as
// this summary: one event per window, and none at all for a window in which
// nothing moved. /health answers the same question live.
const INGEST_SUMMARY_EVERY = 15 * time.Minute

// ingestRejected counts ingest bodies refused as malformed since boot.
var ingestRejected atomic.Int64

type ingestTotals struct {
	enqueued     int64
	flushed      int64
	dropped      int64
	rejected     int64
	authRejected int64
}

func currentIngestTotals() ingestTotals {
	var t ingestTotals
	if Queue != nil {
		t.enqueued, t.dropped, _ = Queue.Stats()
	}
	if Batcher != nil {
		t.flushed = Batcher.Flushed()
	}
	t.rejected = ingestRejected.Load()
	t.authRejected = middleware.IngestAuthRejected()
	return t
}

// RunIngestSummary emits ingest.summary.reported every INGEST_SUMMARY_EVERY
// until ctx is cancelled. Data plane only.
func RunIngestSummary(ctx context.Context) {
	ticker := time.NewTicker(INGEST_SUMMARY_EVERY)
	defer ticker.Stop()

	last := currentIngestTotals()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := currentIngestTotals()
			emitIngestSummary(ctx, last, current, INGEST_SUMMARY_EVERY)
			last = current
		}
	}
}

func emitIngestSummary(ctx context.Context, prev, cur ingestTotals, window time.Duration) {
	delta := ingestTotals{
		enqueued:     cur.enqueued - prev.enqueued,
		flushed:      cur.flushed - prev.flushed,
		dropped:      cur.dropped - prev.dropped,
		rejected:     cur.rejected - prev.rejected,
		authRejected: cur.authRejected - prev.authRejected,
	}
	if delta == (ingestTotals{}) {
		return
	}

	var pending int
	if Queue != nil {
		_, _, pending = Queue.Stats()
	}
	telemetry.Info(ctx, "ingest.summary.reported", map[string]any{
		"window_s":      int(window / time.Second),
		"enqueued":      delta.enqueued,
		"flushed":       delta.flushed,
		"dropped":       delta.dropped,
		"rejected":      delta.rejected,
		"auth_rejected": delta.authRejected,
		"queue_pending": pending,
	})
}
