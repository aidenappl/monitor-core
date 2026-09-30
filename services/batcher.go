package services

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/telemetry"
)

// Writer is the interface for writing event batches
type Writer interface {
	WriteBatch(ctx context.Context, events []*structs.Event) error
}

// Batcher collects events and flushes them in batches
type Batcher struct {
	queue         *Queue
	writer        Writer
	batchSize     int
	flushInterval time.Duration
	batch         []*structs.Event

	// The retry policy. Fields rather than the constants below so a test can
	// drive a drop storm without thirty seconds of real backoff per batch.
	maxRetries    int
	retryBaseWait time.Duration

	// lastFlush is the UnixNano of the most recent SUCCESSFUL WriteBatch, 0 if
	// there has never been one. Read from the /health handler's goroutine, so
	// atomic rather than a plain time.Time.
	lastFlush atomic.Int64

	// flushed counts events the writer accepted, for the periodic ingest
	// summary (routes.RunIngestSummary).
	flushed atomic.Int64

	// The outage in progress, if any. Touched only by the Run goroutine. Kept so
	// the first successful write after a failure can say what the outage cost —
	// the per-failure events are coalesced and cannot.
	failingSince   time.Time
	failedAttempts int
	droppedBatches int
	droppedEvents  int
}

// NewBatcher creates a new batcher
func NewBatcher(queue *Queue, writer Writer, batchSize int, flushInterval time.Duration) *Batcher {
	return &Batcher{
		queue:         queue,
		writer:        writer,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		batch:         make([]*structs.Event, 0, batchSize),
		maxRetries:    maxFlushRetries,
		retryBaseWait: flushRetryBaseWait,
	}
}

// Run starts the batcher loop
func (b *Batcher) Run(ctx context.Context) {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(b.batch) > 0 {
				b.flush(context.Background())
			}
			return

		case event, ok := <-b.queue.Events():
			if !ok {
				if len(b.batch) > 0 {
					b.flush(ctx)
				}
				return
			}
			b.batch = append(b.batch, event)
			if len(b.batch) >= b.batchSize {
				b.flush(ctx)
			}

		case <-ticker.C:
			if len(b.batch) > 0 {
				b.flush(ctx)
			}
		}
	}
}

// LastFlushAt reports when a batch last reached the writer successfully. The
// zero Time means never — which, unlike the drop counter, distinguishes "just
// booted, nothing to write yet" from "has been failing since boot": a process
// whose very first flush fails drops events without the counter ever having had
// a healthy value to move away from.
func (b *Batcher) LastFlushAt() time.Time {
	ns := b.lastFlush.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// Flushed is the number of events the writer has accepted since boot.
func (b *Batcher) Flushed() int64 {
	return b.flushed.Load()
}

const (
	maxFlushRetries    = 5
	flushRetryBaseWait = 2 * time.Second
)

// flush writes the batch, retrying, and drops it when the writer never succeeds.
//
// ⚠️ EVERY EVENT HERE IS COALESCED, and on appleby-core that is not a
// nicety. appleby-core ingests its own telemetry: while its ClickHouse is down,
// the event reporting this dropped batch is itself enqueued, lands in the next
// batch and is dropped with it. Uncoalesced, a ClickHouse outage would make the
// batcher report itself in a loop at the flush rate. Coalesced, the loop costs
// one event per failure mode per minute — and a successful flush after the
// outage emits one batch.write.recovered with the totals.
//
// The same outage cannot be SEEN in the appleby zone while it lasts: its events
// are the ones being dropped. That is why these are also printed on stdout, and
// why AGENTS.md names stdout plus the control plane's zone probe as the only
// places it shows.
func (b *Batcher) flush(ctx context.Context) {
	if len(b.batch) == 0 {
		return
	}
	size := len(b.batch)

	var err error
	for attempt := 1; attempt <= b.maxRetries; attempt++ {
		start := time.Now()
		err = b.writer.WriteBatch(ctx, b.batch)
		duration := time.Since(start)

		if err == nil {
			b.lastFlush.Store(time.Now().UnixNano())
			b.flushed.Add(int64(size))
			b.recovered(ctx, size, attempt)
			b.batch = b.batch[:0]
			return
		}

		b.failedAttempts++
		if b.failingSince.IsZero() {
			b.failingSince = start
		}
		wait := b.retryBaseWait * time.Duration(attempt)
		if attempt < b.maxRetries {
			telemetry.WarnCoalesced(ctx, "batch.write.retrying", "batch.write.retrying", err, map[string]any{
				"dependency":   "clickhouse",
				"batch_size":   size,
				"attempt":      attempt,
				"max_attempts": b.maxRetries,
				"duration_ms":  duration.Milliseconds(),
				"timed_out":    errors.Is(err, context.DeadlineExceeded),
				"reason":       "clickhouse_write_failed",
				"outcome":      fmt.Sprintf("retrying in %s", wait),
			})
		}

		select {
		case <-ctx.Done():
			// Abandoning the batch destroys these events. Record them before
			// truncating, or /health keeps reporting dropped=0 through an
			// outage that is losing everything.
			b.drop(ctx, size, attempt, err, "shutdown_during_retry")
			return
		case <-time.After(wait):
		}
	}

	b.drop(ctx, size, b.maxRetries, err, "retries_exhausted")
}

// drop abandons the batch: counted into /health's `dropped`, reported coalesced.
// Counts only — never an event's name or data, which belong to the tenant that
// sent them.
func (b *Batcher) drop(ctx context.Context, size, attempts int, err error, reason string) {
	b.queue.RecordDropped(int64(size))
	b.droppedBatches++
	b.droppedEvents += size
	b.batch = b.batch[:0]

	_, droppedTotal, pending := b.queue.Stats()
	telemetry.ErrorCoalesced(ctx, "batch.write.dropped", "batch.write.dropped", err, map[string]any{
		"dependency":    "clickhouse",
		"batch_size":    size,
		"attempts":      attempts,
		"max_attempts":  b.maxRetries,
		"dropped_total": droppedTotal,
		"queue_pending": pending,
		"reason":        reason,
		"outcome":       fmt.Sprintf("dropped %d events", size),
	})
}

// recovered closes an outage: one warning with what it cost, because every
// failure inside it was coalesced. A write that succeeded on its first attempt
// with no outage open emits nothing.
func (b *Batcher) recovered(ctx context.Context, size, attempt int) {
	if b.failingSince.IsZero() {
		return
	}
	telemetry.WarnCoalesced(ctx, "batch.write.recovered", "batch.write.recovered", nil, map[string]any{
		"dependency":      "clickhouse",
		"outage_ms":       time.Since(b.failingSince).Milliseconds(),
		"failed_attempts": b.failedAttempts,
		"batches_dropped": b.droppedBatches,
		"events_dropped":  b.droppedEvents,
		"batch_size":      size,
		"attempt":         attempt,
		"outcome":         "writing again",
	})
	b.failingSince = time.Time{}
	b.failedAttempts, b.droppedBatches, b.droppedEvents = 0, 0, 0
}
