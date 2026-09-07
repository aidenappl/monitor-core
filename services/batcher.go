package services

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/aidenappl/monitor-core/structs"
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

	// lastFlush is the UnixNano of the most recent SUCCESSFUL WriteBatch, 0 if
	// there has never been one. Read from the /health handler's goroutine, so
	// atomic rather than a plain time.Time.
	lastFlush atomic.Int64
}

// NewBatcher creates a new batcher
func NewBatcher(queue *Queue, writer Writer, batchSize int, flushInterval time.Duration) *Batcher {
	return &Batcher{
		queue:         queue,
		writer:        writer,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		batch:         make([]*structs.Event, 0, batchSize),
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

const (
	maxFlushRetries    = 5
	flushRetryBaseWait = 2 * time.Second
)

func (b *Batcher) flush(ctx context.Context) {
	if len(b.batch) == 0 {
		return
	}

	var err error
	for attempt := 1; attempt <= maxFlushRetries; attempt++ {
		start := time.Now()
		err = b.writer.WriteBatch(ctx, b.batch)
		duration := time.Since(start)

		if err == nil {
			log.Printf("flushed %d events in %v", len(b.batch), duration)
			b.lastFlush.Store(time.Now().UnixNano())
			b.batch = b.batch[:0]
			return
		}

		wait := flushRetryBaseWait * time.Duration(attempt)
		log.Printf("failed to write batch of %d events (attempt %d/%d): %v — retrying in %v",
			len(b.batch), attempt, maxFlushRetries, err, wait)

		select {
		case <-ctx.Done():
			log.Printf("context cancelled, dropping batch of %d events after %d failed attempt(s)", len(b.batch), attempt)
			// Abandoning the batch destroys these events. Record them before
			// truncating, or /health keeps reporting dropped=0 through an
			// outage that is losing everything.
			b.queue.RecordDropped(int64(len(b.batch)))
			b.batch = b.batch[:0]
			return
		case <-time.After(wait):
		}
	}

	log.Printf("permanently dropping batch of %d events after %d failed attempts: %v", len(b.batch), maxFlushRetries, err)
	b.queue.RecordDropped(int64(len(b.batch)))
	b.batch = b.batch[:0]
}
