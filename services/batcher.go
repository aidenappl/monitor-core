package services

import (
	"context"
	"log"
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

func (b *Batcher) flush(ctx context.Context) {
	if len(b.batch) == 0 {
		return
	}

	start := time.Now()
	err := b.writer.WriteBatch(ctx, b.batch)
	duration := time.Since(start)

	if err != nil {
		log.Printf("failed to write batch of %d events: %v", len(b.batch), err)
	} else {
		log.Printf("flushed %d events in %v", len(b.batch), duration)
	}

	b.batch = b.batch[:0]
}
