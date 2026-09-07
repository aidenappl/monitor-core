package services

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aidenappl/monitor-core/structs"
)

// failingWriter is the shape of a dead ClickHouse: it accepts the call and
// loses the batch.
type failingWriter struct{}

func (w *failingWriter) WriteBatch(ctx context.Context, events []*structs.Event) error {
	return errors.New("clickhouse unreachable")
}

type okWriter struct{}

func (w *okWriter) WriteBatch(ctx context.Context, events []*structs.Event) error { return nil }

// A batch the batcher gives up on must land in the same counter /health reports.
// Before this, a zone whose writer failed every call answered dropped=0 while
// destroying every event.
func TestFlushRecordsDroppedOnAbandonedBatch(t *testing.T) {
	queue := NewQueue(10)
	batcher := NewBatcher(queue, &failingWriter{}, 100, time.Second)
	batcher.batch = []*structs.Event{{Name: "a"}, {Name: "b"}, {Name: "c"}}

	// An already-cancelled context reaches the ctx.Done() abandonment site on
	// the first failure, instead of waiting out the 30s retry ladder. The
	// permanent-drop site below it records identically.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batcher.flush(ctx)

	if _, dropped, _ := queue.Stats(); dropped != 3 {
		t.Errorf("dropped = %d, want 3", dropped)
	}
	if len(batcher.batch) != 0 {
		t.Errorf("batch not cleared: len = %d", len(batcher.batch))
	}
}

func TestFlushTracksLastSuccessfulWrite(t *testing.T) {
	queue := NewQueue(10)
	batcher := NewBatcher(queue, &okWriter{}, 100, time.Second)

	if !batcher.LastFlushAt().IsZero() {
		t.Errorf("LastFlushAt = %v before any flush, want the zero Time", batcher.LastFlushAt())
	}

	batcher.batch = []*structs.Event{{Name: "a"}}
	batcher.flush(context.Background())

	if batcher.LastFlushAt().IsZero() {
		t.Error("LastFlushAt still zero after a successful flush")
	}
	if _, dropped, _ := queue.Stats(); dropped != 0 {
		t.Errorf("dropped = %d after a successful flush, want 0", dropped)
	}
}
