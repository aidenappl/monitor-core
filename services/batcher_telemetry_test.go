package services

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	monitor "github.com/aidenappl/go-monitor"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/telemetry"
)

type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time          { return c.now }
func (c *testClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

// tenantBatch is a batch whose events carry a marker in every tenant-owned
// field. None of it may ever reach a self-telemetry event.
func tenantBatch() []*structs.Event {
	return []*structs.Event{
		{Service: "svc", Name: "LEAKMARKER.name", Data: map[string]interface{}{"secret": "LEAKMARKER", "message": "LEAKMARKER"}},
		{Service: "svc", Name: "LEAKMARKER.name", Data: map[string]interface{}{"path": "/LEAKMARKER"}},
	}
}

// TestBatchDropStormCoalescesToOneEventPerMinute is the batcher's loop guard.
// On appleby-core, batch.write.dropped is itself enqueued behind the batch that
// failed; fifty drops inside a minute must cost one event, then one trailing
// count — and a recovered writer reports the outage's total once.
func TestBatchDropStormCoalescesToOneEventPerMinute(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)}
	t.Cleanup(telemetry.SetClockForTest(clock.Now))
	rec := monitor.StartRecording()
	t.Cleanup(rec.Stop)

	queue := NewQueue(10)
	batcher := NewBatcher(queue, &failingWriter{}, 100, time.Second)
	batcher.maxRetries = 2
	batcher.retryBaseWait = 0

	const storm = 50
	for i := 0; i < storm; i++ {
		batcher.batch = tenantBatch()
		batcher.flush(context.Background())
		clock.Advance(time.Second)
	}

	if _, dropped, _ := queue.Stats(); dropped != storm*2 {
		t.Fatalf("dropped = %d, want %d — coalescing must not cost the counter", dropped, storm*2)
	}
	drops := rec.Named("batch.write.dropped")
	if len(drops) != 1 {
		t.Fatalf("a %d-drop storm emitted %d batch.write.dropped events, want 1", storm, len(drops))
	}
	if n := len(rec.Named("batch.write.retrying")); n != 1 {
		t.Errorf("batch.write.retrying events = %d, want 1", n)
	}
	first := drops[0].Data.(map[string]any)
	if drops[0].Level != monitor.LevelError || first["reason"] != "retries_exhausted" || fmt.Sprint(first["batch_size"]) != "2" {
		t.Errorf("drop event lacks its detail: level=%s %v", drops[0].Level, first)
	}

	clock.Advance(telemetry.COALESCE_WINDOW)
	telemetry.SweepCoalesced()
	drops = rec.Named("batch.write.dropped")
	if len(drops) != 2 {
		t.Fatalf("after the window: %d drop events, want 2", len(drops))
	}
	if s := fmt.Sprint(drops[1].Data.(map[string]any)["suppressed"]); s != fmt.Sprint(storm-1) {
		t.Errorf("trailing suppressed = %s, want %d", s, storm-1)
	}

	batcher.writer = &okWriter{}
	batcher.batch = tenantBatch()
	batcher.flush(context.Background())
	recovered := rec.Named("batch.write.recovered")
	if len(recovered) != 1 {
		t.Fatalf("batch.write.recovered events = %d, want 1", len(recovered))
	}
	if got := fmt.Sprint(recovered[0].Data.(map[string]any)["events_dropped"]); got != fmt.Sprint(storm*2) {
		t.Errorf("recovered events_dropped = %s, want %d", got, storm*2)
	}

	// The same storm, read as the tenant-data boundary: the dropped events'
	// names and data are another service's, and none of it may be in any event.
	for _, e := range rec.Events() {
		line, err := e.ToJSON()
		if err != nil {
			t.Fatalf("marshal %s: %v", e.Name, err)
		}
		if strings.Contains(string(line), "LEAKMARKER") {
			t.Errorf("%s carries tenant data: %s", e.Name, line)
		}
	}
}

// TestHealthyFlushEmitsNothing: the batcher is a background loop, and a
// successful tick is silent.
func TestHealthyFlushEmitsNothing(t *testing.T) {
	rec := monitor.StartRecording()
	t.Cleanup(rec.Stop)

	batcher := NewBatcher(NewQueue(10), &okWriter{}, 100, time.Second)
	for i := 0; i < 5; i++ {
		batcher.batch = tenantBatch()
		batcher.flush(context.Background())
	}
	if got := rec.Events(); len(got) != 0 {
		t.Fatalf("healthy flushes emitted %d events, want 0", len(got))
	}
	if batcher.Flushed() != 10 {
		t.Errorf("Flushed = %d, want 10", batcher.Flushed())
	}
}
