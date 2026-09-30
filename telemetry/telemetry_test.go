package telemetry

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	monitor "github.com/aidenappl/go-monitor"
)

// fakeClock is a settable clock for the coalescing window.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func withFakeClock(t *testing.T) *fakeClock {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)}
	t.Cleanup(SetClockForTest(clock.Now))
	return clock
}

func recordEvents(t *testing.T) *monitor.Recorder {
	t.Helper()
	rec := monitor.StartRecording()
	t.Cleanup(rec.Stop)
	return rec
}

// captureLog redirects the standard logger — the stdout channel — for one test.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prevOut, prevFlags := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	return &buf
}

func dataOf(t *testing.T, e monitor.Event) map[string]any {
	t.Helper()
	m, ok := e.Data.(map[string]any)
	if !ok {
		t.Fatalf("event %s carries %T, want a data map", e.Name, e.Data)
	}
	return m
}

// TestCoalescedStormEmitsOncePerWindow is the loop guard at its source. On
// appleby-core the event reporting a dropped batch lands in the queue that is
// dropping batches; a hundred failures in one window must cost one event, and
// the count of the other ninety-nine must still arrive.
func TestCoalescedStormEmitsOncePerWindow(t *testing.T) {
	clock := withFakeClock(t)
	rec := recordEvents(t)
	captureLog(t)

	for i := 0; i < 100; i++ {
		ErrorCoalesced(context.Background(), "batch.write.dropped", "batch.write.dropped", errors.New("clickhouse down"), map[string]any{"batch_size": i})
		clock.Advance(100 * time.Millisecond)
	}

	got := rec.Named("batch.write.dropped")
	if len(got) != 1 {
		t.Fatalf("a 100-failure storm inside one window emitted %d events, want 1", len(got))
	}
	first := dataOf(t, got[0])
	if fmt.Sprint(first["suppressed"]) != "0" {
		t.Errorf("first event suppressed = %v, want 0", first["suppressed"])
	}
	if got[0].Level != monitor.LevelError || first["stack_trace"] == nil || first["error"] != "clickhouse down" {
		t.Errorf("first event is not a full error event: level=%s data=%v", got[0].Level, first)
	}

	// The window closes: the sweeper replays the last event with the count.
	clock.Advance(COALESCE_WINDOW)
	SweepCoalesced()
	got = rec.Named("batch.write.dropped")
	if len(got) != 2 {
		t.Fatalf("after the window closed there are %d events, want 2 (the first and one trailing count)", len(got))
	}
	trailing := dataOf(t, got[1])
	if fmt.Sprint(trailing["suppressed"]) != "99" || trailing["trailing"] != true {
		t.Errorf("trailing event = %v, want suppressed=99 trailing=true", trailing)
	}
	// It describes the failure as the window closed, not as it opened: the last
	// occurrence's fields, not the first's.
	if fmt.Sprint(trailing["batch_size"]) != "99" || fmt.Sprint(first["batch_size"]) != "0" {
		t.Errorf("trailing batch_size = %v (first %v), want the latest occurrence's 99", trailing["batch_size"], first["batch_size"])
	}
	if _, has := trailing["stack_trace"]; has {
		t.Error("the trailing replay carries a stack; it repeats a report, it is not a new failure")
	}

	// A window with no occurrences forgets the key, so the next failure is
	// reported at once rather than waiting out a stale window.
	clock.Advance(2 * COALESCE_WINDOW)
	SweepCoalesced()
	if n := len(rec.Named("batch.write.dropped")); n != 2 {
		t.Fatalf("a quiet window emitted an event: %d, want 2", n)
	}
	ErrorCoalesced(context.Background(), "batch.write.dropped", "batch.write.dropped", errors.New("clickhouse down"), nil)
	got = rec.Named("batch.write.dropped")
	if len(got) != 3 || fmt.Sprint(dataOf(t, got[2])["suppressed"]) != "0" {
		t.Fatalf("the first failure after a quiet window was not reported fresh: %d events", len(got))
	}
}

func TestCoalescingKeysAreIndependent(t *testing.T) {
	withFakeClock(t)
	rec := recordEvents(t)
	captureLog(t)

	for i := 0; i < 5; i++ {
		WarnCoalesced(context.Background(), "alert.notify.failed:ch-1", "alert.notify.failed", nil, nil)
		WarnCoalesced(context.Background(), "alert.notify.failed:ch-2", "alert.notify.failed", nil, nil)
	}
	if n := len(rec.Named("alert.notify.failed")); n != 2 {
		t.Fatalf("two failing channels emitted %d events, want one each", n)
	}
}

// TestStdoutPolicy pins the last-resort channel: warnings and errors always
// reach the container log, info only while booting or stopping.
func TestStdoutPolicy(t *testing.T) {
	captureLog(t)
	Init(Settings{Env: "test"})
	t.Cleanup(func() {
		initialised.Store(false)
		sdkStdout.Store(false)
		monitor.Shutdown()
	})

	tests := []struct {
		name      string
		booting   bool
		sdkStdout bool
		level     string
		printed   bool
	}{
		{name: "info while booting", booting: true, level: monitor.LevelInfo, printed: true},
		{name: "info while serving", booting: false, level: monitor.LevelInfo, printed: false},
		{name: "warn while serving", booting: false, level: monitor.LevelWarn, printed: true},
		{name: "error while serving", booting: false, level: monitor.LevelError, printed: true},
		{name: "fatal while serving", booting: false, level: monitor.LevelFatal, printed: true},
		{name: "warn when the SDK already prints", booting: false, sdkStdout: true, level: monitor.LevelWarn, printed: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureLog(t)
			booting.Store(tt.booting)
			sdkStdout.Store(tt.sdkStdout)

			At(context.Background(), tt.level, "policy.probe.event", map[string]any{"k": "v"})

			if got := strings.Contains(buf.String(), "policy.probe.event"); got != tt.printed {
				t.Errorf("printed = %v, want %v (output %q)", got, tt.printed, buf.String())
			}
		})
	}
}

// TestStdoutLineRedacts: the stdout line is written here, not by the SDK, so
// it carries its own redaction.
func TestStdoutLineRedacts(t *testing.T) {
	line := formatLine(monitor.LevelWarn, "mariadb.connect.retrying", map[string]any{
		"error":         "dial monitor:hunter2@tcp(mariadb:3306)/monitor_auth failed; Authorization: Bearer abcdefghijklmnop",
		"client_secret": "s3cr3t-value",
		"detail":        "Duplicate entry 'jane@example.com' for key 'uq_users_email'",
		"stack_trace":   "goroutine 1 [running]",
		"attempt":       3,
	})

	for _, leaked := range []string{"hunter2", "abcdefghijklmnop", "s3cr3t-value", "jane@example.com", "goroutine 1"} {
		if strings.Contains(line, leaked) {
			t.Errorf("stdout line leaks %q: %s", leaked, line)
		}
	}
	if !strings.Contains(line, "attempt=3") || !strings.HasPrefix(line, "WARN mariadb.connect.retrying") {
		t.Errorf("stdout line lost its shape: %s", line)
	}
}

// queryErr stands in for db.QueryError: a message that quotes a bound value, a
// safe rendering of it, and context of its own.
type queryErr struct{}

func (queryErr) Error() string          { return "Code: 62. Syntax error near 'jane@example.com'" }
func (queryErr) TelemetryError() string { return "Code: 62. Syntax error near '…'" }
func (queryErr) TelemetryFields() map[string]any {
	return map[string]any{"query_kind": "services.QueryEvents", "duration_ms": int64(12)}
}

func TestErrorCarriesSafeTextAndItsOwnContext(t *testing.T) {
	rec := recordEvents(t)
	captureLog(t)

	Error(context.Background(), "clickhouse.query.failed", fmt.Errorf("failed to count events: %w", queryErr{}), map[string]any{
		"query_kind": "caller-supplied wins",
	})

	got := rec.Named("clickhouse.query.failed")
	if len(got) != 1 {
		t.Fatalf("want one event, got %d", len(got))
	}
	data := dataOf(t, got[0])
	if data["error"] != "failed to count events: Code: 62. Syntax error near '…'" {
		t.Errorf("error = %q, want the wrapper's context with the literal elided", data["error"])
	}
	if data["error_type"] != "telemetry.queryErr" {
		t.Errorf("error_type = %v, want the innermost type", data["error_type"])
	}
	if data["query_kind"] != "caller-supplied wins" || fmt.Sprint(data["duration_ms"]) != "12" {
		t.Errorf("context not merged correctly: %v", data)
	}
	if s, _ := data["stack_trace"].(string); !strings.Contains(s, "TestErrorCarriesSafeTextAndItsOwnContext") {
		t.Errorf("stack does not reach the caller: %.200q", s)
	}
}

func TestEmailsAreScrubbedFromFreeText(t *testing.T) {
	rec := recordEvents(t)
	captureLog(t)

	Warn(context.Background(), "alert.notify.failed", map[string]any{
		"error": "550 5.1.1 <oncall@example.com>: Recipient address rejected",
	})

	data := dataOf(t, rec.Named("alert.notify.failed")[0])
	if strings.Contains(fmt.Sprint(data["error"]), "oncall@example.com") {
		t.Errorf("address survived: %v", data["error"])
	}
}

// TestGoroutinePanicIsReported: a panic in a goroutine kills the process and
// everything buffered with it unless the goroutine reports it itself.
func TestGoroutinePanicIsReported(t *testing.T) {
	rec := recordEvents(t)
	captureLog(t)

	Go(context.Background(), "test-loop", func(context.Context) { panic("boom") })

	deadline := time.Now().Add(2 * time.Second)
	for len(rec.Named("panic.recovered")) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	got := rec.Named("panic.recovered")
	if len(got) != 1 {
		t.Fatalf("panic.recovered events = %d, want 1", len(got))
	}
	data := dataOf(t, got[0])
	if data["goroutine"] != "test-loop" || data["error"] != "boom" || got[0].Level != monitor.LevelError {
		t.Errorf("panic event = level %s %v", got[0].Level, data)
	}
	if s, _ := data["stack_trace"].(string); s == "" {
		t.Error("panic event has no stack")
	}
}

func TestFatalReportsThenExits(t *testing.T) {
	rec := recordEvents(t)
	captureLog(t)
	code := -1
	prev := exit
	exit = func(c int) { code = c }
	t.Cleanup(func() { exit = prev })

	Fatal("clickhouse.connect.failed", "failed to connect to ClickHouse", errors.New("dial tcp: connection refused"), map[string]any{"reason": "clickhouse_unreachable"})

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	got := rec.Named("clickhouse.connect.failed")
	if len(got) != 1 || got[0].Level != monitor.LevelFatal {
		t.Fatalf("want one fatal event, got %d", len(got))
	}
	data := dataOf(t, got[0])
	if data["message"] != "failed to connect to ClickHouse" || data["reason"] != "clickhouse_unreachable" || data["error"] == nil {
		t.Errorf("fatal event lacks its cause: %v", data)
	}
}

// TestStopFlushDeliversBeforeTheListenerCloses pins main's stop order.
// appleby-core ships to its own listener, which server.Shutdown closes; what is
// still buffered after that (the stop event, a coalesced failure's trailing
// count) is lost without a spool. Flush must have delivered both by the time it
// returns.
func TestStopFlushDeliversBeforeTheListenerCloses(t *testing.T) {
	clock := withFakeClock(t)
	captureLog(t)

	var mu sync.Mutex
	var received bytes.Buffer
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := io.Reader(r.Body)
		if r.Header.Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			body = zr
		}
		mu.Lock()
		_, _ = io.Copy(&received, body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// The shipper alone, not Init: a ticking flush would deliver on its own and
	// prove nothing, and Init's sweeper would outlive the test.
	if err := monitor.Init(monitor.Config{
		Service:       Service,
		IngestURL:     srv.URL + "/v1/events",
		APIKey:        "test-key",
		FlushEvery:    time.Hour,
		DisableStdout: true,
	}); err != nil {
		t.Fatalf("monitor.Init: %v", err)
	}
	t.Cleanup(monitor.Shutdown)

	cause := errors.New("dial tcp [::1]:9000: connect: connection refused")
	WarnCoalesced(context.Background(), "test.stop-flush", "batch.write.retrying", cause, map[string]any{"batch_size": 1})
	clock.Advance(time.Second)
	WarnCoalesced(context.Background(), "test.stop-flush", "batch.write.retrying", cause, map[string]any{"batch_size": 2})
	Info(context.Background(), "service.stopping", map[string]any{"signal": "terminated"})

	Flush(2 * time.Second)

	mu.Lock()
	got := received.String()
	mu.Unlock()
	for _, want := range []string{`"service.stopping"`, `"trailing":true`, `"suppressed":1`} {
		if !strings.Contains(got, want) {
			t.Errorf("after Flush the ingest endpoint has not seen %s; it received %q", want, got)
		}
	}
}
