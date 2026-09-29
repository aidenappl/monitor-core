package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	monitor "github.com/aidenappl/go-monitor"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/routes"
	"github.com/aidenappl/monitor-core/services"
	"github.com/aidenappl/monitor-core/telemetry"
)

// These run the ingest path through the REAL router — middleware stack,
// IngestAuthMiddleware and IngestEventsHandler as buildRouter wires them —
// because the loop guards are properties of the whole stack, not of one layer.

const testIngestKey = "self-telemetry-test-master-key"

func withIngest(t *testing.T, queueSize int) {
	t.Helper()
	prevKey, prevQueue := env.IngestKey, routes.Queue
	env.IngestKey = testIngestKey
	routes.Queue = services.NewQueue(queueSize)
	t.Cleanup(func() {
		env.IngestKey, routes.Queue = prevKey, prevQueue
	})
}

// freshCoalescing gives a test its own coalescing state, so an earlier test's
// window cannot suppress this one's first event.
func freshCoalescing(t *testing.T) {
	t.Helper()
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	t.Cleanup(telemetry.SetClockForTest(func() time.Time { return now }))
}

func ingest(t *testing.T, key, contentEncoding, body string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-ndjson")
	if key != "" {
		req.Header.Set("X-Api-Key", key)
	}
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	w := httptest.NewRecorder()
	buildRouter(env.RoleZone).ServeHTTP(w, req)
	return w.Code
}

// TestSuccessfulIngestEmitsNothing is the primary loop guard. appleby-core
// ships its own telemetry to its own ingest endpoint: if a successful batch
// produced an event, that event would ride the next batch and produce another,
// once per flush interval, forever.
func TestSuccessfulIngestEmitsNothing(t *testing.T) {
	withIngest(t, 100)
	freshCoalescing(t)
	rec := monitor.StartRecording()
	t.Cleanup(rec.Stop)

	body := `{"timestamp":"2026-09-13T12:00:00Z","service":"appleby-core","name":"batch.write.dropped","level":"error","data":{"batch_size":3}}` + "\n"
	for i := 0; i < 20; i++ {
		if code := ingest(t, testIngestKey, "", body); code != http.StatusOK {
			t.Fatalf("ingest %d answered %d", i, code)
		}
	}

	if events := rec.Events(); len(events) != 0 {
		t.Fatalf("20 successful ingest requests emitted %d events (first: %s), want 0", len(events), events[0].Name)
	}
}

// TestIngestRejectionsAreCoalesced: a producer with a bug sends the same bad
// line on every retry, and each rejection must not become an event per retry.
func TestIngestRejectionsAreCoalesced(t *testing.T) {
	withIngest(t, 100)
	freshCoalescing(t)
	rec := monitor.StartRecording()
	t.Cleanup(rec.Stop)

	for i := 0; i < 10; i++ {
		ingest(t, testIngestKey, "", `{"service":"svc"`+"\n")
	}

	got := rec.Named("ingest.request.rejected")
	if len(got) != 1 || len(rec.Events()) != 1 {
		t.Fatalf("10 rejected bodies emitted %d events, want 1 ingest.request.rejected", len(rec.Events()))
	}
	if got[0].Level != monitor.LevelWarn || got[0].Data.(map[string]any)["reason"] != "invalid_json" {
		t.Errorf("rejection = %s %v", got[0].Level, got[0].Data)
	}
}

// TestTenantEventDataNeverReachesSelfTelemetry: monitor-core holds every
// Trailblaze service's events, and its self-telemetry lands in the appleby
// zone. A failing ingest path that copied a rejected event's data, name,
// message or timestamp into its report would move one zone's data into another.
// Every failing path is driven with a marker in every tenant-owned field.
func TestTenantEventDataNeverReachesSelfTelemetry(t *testing.T) {
	const marker = "LEAKMARKER"
	valid := `{"timestamp":"2026-09-13T12:00:00Z","service":"svc","name":"` + marker + `.name","level":"error","data":{"error":"` + marker + `","path":"/` + marker + `","email":"` + marker + `@example.com"}}` + "\n"

	tests := []struct {
		name            string
		queueSize       int
		key             string
		contentEncoding string
		body            string
		wantEvent       string
	}{
		{
			name: "unparseable timestamp", queueSize: 10, key: testIngestKey, wantEvent: "ingest.request.rejected",
			body: `{"timestamp":"` + marker + `","service":"svc","name":"` + marker + `","level":"error","data":{"secret":"` + marker + `"}}` + "\n",
		},
		{
			name: "invalid correlation id", queueSize: 10, key: testIngestKey, wantEvent: "ingest.request.rejected",
			body: `{"timestamp":"2026-09-13T12:00:00Z","service":"svc","name":"` + marker + `","level":"error","request_id":"` + marker + `","data":{"secret":"` + marker + `"}}` + "\n",
		},
		{
			name: "malformed JSON", queueSize: 10, key: testIngestKey, wantEvent: "ingest.request.rejected",
			body: `{"timestamp":"2026-09-13T12:00:00Z","data":{"secret":"` + marker + `"` + "\n",
		},
		{
			name: "wrong field type", queueSize: 10, key: testIngestKey, wantEvent: "ingest.request.rejected",
			body: `{"timestamp":"2026-09-13T12:00:00Z","service":"svc","name":"n","level":"error","data":"` + marker + `"}` + "\n",
		},
		{
			name: "not gzip", queueSize: 10, key: testIngestKey, contentEncoding: "gzip", wantEvent: "ingest.request.rejected",
			body: valid,
		},
		{
			name: "queue full", queueSize: 0, key: testIngestKey, wantEvent: "ingest.queue.overflow",
			body: valid + valid,
		},
		{
			// The key itself is not tenant data — its 12-character prefix is
			// reported by design, as the admin UI shows it — so the marker rides
			// in the body the refused producer sent.
			name: "refused credential", queueSize: 10, key: "0123456789abcdef-revoked-key-value", wantEvent: "ingest.auth.rejected",
			body: valid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withIngest(t, tt.queueSize)
			freshCoalescing(t)
			rec := monitor.StartRecording()
			defer rec.Stop()

			ingest(t, tt.key, tt.contentEncoding, tt.body)

			// Non-vacuous: the failure WAS reported, so the absence of the marker
			// below means something.
			if len(rec.Named(tt.wantEvent)) != 1 {
				t.Fatalf("want one %s, got %d events", tt.wantEvent, len(rec.Events()))
			}
			for _, e := range rec.Events() {
				line, err := e.ToJSON()
				if err != nil {
					t.Fatalf("marshal %s: %v", e.Name, err)
				}
				if strings.Contains(string(line), marker) {
					t.Errorf("%s carries tenant data: %s", e.Name, line)
				}
			}
		})
	}
}
