package routes

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/services"
	"github.com/aidenappl/monitor-core/structs"
)

// withTestQueue swaps the package-level Queue for a private one and drains it
// into a slice, so a test can inspect exactly what the handler enqueued rather
// than only the status code it returned.
func withTestQueue(t *testing.T, size int) *services.Queue {
	t.Helper()
	previous := Queue
	Queue = services.NewQueue(size)
	t.Cleanup(func() { Queue = previous })
	return Queue
}

// drainQueue reads everything currently buffered without blocking.
func drainQueue(q *services.Queue) []*structs.Event {
	var out []*structs.Event
	for {
		select {
		case event := <-q.Events():
			out = append(out, event)
		default:
			return out
		}
	}
}

// TestIngestOverwritesClientSuppliedProject is the test the Project field
// exists for.
//
// Nothing else enforces tenancy on the write path: the project column is not
// derived from anything ClickHouse checks, so if a client-supplied "project" in
// the NDJSON body survived into the stored row, any holder of any ingest key
// could file events under any other tenant's project — and the forged rows would
// be indistinguishable from that tenant's real traffic afterwards. The overwrite
// in IngestEventsHandler is the whole boundary, so it is asserted, not assumed.
func TestIngestOverwritesClientSuppliedProject(t *testing.T) {
	q := withTestQueue(t, 8)

	// Line 1 names a project it has no right to; line 2 names none at all. Both
	// must come out stamped with the project the CREDENTIAL resolved to.
	lines := []string{
		`{"timestamp":"2026-09-06T12:00:00Z","service":"svc-a","name":"request","level":"info","project":"victim-project"}`,
		`{"timestamp":"2026-09-06T12:00:01Z","service":"svc-b","name":"request","level":"info"}`,
	}

	// Guard against a false green. If Event.Project ever stopped being
	// unmarshalled from the body — a renamed json tag, say — the assertions
	// below would pass while proving nothing, because the attacker's value would
	// never have been in the struct to overwrite in the first place.
	var probe structs.Event
	if err := json.Unmarshal([]byte(lines[0]), &probe); err != nil {
		t.Fatalf("probe unmarshal failed: %v", err)
	}
	if probe.Project != "victim-project" {
		t.Fatalf("Project = %q after unmarshal, want %q — the client-supplied field is not even being parsed, so this test cannot prove it is discarded",
			probe.Project, "victim-project")
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(strings.Join(lines, "\n")))
	req = req.WithContext(middleware.WithProject(req.Context(), "key-project"))
	rec := httptest.NewRecorder()

	IngestEventsHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp struct {
		Accepted int `json:"accepted"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response %q: %v", rec.Body.String(), err)
	}
	if resp.Accepted != len(lines) {
		t.Fatalf("accepted = %d, want %d", resp.Accepted, len(lines))
	}

	enqueued := drainQueue(q)
	if len(enqueued) != len(lines) {
		t.Fatalf("enqueued %d events, want %d", len(enqueued), len(lines))
	}
	for i, event := range enqueued {
		if event.Project != "key-project" {
			t.Errorf("event %d (%s): Project = %q, want %q", i, event.Service, event.Project, "key-project")
		}
	}
}

// TestIngestStampsProjectOnEveryEventUnconditionally pins the "unconditionally"
// half of the contract separately, because it is the half an optimisation would
// break first: a `if event.Project == ""` guard added to "respect what the
// client sent" reads as harmless and re-opens the whole hole. Every event in the
// batch carries a different pre-set project here, so any skip-when-non-empty
// behaviour survives into the queue and fails this.
func TestIngestStampsProjectOnEveryEventUnconditionally(t *testing.T) {
	q := withTestQueue(t, 8)

	lines := []string{
		`{"timestamp":"2026-09-06T12:00:00Z","service":"svc-a","name":"request","level":"info","project":"tenant-one"}`,
		`{"timestamp":"2026-09-06T12:00:01Z","service":"svc-b","name":"request","level":"info","project":"tenant-two"}`,
		`{"timestamp":"2026-09-06T12:00:02Z","service":"svc-c","name":"request","level":"info","project":""}`,
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(strings.Join(lines, "\n")))
	req = req.WithContext(middleware.WithProject(req.Context(), "key-project"))
	rec := httptest.NewRecorder()

	IngestEventsHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusOK, rec.Body.String())
	}

	enqueued := drainQueue(q)
	if len(enqueued) != len(lines) {
		t.Fatalf("enqueued %d events, want %d", len(enqueued), len(lines))
	}
	for i, event := range enqueued {
		if event.Project != "key-project" {
			t.Errorf("event %d (%s): Project = %q, want %q", i, event.Service, event.Project, "key-project")
		}
	}
}
