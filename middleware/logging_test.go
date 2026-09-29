package middleware

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	monitor "github.com/aidenappl/go-monitor"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/jwt"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/telemetry"
	"github.com/gorilla/mux"
)

func record(t *testing.T) *monitor.Recorder {
	t.Helper()
	rec := monitor.StartRecording()
	t.Cleanup(rec.Stop)
	return rec
}

// instrumented is the production middleware order: RequestID → Logging →
// Recover, as buildRouter registers it.
func instrumented() *mux.Router {
	r := mux.NewRouter()
	r.Use(RequestIDMiddleware)
	r.Use(LoggingMiddleware)
	r.Use(RecoverMiddleware)
	return r
}

func names(events []monitor.Event) []string {
	var out []string
	for _, e := range events {
		out = append(out, e.Name+"/"+e.Level)
	}
	return out
}

// TestServerErrorIsOneEnrichedEvent: a 5xx that reaches the responder is
// reported by exactly one event — the request's — carrying the cause, its type,
// the stack inside the handler and the handler's own context. A second
// CaptureErrorAs in the handler would file a second issue for one failure.
func TestServerErrorIsOneEnrichedEvent(t *testing.T) {
	rec := record(t)
	r := instrumented()
	r.HandleFunc("/v1/alert-rules/{id}", func(w http.ResponseWriter, r *http.Request) {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete alert rule",
			fmt.Errorf("delete rule: %w", errors.New("mariadb: connection refused")),
			map[string]any{"rule_id": "rule-1"})
	}).Methods(http.MethodDelete)

	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodDelete, "/v1/alert-rules/rule-1", nil))

	events := rec.Events()
	if len(events) != 1 {
		t.Fatalf("a 5xx produced %d events %v, want exactly one", len(events), names(events))
	}
	e := events[0]
	if e.Name != "http.request.end" || e.Level != monitor.LevelError {
		t.Fatalf("event = %s/%s, want http.request.end/error", e.Name, e.Level)
	}
	if e.RequestID == "" {
		t.Error("the request event carries no request_id")
	}
	data := e.Data.(map[string]any)
	checks := map[string]string{
		"path":          "/v1/alert-rules/{id}",
		"status_code":   "500",
		"error":         "delete rule: mariadb: connection refused",
		"error_type":    "*errors.errorString",
		"error_message": "failed to delete alert rule",
		"rule_id":       "rule-1",
		"outcome":       "returned 500",
	}
	for key, want := range checks {
		if got := fmt.Sprint(data[key]); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	if s, _ := data["stack_trace"].(string); !strings.Contains(s, "TestServerErrorIsOneEnrichedEvent") {
		t.Errorf("stack does not reach the handler that failed: %.300q", s)
	}
}

// TestSuccessfulAuthenticatedReadEmitsOneEvent is the noise budget: a
// successful read costs one info event, attributed to its user, scoped to its
// project, and nothing else.
func TestSuccessfulAuthenticatedReadEmitsOneEvent(t *testing.T) {
	withRole(t, env.RoleZone) // a zone trusts the token and never touches MariaDB
	rec := record(t)

	token, _, err := jwt.NewAccessToken(42, "viewer", "0a0b0c0d")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	r := instrumented()
	v1 := r.PathPrefix("/v1").Subrouter()
	v1.Use(QueryAuthMiddleware)
	v1.HandleFunc("/issues", func(w http.ResponseWriter, r *http.Request) {
		responder.New(w, []string{})
	}).Methods(http.MethodGet)

	req := httptest.NewRequest(http.MethodGet, "/v1/issues", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	events := rec.Events()
	if len(events) != 1 {
		t.Fatalf("a successful read produced %d events %v, want 1", len(events), names(events))
	}
	e := events[0]
	if e.Name != "http.request.end" || e.Level != monitor.LevelInfo || e.UserID != "42" {
		t.Errorf("event = %s/%s user=%q, want http.request.end/info user=42", e.Name, e.Level, e.UserID)
	}
	data := e.Data.(map[string]any)
	if data["auth"] != "session" || data["project"] != env.DefaultProjectSlug {
		t.Errorf("event not annotated with its credential and scope: %v", data)
	}
}

// TestRequestLevels pins the level policy for the request event.
func TestRequestLevels(t *testing.T) {
	tests := []struct {
		name      string
		method    string
		path      string
		handler   http.HandlerFunc
		wantCount int
		wantLevel string
	}{
		{
			name: "caller mistake is a warning", method: http.MethodPost, path: "/v1/views",
			handler: func(w http.ResponseWriter, r *http.Request) {
				responder.Error(w, http.StatusBadRequest, "invalid request body")
			},
			wantCount: 1, wantLevel: monitor.LevelWarn,
		},
		{
			name: "expired session is routine", method: http.MethodGet, path: "/v1/views",
			handler: func(w http.ResponseWriter, r *http.Request) {
				SessionMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(w, r)
			},
			wantCount: 1, wantLevel: monitor.LevelInfo,
		},
		{
			name: "successful ingest emits nothing", method: http.MethodPost, path: INGEST_PATH,
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
			wantCount: 0,
		},
		{
			name: "rejected ingest is reported by the ingest path, not here", method: http.MethodPost, path: INGEST_PATH,
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
			},
			wantCount: 0,
		},
		{
			name: "a failing ingest is still an error", method: http.MethodPost, path: INGEST_PATH,
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			wantCount: 1, wantLevel: monitor.LevelError,
		},
		{
			name: "an event query is not ingest", method: http.MethodGet, path: INGEST_PATH,
			handler: func(w http.ResponseWriter, r *http.Request) {
				responder.New(w, nil)
			},
			wantCount: 1, wantLevel: monitor.LevelInfo,
		},
		{
			name: "health probes emit nothing", method: http.MethodGet, path: "/health",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := record(t)
			r := instrumented()
			r.HandleFunc(tt.path, tt.handler).Methods(tt.method)

			r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(tt.method, tt.path, nil))

			events := rec.Events()
			if len(events) != tt.wantCount {
				t.Fatalf("events = %v, want %d", names(events), tt.wantCount)
			}
			if tt.wantCount == 1 && events[0].Level != tt.wantLevel {
				t.Errorf("level = %s, want %s", events[0].Level, tt.wantLevel)
			}
		})
	}
}

// TestPanicFilesOneIssue: a panic is reported as panic.recovered, and the
// request event that records the 500 stays a warning so the one failure files
// one issue.
func TestPanicFilesOneIssue(t *testing.T) {
	rec := record(t)
	r := instrumented()
	r.HandleFunc("/v1/dashboards", func(http.ResponseWriter, *http.Request) {
		panic("nil map")
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/dashboards", nil))

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	errorsSeen := 0
	for _, e := range rec.Events() {
		if e.Level == monitor.LevelError {
			errorsSeen++
		}
	}
	if errorsSeen != 1 || len(rec.Named("panic.recovered")) != 1 || len(rec.Named("http.request.end")) != 1 {
		t.Fatalf("events = %v, want one panic.recovered/error and one http.request.end/warn", names(rec.Events()))
	}
}

// TestIngestAuthRejectionIsCoalescedAndNeverCarriesTheKey: a producer with a
// revoked key retries every batch — appleby-core's own telemetry among them.
func TestIngestAuthRejectionIsCoalescedAndNeverCarriesTheKey(t *testing.T) {
	withEnvIngestKey(t, "master-key-value")
	clock := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	t.Cleanup(telemetry.SetClockForTest(func() time.Time { return clock }))
	rec := record(t)

	const presented = "0123456789abcdef0123456789abcdef-revoked"
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodPost, INGEST_PATH, nil)
		req.Header.Set("X-Api-Key", presented)
		IngestAuthMiddleware(ingestProbe(new(bool))).ServeHTTP(httptest.NewRecorder(), req)
	}

	got := rec.Named("ingest.auth.rejected")
	if len(got) != 1 {
		t.Fatalf("5 refusals emitted %d events, want 1", len(got))
	}
	data := got[0].Data.(map[string]any)
	if data["reason"] != "unknown_key" || data["key_prefix"] != presented[:12] {
		t.Errorf("rejection = %v", data)
	}
	line, _ := got[0].ToJSON()
	if strings.Contains(string(line), presented) {
		t.Errorf("the rejection carries the whole presented key: %s", line)
	}
}
