package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aidenappl/monitor-core/env"
)

// ingestProbe returns an ingest handler that records whether it ran.
func ingestProbe(called *bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		*called = true
		w.WriteHeader(http.StatusOK)
	}
}

// TestIngestAuthAcceptsMasterKey covers the env-master-key branch, which is
// deliberately still accepted here: the whole deployed fleet presents it.
func TestIngestAuthAcceptsMasterKey(t *testing.T) {
	withEnvIngestKey(t, "master-key-value")

	var called bool
	req := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
	req.Header.Set("X-Api-Key", "master-key-value")
	rec := httptest.NewRecorder()

	IngestAuthMiddleware(ingestProbe(&called)).ServeHTTP(rec, req)

	if !called {
		t.Fatalf("ingest handler was not called; status = %d", rec.Code)
	}
}

// TestIngestAuthRejectsNonMatchingKeys guards the constant-time master-key
// comparison against being loosened into a prefix or emptiness check, and pins
// that a key absent from the api-key cache does not get in.
func TestIngestAuthRejectsNonMatchingKeys(t *testing.T) {
	withEnvIngestKey(t, "master-key-value")

	tests := []struct {
		name string
		key  string
	}{
		{name: "no key at all", key: ""},
		{name: "wrong value", key: "not-the-master-key"},
		{name: "prefix of the master key", key: "master-key"},
		{name: "master key with suffix", key: "master-key-value-extra"},
		{name: "unknown db key", key: "raw-key-not-in-the-cache"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var called bool
			req := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
			if tt.key != "" {
				req.Header.Set("X-Api-Key", tt.key)
			}
			rec := httptest.NewRecorder()

			IngestAuthMiddleware(ingestProbe(&called)).ServeHTTP(rec, req)

			if called {
				t.Errorf("ingest handler ran for key %q", tt.key)
			}
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
			}
		})
	}
}

// TestIngestAuthEmptyMasterKeyIsNotAMatch pins the emptiness guard in
// matchesEnvMasterKey. subtle.ConstantTimeCompare("", "") returns 1, so without
// it an unset MONITOR_API_KEY would let every unauthenticated POST write events.
func TestIngestAuthEmptyMasterKeyIsNotAMatch(t *testing.T) {
	withEnvIngestKey(t, "")

	var called bool
	req := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
	req.Header.Set("X-Api-Key", "")
	rec := httptest.NewRecorder()

	IngestAuthMiddleware(ingestProbe(&called)).ServeHTTP(rec, req)

	if called {
		t.Error("ingest handler ran with an unset master key and an empty header")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// withDefaultProject pins env.DefaultProjectSlug for the duration of a test.
// env.Load() never runs under `go test`, so the var is empty unless a test sets
// it — asserting against the real default here would be asserting against "".
func withDefaultProject(t *testing.T, slug string) {
	t.Helper()
	previous := env.DefaultProjectSlug
	env.DefaultProjectSlug = slug
	t.Cleanup(func() { env.DefaultProjectSlug = previous })
}

// captureIngestProject returns an ingest handler that records the project the
// middleware injected, so a test can assert on the tenant decision rather than
// on a status code alone.
func captureIngestProject(seen *string, called *bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		*called = true
		if project, ok := GetProject(r.Context()); ok {
			*seen = project
		}
		w.WriteHeader(http.StatusOK)
	}
}

// TestIngestAuthStampsDefaultProjectForMasterKey covers the credential that has
// no api_keys row and therefore no tenant binding of its own.
//
// It is the whole fleet's ingest key, so getting this wrong does not produce a
// handful of odd rows — it produces every event Monitor receives with an empty
// project, which no project-scoped filter can ever select again. Mimir's
// "anonymous" and Loki's "fake" exist for this case; the assertion is that
// Monitor behaves the same way and never leaves the dimension null.
func TestIngestAuthStampsDefaultProjectForMasterKey(t *testing.T) {
	withEnvIngestKey(t, "master-key-value")
	withDefaultProject(t, "default")

	var seen string
	var called bool
	req := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
	req.Header.Set("X-Api-Key", "master-key-value")
	rec := httptest.NewRecorder()

	IngestAuthMiddleware(captureIngestProject(&seen, &called)).ServeHTTP(rec, req)

	if !called {
		t.Fatalf("ingest handler was not called; status = %d", rec.Code)
	}
	if seen != "default" {
		t.Errorf("injected project = %q, want %q", seen, "default")
	}
}

// TestIngestAuthRejectionInjectsNoProject pins that an unauthenticated request
// never reaches the handler with a tenant attached. A rejected request that
// still carried a project would be one refactor away from being a request that
// still wrote events under it.
func TestIngestAuthRejectionInjectsNoProject(t *testing.T) {
	withEnvIngestKey(t, "master-key-value")
	withDefaultProject(t, "default")

	var seen string
	var called bool
	req := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
	req.Header.Set("X-Api-Key", "not-the-master-key")
	rec := httptest.NewRecorder()

	IngestAuthMiddleware(captureIngestProject(&seen, &called)).ServeHTTP(rec, req)

	if called {
		t.Fatal("ingest handler ran for an invalid key")
	}
	if seen != "" {
		t.Errorf("injected project = %q, want none", seen)
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}
