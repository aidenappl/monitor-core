package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/structs"
)

// captureActor returns a handler that records the actor it saw, so a test can
// assert on what the middleware injected rather than on a status code alone.
func captureActor(seen **structs.Actor, called *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*called = true
		if actor, ok := GetActor(r.Context()); ok {
			*seen = actor
		}
		w.WriteHeader(http.StatusOK)
	})
}

func withEnvIngestKey(t *testing.T, key string) {
	t.Helper()
	previous := env.IngestKey
	env.IngestKey = key
	t.Cleanup(func() { env.IngestKey = previous })
}

// TestQueryAuthInjectsSystemActorForMasterKey covers the env-master-key branch.
// The key has no database row, so it can only ever be attributed to a fixed
// system label — the case that would otherwise produce an unattributed audit row.
func TestQueryAuthInjectsSystemActorForMasterKey(t *testing.T) {
	withEnvIngestKey(t, "master-key-value")

	var seen *structs.Actor
	var called bool

	req := httptest.NewRequest(http.MethodGet, "/v1/issues", nil)
	req.Header.Set("X-Api-Key", "master-key-value")
	rec := httptest.NewRecorder()

	QueryAuthMiddleware(captureActor(&seen, &called)).ServeHTTP(rec, req)

	if !called {
		t.Fatalf("next handler was not called; status = %d", rec.Code)
	}
	if seen == nil {
		t.Fatal("no actor was injected into the request context")
	}
	if seen.Kind != structs.ActorKindSystem {
		t.Errorf("Kind = %q, want %q", seen.Kind, structs.ActorKindSystem)
	}
	if seen.Label != EnvMasterKeyLabel {
		t.Errorf("Label = %q, want %q", seen.Label, EnvMasterKeyLabel)
	}
	if !seen.Kind.IsValid() {
		t.Errorf("injected actor kind %q is not valid", seen.Kind)
	}
}

// TestQueryAuthRejectsUnauthenticated asserts the session fallback still 401s
// when nothing authenticates, and that no actor leaks into the context.
func TestQueryAuthRejectsUnauthenticated(t *testing.T) {
	withEnvIngestKey(t, "master-key-value")

	var seen *structs.Actor
	var called bool

	req := httptest.NewRequest(http.MethodGet, "/v1/issues", nil)
	rec := httptest.NewRecorder()

	QueryAuthMiddleware(captureActor(&seen, &called)).ServeHTTP(rec, req)

	if called {
		t.Error("next handler ran for an unauthenticated request")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if seen != nil {
		t.Errorf("actor %+v was injected for an unauthenticated request", seen)
	}
}

// TestQueryAuthWrongMasterKeyDoesNotAuthenticate guards against the master-key
// comparison being loosened into a prefix or emptiness check.
func TestQueryAuthWrongMasterKeyDoesNotAuthenticate(t *testing.T) {
	withEnvIngestKey(t, "master-key-value")

	tests := []struct {
		name string
		key  string
	}{
		{name: "wrong value", key: "not-the-master-key"},
		{name: "prefix of the master key", key: "master-key"},
		{name: "master key with suffix", key: "master-key-value-extra"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seen *structs.Actor
			var called bool

			req := httptest.NewRequest(http.MethodGet, "/v1/issues", nil)
			req.Header.Set("X-Api-Key", tt.key)
			rec := httptest.NewRecorder()

			QueryAuthMiddleware(captureActor(&seen, &called)).ServeHTTP(rec, req)

			if called {
				t.Errorf("next handler ran for key %q", tt.key)
			}
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
			}
		})
	}
}

// TestQueryAuthEmptyMasterKeyIsNotAMatch pins the env.IngestKey != "" guard: an
// unset master key must not make an empty X-Api-Key header authenticate.
func TestQueryAuthEmptyMasterKeyIsNotAMatch(t *testing.T) {
	withEnvIngestKey(t, "")

	var seen *structs.Actor
	var called bool

	req := httptest.NewRequest(http.MethodGet, "/v1/issues", nil)
	req.Header.Set("X-Api-Key", "")
	rec := httptest.NewRecorder()

	QueryAuthMiddleware(captureActor(&seen, &called)).ServeHTTP(rec, req)

	if called {
		t.Error("next handler ran with an unset master key and an empty header")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestGetActorRoundTrip(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/issues", nil)

	if actor, ok := GetActor(req.Context()); ok || actor != nil {
		t.Errorf("GetActor on a bare context returned (%+v, %v), want (nil, false)", actor, ok)
	}

	want := structs.APIKeyActor("k-1", "monitor-mcp")
	ctx := WithActor(req.Context(), want)

	got, ok := GetActor(ctx)
	if !ok {
		t.Fatal("GetActor did not find the actor that WithActor stored")
	}
	if got.Label != want.Label || got.Kind != want.Kind {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// TestGetActorNilStored asserts a nil actor stored under the key reads back as
// absent rather than as a non-nil-but-empty actor.
func TestGetActorNilStored(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/issues", nil)
	ctx := WithActor(req.Context(), nil)

	if actor, ok := GetActor(ctx); ok || actor != nil {
		t.Errorf("GetActor returned (%+v, %v) for a stored nil, want (nil, false)", actor, ok)
	}
}
