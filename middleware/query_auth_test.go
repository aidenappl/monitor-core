package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/scope"
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

// captureQueryProject returns a handler that records the project the middleware
// injected. The read path's tenancy boundary is decided here and enforced later
// in the SQL builders, so this is the assertion that the decision was made at
// all — a branch that authenticates without injecting a project produces a
// handler that cannot build a query, which is loud, but only at runtime.
func captureQueryProject(seen *string, called *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*called = true
		if project, ok := scope.GetProject(r.Context()); ok {
			*seen = project
		}
		w.WriteHeader(http.StatusOK)
	})
}

// TestQueryAuthScopesMasterKeyToTheDefaultProject covers the credential with no
// api_keys row and therefore no binding of its own.
//
// It reads the DEFAULT project because that is where its own writes land —
// ingest stamps the same slug for the same credential — so any other choice
// would give the fleet's master key a view that excludes the events it wrote.
func TestQueryAuthScopesMasterKeyToTheDefaultProject(t *testing.T) {
	withEnvIngestKey(t, "master-key-value")
	withDefaultProject(t, "default")

	var seen string
	var called bool

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req.Header.Set("X-Api-Key", "master-key-value")
	rec := httptest.NewRecorder()

	QueryAuthMiddleware(captureQueryProject(&seen, &called)).ServeHTTP(rec, req)

	if !called {
		t.Fatalf("next handler was not called; status = %d", rec.Code)
	}
	if seen != "default" {
		t.Errorf("injected project = %q, want %q", seen, "default")
	}
}

// TestQueryAuthRejectionInjectsNoProject pins that a request which fails
// authentication never reaches a handler carrying a tenant. A rejected request
// that still had a project attached would be one refactor away from being a
// request that could still read under it.
func TestQueryAuthRejectionInjectsNoProject(t *testing.T) {
	withEnvIngestKey(t, "master-key-value")
	withDefaultProject(t, "default")

	var seen string
	var called bool

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req.Header.Set("X-Api-Key", "not-the-master-key")
	rec := httptest.NewRecorder()

	QueryAuthMiddleware(captureQueryProject(&seen, &called)).ServeHTTP(rec, req)

	if called {
		t.Fatal("next handler ran for an unauthenticated request")
	}
	if seen != "" {
		t.Errorf("injected project = %q, want none", seen)
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestQueryAuthProjectFollowsAConfiguredDefault pins that the injected project
// is read from env.DefaultProjectSlug rather than hardcoded. A deployment that
// set MON_DEFAULT_PROJECT to something else would otherwise have every session
// and every master-key read scoped to a project that does not exist, and see
// nothing at all.
func TestQueryAuthProjectFollowsAConfiguredDefault(t *testing.T) {
	withEnvIngestKey(t, "master-key-value")
	withDefaultProject(t, "trailblaze")

	var seen string
	var called bool

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req.Header.Set("X-Api-Key", "master-key-value")
	rec := httptest.NewRecorder()

	QueryAuthMiddleware(captureQueryProject(&seen, &called)).ServeHTTP(rec, req)

	if !called {
		t.Fatalf("next handler was not called; status = %d", rec.Code)
	}
	if seen != "trailblaze" {
		t.Errorf("injected project = %q, want %q", seen, "trailblaze")
	}
}

// withProjectSelectable pins the registry answer for one test. The real
// selector reads registry's package-private cache, which needs a live MariaDB to
// populate — this is the seam that lets the middleware's own decision be tested
// without one.
func withProjectSelectable(t *testing.T, fn func(string) bool) {
	t.Helper()
	previous := projectSelectable
	projectSelectable = fn
	t.Cleanup(func() { projectSelectable = previous })
}

// withZoneSlug pins env.ZoneSlug, which the refusal message names.
func withZoneSlug(t *testing.T, slug string) {
	t.Helper()
	previous := env.ZoneSlug
	env.ZoneSlug = slug
	t.Cleanup(func() { env.ZoneSlug = previous })
}

// The three tests below drive withSessionProject directly rather than the whole
// QueryAuthMiddleware. The session branch is reached only through
// SessionMiddleware, which validates a JWT and loads the user through db.SQL —
// there is no database in a unit test, and the decision under test is entirely
// this function's. Its callers are covered above.

// TestSessionProjectHonoursAnExplicitSelection is the gap this closed: before
// the selector existed this function stamped env.DefaultProjectSlug
// unconditionally, so a logged-in user could only ever read the default project
// and any switcher built on top of it would have been decorative.
func TestSessionProjectHonoursAnExplicitSelection(t *testing.T) {
	withDefaultProject(t, "default")
	withProjectSelectable(t, func(slug string) bool { return slug == "atlas" })

	var seen string
	var called bool

	req := httptest.NewRequest(http.MethodGet, "/v1/events?project=atlas", nil)
	rec := httptest.NewRecorder()

	withSessionProject(captureQueryProject(&seen, &called)).ServeHTTP(rec, req)

	if !called {
		t.Fatalf("next handler was not called; status = %d", rec.Code)
	}
	if seen != "atlas" {
		t.Errorf("injected project = %q, want %q", seen, "atlas")
	}
}

// TestSessionProjectRejectsAnUnknownProject pins the refusal, and pins that it
// is a refusal rather than a fallback.
//
// A silent fallback is the failure this exists to prevent: it would answer with
// the default project's events under the label the user asked for, which is a
// chart that is wrong while looking right. The assertions are therefore three —
// the handler did not run, no project reached the context, and the message names
// the slug so the switcher can say which one died.
func TestSessionProjectRejectsAnUnknownProject(t *testing.T) {
	withDefaultProject(t, "default")
	withZoneSlug(t, "trailblaze")
	withProjectSelectable(t, func(slug string) bool { return slug == "atlas" })

	tests := []struct {
		name string
		slug string
	}{
		{name: "never existed", slug: "payments"},
		{name: "retired", slug: "billing"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seen string
			var called bool

			req := httptest.NewRequest(http.MethodGet, "/v1/events?project="+tt.slug, nil)
			rec := httptest.NewRecorder()

			withSessionProject(captureQueryProject(&seen, &called)).ServeHTTP(rec, req)

			if called {
				t.Error("next handler ran for an unknown project")
			}
			if seen != "" {
				t.Errorf("injected project = %q, want none", seen)
			}
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
			}
			if !strings.Contains(rec.Body.String(), tt.slug) {
				t.Errorf("body %q does not name the refused project %q", rec.Body.String(), tt.slug)
			}
		})
	}
}

// TestSessionProjectFallsBackToTheDefault covers the no-selection case — every
// request monitor-web makes today, and every request monitor-mcp will ever make.
// A blank value counts as absent: that is what an unset variable interpolated
// into a URL produces, and refusing it would name the empty string in an error.
func TestSessionProjectFallsBackToTheDefault(t *testing.T) {
	withDefaultProject(t, "default")

	// Nothing is selectable — the fallback must not consult the registry at all.
	// If it did, a cold cache would break every session that named no project.
	withProjectSelectable(t, func(string) bool {
		t.Error("the registry was consulted for a request that named no project")
		return false
	})

	tests := []struct {
		name string
		url  string
	}{
		{name: "absent", url: "/v1/events"},
		{name: "empty", url: "/v1/events?project="},
		{name: "whitespace", url: "/v1/events?project=%20%20"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seen string
			var called bool

			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			rec := httptest.NewRecorder()

			withSessionProject(captureQueryProject(&seen, &called)).ServeHTTP(rec, req)

			if !called {
				t.Fatalf("next handler was not called; status = %d", rec.Code)
			}
			if seen != "default" {
				t.Errorf("injected project = %q, want %q", seen, "default")
			}
		})
	}
}

// TestSessionProjectRefusesMultipleSelections pins single-select. A repeatable
// project param is the shape this scheme grows into, and until it does, reading
// two and honouring one would be the same silent wrong answer the fallback is
// refused for.
func TestSessionProjectRefusesMultipleSelections(t *testing.T) {
	withDefaultProject(t, "default")
	withProjectSelectable(t, func(string) bool { return true })

	var seen string
	var called bool

	req := httptest.NewRequest(http.MethodGet, "/v1/events?project=atlas&project=payments", nil)
	rec := httptest.NewRecorder()

	withSessionProject(captureQueryProject(&seen, &called)).ServeHTTP(rec, req)

	if called {
		t.Error("next handler ran for a request naming two projects")
	}
	if seen != "" {
		t.Errorf("injected project = %q, want none", seen)
	}
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

// TestQueryAuthMasterKeyIgnoresTheProjectSelector pins that the selector did not
// leak into the credential-derived branches. Those are real tenancy boundaries —
// a request-supplied slug must never be able to move one — so the master key
// still reads the default project no matter what the query string asks for.
func TestQueryAuthMasterKeyIgnoresTheProjectSelector(t *testing.T) {
	withEnvIngestKey(t, "master-key-value")
	withDefaultProject(t, "default")
	withProjectSelectable(t, func(string) bool { return true })

	var seen string
	var called bool

	req := httptest.NewRequest(http.MethodGet, "/v1/events?project=payments", nil)
	req.Header.Set("X-Api-Key", "master-key-value")
	rec := httptest.NewRecorder()

	QueryAuthMiddleware(captureQueryProject(&seen, &called)).ServeHTTP(rec, req)

	if !called {
		t.Fatalf("next handler was not called; status = %d", rec.Code)
	}
	if seen != "default" {
		t.Errorf("injected project = %q, want %q — the selector must not move a credential-derived project", seen, "default")
	}
}
