package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/services"
)

// The live tail is the read where an unscoped subscription is least likely to be
// noticed. A stored query leaves a statement someone can read back; a stream
// leaves nothing but a held connection quietly delivering other projects' errors
// as they happen, for as long as the client stays connected.

// TestStreamFiltersAlwaysCarryTheProject pins that a subscription is scoped even
// when the client sends no query parameters at all — the plain "tail everything"
// case, which is both the common one and the one that would otherwise subscribe
// to the whole zone.
func TestStreamFiltersAlwaysCarryTheProject(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/events/stream", nil)
	req = req.WithContext(scope.WithProject(req.Context(), "atlas"))

	filters, ok := subscriptionFilters(req)
	if !ok {
		t.Fatal("subscriptionFilters refused a scoped request")
	}
	if filters["project"] != "atlas" {
		t.Errorf("project filter = %q, want %q; an unfiltered subscription tails every project", filters["project"], "atlas")
	}
}

// TestStreamProjectIsServerDerivedNotClientSupplied is the test that would catch
// "project" being added to clientStreamFilters.
//
// Every other filter on this endpoint is the client's to choose. This one is
// not: a client that could name its own project on the stream could subscribe to
// anyone's live errors with a single query string.
func TestStreamProjectIsServerDerivedNotClientSupplied(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/events/stream?project=johnnies&service=monitor-core", nil)
	req = req.WithContext(scope.WithProject(req.Context(), "atlas"))

	filters, ok := subscriptionFilters(req)
	if !ok {
		t.Fatal("subscriptionFilters refused a scoped request")
	}
	if filters["project"] != "atlas" {
		t.Errorf("project filter = %q, want %q — the client's query parameter overrode the credential", filters["project"], "atlas")
	}
	if filters["service"] != "monitor-core" {
		t.Errorf("service filter = %q, want the client's own filters to still apply", filters["service"])
	}

	// The allowlist and the overwrite are two independent guards; assert the
	// first directly so removing it fails here even while the second still masks
	// the effect.
	for _, key := range clientStreamFilters {
		if key == "project" {
			t.Error("\"project\" is in clientStreamFilters; the tenancy filter must never be client-supplied")
		}
	}
}

// TestStreamRefusesAnUnscopedRequest pins the fail-closed branch end to end.
// Subscribing with the project filter merely absent would match every event in
// the zone — which is the firehose the hub's default arm exists to prevent, and
// which this handler must never ask for.
func TestStreamRefusesAnUnscopedRequest(t *testing.T) {
	hub := services.NewHub(10)
	previous := EventHub
	EventHub = hub
	t.Cleanup(func() { EventHub = previous })

	req := httptest.NewRequest(http.MethodGet, "/v1/events/stream", nil)
	rec := httptest.NewRecorder()

	StreamEventsHandler(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d for a request carrying no project", rec.Code, http.StatusInternalServerError)
	}
	if hub.SubscriberCount() != 0 {
		t.Errorf("subscriber count = %d, want 0; an unscoped request must not reach the hub at all", hub.SubscriberCount())
	}
}
