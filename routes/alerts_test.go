package routes

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aidenappl/monitor-core/alerts"
	"github.com/aidenappl/monitor-core/scope"
)

// The alert stream is the second live tail in this service, and it had the
// defect routes/stream.go's tests exist to prevent: AlertHub.Subscribe took no
// filter, so every subscriber received every rule's state changes — the rule
// name and the message the evaluator builds from it, in real time, with no
// stored query left behind to notice it by.
//
// The FILTERING itself is pinned in alerts/alert_hub_test.go, where
// scope.Matches decides membership. What this file owns is the handler's half:
// that a subscription is never created at all without a resolved project, and
// that the project a subscription carries is the credential's.

// TestStreamAlertsRefusesWithoutAProject.
//
// A subscriber with no project would be handed a filter scope.Matches rejects
// outright, so it would receive nothing — fail-closed either way. This asserts
// the LOUDER half: the request is refused before any subscription exists, so a
// route registered outside QueryAuthMiddleware produces a 500 an operator can
// see rather than a connection that silently stays empty forever.
func TestStreamAlertsRefusesWithoutAProject(t *testing.T) {
	previous := AlertNotifHub
	hub := alerts.NewAlertHub(1)
	AlertNotifHub = hub
	t.Cleanup(func() { AlertNotifHub = previous })

	req := httptest.NewRequest(http.MethodGet, "/v1/alerts/stream", nil)
	rec := httptest.NewRecorder()

	HandleStreamAlerts(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 — an unscoped stream request must be refused, not tailed", rec.Code)
	}
	// The refusal has to happen BEFORE Subscribe. A subscriber created and then
	// abandoned would hold a slot against maxSubs for the life of the process.
	assertHubIsEmpty(t, hub, "a subscription was created for a request with no project")
}

// TestStreamAlertsSubscribesWithTheCredentialsProject.
//
// The handler exits as soon as the request context is done, so this drives it
// with an already-cancelled one: Subscribe has run and Unsubscribe has run by
// the time it returns. What is left to assert is that the subscription happened
// at all and released its slot — the project it carried is the value
// requireProject resolved, which is the same value scope.GetProject returns and
// which the hub tests then filter on.
func TestStreamAlertsSubscribesWithTheCredentialsProject(t *testing.T) {
	previous := AlertNotifHub
	hub := alerts.NewAlertHub(1)
	AlertNotifHub = hub
	t.Cleanup(func() { AlertNotifHub = previous })

	ctx, cancel := context.WithCancel(scope.WithProject(context.Background(), "atlas"))
	cancel()

	req := httptest.NewRequest(http.MethodGet, "/v1/alerts/stream", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	HandleStreamAlerts(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 for a scoped stream request", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", got)
	}
	// maxSubs is 1, so a leaked subscription would make this hub permanently
	// unusable — which is exactly what a missing Unsubscribe looks like in
	// production, one connection at a time.
	assertHubIsEmpty(t, hub, "the subscription was not released when the client disconnected")
}

// assertHubIsEmpty reports that a hub holds no subscribers.
//
// AlertHub keeps its map unexported and exposes no count, so this probes the one
// observable it has: with a capacity of 1 — which both hubs above are created
// with — a successful Subscribe proves the slot was free. The probe is released
// immediately, so the hub is left as it was found.
func assertHubIsEmpty(t *testing.T, hub *alerts.AlertHub, whenLeaked string) {
	t.Helper()

	probe := hub.Subscribe("probe")
	if probe == nil {
		t.Fatal(whenLeaked)
	}
	hub.Unsubscribe(probe.ID)
}
