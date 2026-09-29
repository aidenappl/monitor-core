package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/services"
)

// resetDraining gives one test its own draining signal. The real one is a
// process-global closed-once channel — correct in a server, but a test that
// closed it would leave every later stream in this package starting drained.
func resetDraining(t *testing.T) {
	t.Helper()
	drainingMu.Lock()
	previous := drainingCh
	drainingCh = make(chan struct{})
	drainingMu.Unlock()
	t.Cleanup(func() {
		drainingMu.Lock()
		drainingCh = previous
		drainingMu.Unlock()
	})
}

// TestStreamEndsWhenDrainingStarts is the shutdown-duration guard.
//
// An SSE handler ends when its client goes away, and `server.Shutdown` waits for
// active requests — so before this, one open live-tail tab held every deploy for
// the whole 10s timeout, and a stop could exceed Docker's stop grace and be
// SIGKILLed, losing the shutdown events. The handler must return on its own once
// draining begins, with the request context still live.
func TestStreamEndsWhenDrainingStarts(t *testing.T) {
	resetDraining(t)

	hub := services.NewHub(10)
	previous := EventHub
	EventHub = hub
	t.Cleanup(func() { EventHub = previous })

	req := httptest.NewRequest(http.MethodGet, "/v1/events/stream", nil)
	// A live context: this test proves the stream ends WITHOUT the client
	// leaving, which is the case Shutdown could not resolve on its own.
	req = req.WithContext(scope.WithProject(req.Context(), "atlas"))

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		StreamEventsHandler(httptest.NewRecorder(), req)
	}()

	// The handler must be subscribed before draining, or it could be ending for
	// the unscoped-refusal reason instead of this one.
	deadline := time.Now().Add(2 * time.Second)
	for hub.SubscriberCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the stream handler never subscribed")
		}
		time.Sleep(time.Millisecond)
	}

	StartDraining()

	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("the stream handler did not return when draining started; Shutdown would wait out its full timeout")
	}

	if hub.SubscriberCount() != 0 {
		t.Errorf("subscriber count = %d, want 0; the handler returned without unsubscribing", hub.SubscriberCount())
	}
}

// TestStartDrainingIsIdempotent — it is wired to RegisterOnShutdown, and a
// second call (a re-entered shutdown path) must not panic on a closed channel.
func TestStartDrainingIsIdempotent(t *testing.T) {
	resetDraining(t)

	StartDraining()
	StartDraining()

	select {
	case <-Draining():
	default:
		t.Error("Draining() is not closed after StartDraining")
	}
}
