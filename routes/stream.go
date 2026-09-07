package routes

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/services"
)

// EventHub is the global SSE hub (set from main.go)
var EventHub *services.Hub

// clientStreamFilters are the filter keys a subscriber may choose for itself.
// "project" is deliberately absent and must stay absent — see below.
var clientStreamFilters = []string{"service", "env", "level", "name"}

// subscriptionFilters builds the hub filter map for one stream request, and
// reports false when the request carries no project.
//
// It is split out of the handler so it can be tested, which the handler itself
// cannot be: StreamEventsHandler blocks until the client disconnects, and the
// thing worth asserting on happens before that — the filter map handed to the
// hub, which is the entire tenancy boundary for a live tail.
//
// The project is MANDATORY and SERVER-DERIVED. It is applied AFTER the client's
// filters, so even if "project" were one day added to clientStreamFilters by
// mistake, the credential's value would overwrite the query parameter rather
// than lose to it. Two independent guards for one property, because the failure
// they prevent leaves no trace: a stream is not a stored query, so a subscriber
// receiving another project's live errors produces no statement to read back,
// no audit row, and no error — just the wrong data, silently, for as long as the
// connection is held. Refusing outright is the only safe response to "I do not
// know whose events these should be".
func subscriptionFilters(r *http.Request) (map[string]string, bool) {
	filters := make(map[string]string)
	for _, key := range clientStreamFilters {
		if v := r.URL.Query().Get(key); v != "" {
			filters[key] = v
		}
	}

	project, ok := scope.GetProject(r.Context())
	if !ok {
		return nil, false
	}
	filters["project"] = project

	return filters, true
}

// StreamEventsHandler handles GET /v1/events/stream (SSE)
func StreamEventsHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	filters, ok := subscriptionFilters(r)
	if !ok {
		http.Error(w, "no project resolved for this request", http.StatusInternalServerError)
		return
	}

	sub := EventHub.Subscribe(filters)
	if sub == nil {
		http.Error(w, "too many concurrent subscribers", http.StatusServiceUnavailable)
		return
	}
	defer EventHub.Unsubscribe(sub.ID)

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	// Clear the server's WriteTimeout for this connection so the SSE stream is
	// not severed after WriteTimeout (30s in main.go). Relies on the logging
	// middleware exposing Unwrap() so the controller can reach the raw conn.
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})

	flusher.Flush()

	log.Printf("SSE subscriber connected: %s (filters: %v)", sub.ID, filters)

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			log.Printf("SSE subscriber disconnected: %s", sub.ID)
			return
		case <-keepalive.C:
			rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		case event, ok := <-sub.Events:
			if !ok {
				return
			}
			data, err := json.Marshal(event)
			if err != nil {
				continue
			}
			rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}
