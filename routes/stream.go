package routes

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/services"
	"github.com/aidenappl/monitor-core/telemetry"
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
//
// TELEMETRY. The connection's request event fires when it closes, carrying how
// long it was held. Connect and disconnect are debug. A failed write or a
// subscriber too slow to keep up is a warning, coalesced — the event is about
// this stream, but the write loop runs per event.
func StreamEventsHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		middleware.RecordFailure(w, http.StatusInternalServerError, "streaming not supported", nil, map[string]any{"stream": "events", "reason": "writer_cannot_flush"})
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	filters, ok := subscriptionFilters(r)
	if !ok {
		middleware.RecordFailure(w, http.StatusInternalServerError, "no project resolved for this request", nil, map[string]any{"stream": "events", "reason": "no_project"})
		http.Error(w, "no project resolved for this request", http.StatusInternalServerError)
		return
	}

	sub := EventHub.Subscribe(filters)
	if sub == nil {
		middleware.RecordFailure(w, http.StatusServiceUnavailable, "too many concurrent subscribers", nil, map[string]any{"stream": "events", "reason": "max_subscribers"})
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
	// An error here means the writer cannot take deadlines at all, in which case
	// the stream simply keeps the server's — nothing to report per connection.
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})

	flusher.Flush()

	ctx := r.Context()
	telemetry.Debug(ctx, "stream.subscriber.connected", map[string]any{
		"stream":        "events",
		"subscriber_id": sub.ID,
		"project":       filters["project"],
		"filters":       len(filters) - 1,
	})

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	started := time.Now()
	sent := 0
	for {
		select {
		case <-ctx.Done():
			telemetry.Debug(ctx, "stream.subscriber.disconnected", map[string]any{
				"stream":        "events",
				"subscriber_id": sub.ID,
				"events_sent":   sent,
				"duration_ms":   time.Since(started).Milliseconds(),
			})
			return
		case <-keepalive.C:
			reportLaggingSubscriber(ctx, "events", filters["project"], sub.TakeDropped(), services.SUBSCRIBER_BUFFER)
			_ = rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
			if _, err := fmt.Fprintf(w, ": keepalive\n\n"); err != nil {
				reportStreamWriteFailure(ctx, "events", sent, err)
				return
			}
			flusher.Flush()
		case event, ok := <-sub.Events:
			if !ok {
				return
			}
			data, err := json.Marshal(event)
			if err != nil {
				telemetry.WarnCoalesced(ctx, "stream.event.encode.failed", "stream.event.encode.failed", err, map[string]any{
					"stream":  "events",
					"outcome": "one event was skipped for this subscriber",
				})
				continue
			}
			_ = rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				reportStreamWriteFailure(ctx, "events", sent, err)
				return
			}
			flusher.Flush()
			sent++
		}
	}
}

// reportStreamWriteFailure records a live stream that could not be written to —
// almost always a client that went away without closing cleanly, occasionally a
// proxy cutting an idle connection. The stream ends.
func reportStreamWriteFailure(ctx context.Context, stream string, sent int, err error) {
	telemetry.WarnCoalesced(ctx, "stream.write.failed:"+stream, "stream.write.failed", err, map[string]any{
		"stream":      stream,
		"events_sent": sent,
		"outcome":     "stream closed",
	})
}

// reportLaggingSubscriber records events skipped for a subscriber that read too
// slowly to keep up with its buffer. Checked on the keepalive tick, not per
// event: the skip itself happens on the ingest hot path, which only counts it.
func reportLaggingSubscriber(ctx context.Context, stream, project string, dropped int64, buffer int) {
	if dropped == 0 {
		return
	}
	telemetry.WarnCoalesced(ctx, "stream.subscriber.lagging:"+stream, "stream.subscriber.lagging", nil, map[string]any{
		"stream":  stream,
		"project": project,
		"dropped": dropped,
		"buffer":  buffer,
		"outcome": "events were skipped for this subscriber; the stream stays open",
	})
}
