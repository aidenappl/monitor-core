package routes

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/aidenappl/monitor-core/services"
)

// EventHub is the global SSE hub (set from main.go)
var EventHub *services.Hub

// StreamEventsHandler handles GET /v1/events/stream (SSE)
func StreamEventsHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	// Parse filters from query params
	filters := make(map[string]string)
	for _, key := range []string{"service", "env", "level", "name"} {
		if v := r.URL.Query().Get(key); v != "" {
			filters[key] = v
		}
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
