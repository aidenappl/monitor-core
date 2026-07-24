package routes

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-core/issues"
	"github.com/aidenappl/monitor-core/services"
	"github.com/aidenappl/monitor-core/structs"
)

// MaxRequestBodySize limits request body to 10MB
const MaxRequestBodySize = 10 * 1024 * 1024

// Queue is the global event queue (set from main.go)
var Queue *services.Queue

// HealthHandler returns queue stats
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	enqueued, dropped, pending := Queue.Stats()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "ok",
		"enqueued": enqueued,
		"dropped":  dropped,
		"pending":  pending,
	})
}

// IngestEventsHandler processes incoming NDJSON events
func IngestEventsHandler(w http.ResponseWriter, r *http.Request) {
	// Limit request body size
	r.Body = http.MaxBytesReader(w, r.Body, MaxRequestBodySize)

	bodyReader, err := getBodyReader(r)
	if err != nil {
		log.Printf("failed to get body reader: %v", err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer bodyReader.Close()

	// Parse + validate the ENTIRE body first. If any line is malformed we return
	// 400 having enqueued nothing, so a client retry cannot double-commit the
	// lines that preceded the bad one.
	events, err := parseEvents(bodyReader)
	if err != nil {
		log.Printf("failed to parse events: %v", err)
		http.Error(w, fmt.Sprintf("Invalid event: %v", err), http.StatusBadRequest)
		return
	}

	// Whole body parsed cleanly — now enqueue. Count only events the queue
	// actually accepted; dropped events are reflected in /health's `dropped`.
	accepted := 0
	for _, event := range events {
		if Queue.Enqueue(event) {
			accepted++
		}

		// Publish to SSE hub for live streaming
		if EventHub != nil {
			EventHub.Publish(event)
		}

		// Track errors as issues (bounded, non-blocking worker pool)
		if event.Level == "error" || event.Level == "fatal" {
			issues.TrackError(event)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"accepted": accepted,
	})
}

func getBodyReader(r *http.Request) (io.ReadCloser, error) {
	contentEncoding := r.Header.Get("Content-Encoding")
	if strings.Contains(strings.ToLower(contentEncoding), "gzip") {
		gzReader, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		return gzReader, nil
	}
	return r.Body, nil
}

// parseEvents streams the NDJSON body line-by-line into a slice, validating each
// event. It enqueues NOTHING — if any line is invalid JSON or fails Validate(),
// it returns an error and the caller commits none of the events. This keeps the
// ingestion endpoint all-or-nothing so client retries can't partially duplicate.
func parseEvents(reader io.Reader) ([]*structs.Event, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var events []*structs.Event
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()

		if len(line) == 0 {
			continue
		}

		var event structs.Event
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, fmt.Errorf("line %d: invalid JSON: %w", lineNum, err)
		}

		if err := event.Validate(); err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNum, err)
		}

		events = append(events, &event)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading body: %w", err)
	}

	return events, nil
}
