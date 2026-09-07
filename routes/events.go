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
	"time"

	"github.com/aidenappl/monitor-core/issues"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/services"
	"github.com/aidenappl/monitor-core/structs"
)

// MaxRequestBodySize limits request body to 10MB
const MaxRequestBodySize = 10 * 1024 * 1024

// Queue is the global event queue (set from main.go)
var Queue *services.Queue

// Batcher is the global event batcher (set from main.go). Only /health reads
// it, and only for last_flush_at, so a nil Batcher reports null rather than
// panicking a liveness probe.
var Batcher *services.Batcher

// HealthHandler is the LIVENESS probe. It reports queue stats plus the state of
// the two stores, and it ALWAYS returns 200 with status "ok".
//
// That is deliberate, not an oversight. The container HEALTHCHECK points here:
// failing it during a ClickHouse outage would have Docker kill and restart a
// process that is running perfectly and cannot fix ClickHouse by rebooting —
// turning a partial outage into a restart loop that also loses the in-memory
// queue. Readiness, which DOES fail on a dead dependency, is `GET /ready`.
//
// The dependency booleans and last_flush_at are reported here anyway because
// this is the endpoint operators and monitor-web already poll; they are
// diagnostics, not a verdict on the status code. The original four keys
// (status, enqueued, dropped, pending) are unchanged — monitor-web's transport
// and the container healthcheck both read that exact shape — and `dropped` now
// finally counts batches the writer gave up on, not just queue overflow.
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	enqueued, dropped, pending := Queue.Stats()
	clickhouseOK, mariadbOK := cachedPingDependencies(r.Context())

	// interface{} so "never flushed" serialises as null rather than the zero
	// time, which would read as a flush in year 1.
	var lastFlushAt interface{}
	if Batcher != nil {
		if t := Batcher.LastFlushAt(); !t.IsZero() {
			lastFlushAt = t.UTC().Format(time.RFC3339)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":        "ok",
		"enqueued":      enqueued,
		"dropped":       dropped,
		"pending":       pending,
		"clickhouse_ok": clickhouseOK,
		"mariadb_ok":    mariadbOK,
		"last_flush_at": lastFlushAt,
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

	// The tenant is resolved ONCE per request, not once per event: every line in
	// an NDJSON body arrived under a single credential and therefore belongs to
	// a single project.
	//
	// No fallback is applied when the lookup misses, deliberately.
	// IngestAuthMiddleware is the only route to this handler and it resolves a
	// project for every credential it accepts — the env master key included,
	// which is precisely why that branch stamps env.DefaultProjectSlug rather
	// than nothing. A second default here would be a second answer to "whose
	// data is this", and the two would drift the first time one of them changed.
	project, _ := middleware.GetProject(r.Context())

	// Whole body parsed cleanly — now enqueue. Count only events the queue
	// actually accepted; dropped events are reflected in /health's `dropped`.
	accepted := 0
	for _, event := range events {
		// Stamp the project and the issue id BEFORE enqueueing, so both land on
		// the stored row. Both are always assigned, never merged: a project or
		// an issue_id supplied by a client is overwritten (issue_id with "" for
		// non-error levels), so no caller can file its events under another
		// project or another issue.
		//
		// THESE TWO LINES ARE ORDERED, not merely adjacent. The issue id is a
		// UUIDv5 over a fingerprint that now includes the project, so the
		// project must already be on the event when IssueIDForEvent reads it.
		// Swapping them still compiles and still produces a well-formed id — the
		// wrong one, pointing at whichever project the client happened to claim,
		// or at the default when it claimed none.
		event.Project = project
		event.IssueID = issues.IssueIDForEvent(event)

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
