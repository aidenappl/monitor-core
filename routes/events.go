package routes

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/issues"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/services"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/telemetry"
)

// MaxRequestBodySize limits request body to 10MB
const MaxRequestBodySize = 10 * 1024 * 1024

// Queue is the global event queue (set from main.go).
//
// NIL IN A CONTROL-PLANE-ONLY PROCESS (MON_ROLE=app), which runs no ingest
// machinery at all. Nothing that reads it is registered there — the ingest route
// is gated in buildRouter — except HealthHandler, which must answer in every
// role and therefore checks.
var Queue *services.Queue

// Batcher is the global event batcher (set from main.go). Only /health reads
// it, and only for last_flush_at, so a nil Batcher reports null rather than
// panicking a liveness probe.
var Batcher *services.Batcher

// AlertingDisabledReason is non-empty when the alert evaluator was deliberately
// not started — today, only because the Phase 2 configuration cutover has not
// run and the rule table is therefore empty.
//
// Reported on /health and /ready because a disabled evaluator is otherwise
// indistinguishable from a working one that has nothing to fire on: both are a
// quiet process and an empty alert history. Degrading rather than crash-looping
// is only defensible if the degraded state is visible, and this is the endpoint
// an operator already polls.
var AlertingDisabledReason string

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
//
// `role` is reported for the same reason main() logs it at boot, and here
// because a log line scrolls away while this endpoint can be asked. The risk the
// role split carries is not that `both` is wrong — it is that `both` becomes an
// unexamined default nobody remembers choosing, and a value that can be curled
// is the cheapest defence against that.
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	// Queue is nil in an app process. Reporting zeroes there is honest: it runs
	// no queue, so nothing has been enqueued, dropped or is pending. Calling
	// Stats() on the nil pointer would panic and take out the endpoint the
	// container HEALTHCHECK polls, turning a role that is working perfectly into
	// a restart loop.
	var enqueued, dropped int64
	var pending int
	if Queue != nil {
		enqueued, dropped, pending = Queue.Stats()
	}
	clickhouseOK, mariadbOK := cachedPingDependencies(r.Context())

	// interface{} so "never flushed" serialises as null rather than the zero
	// time, which would read as a flush in year 1.
	var lastFlushAt interface{}
	if Batcher != nil {
		if t := Batcher.LastFlushAt(); !t.IsZero() {
			lastFlushAt = t.UTC().Format(time.RFC3339)
		}
	}

	// ⚠️ `status` USED TO BE THE LITERAL "ok", beside a `mariadb_ok` that could
	// say false — so this endpoint reported a healthy service during a datastore
	// outage, which is the exact failure the dependency pings were added to end.
	// It now answers from the same judgement /ready uses.
	//
	// The HTTP status stays 200 either way, deliberately: the container
	// HEALTHCHECK polls this path with `curl -f`, so degrading the status code
	// would turn a MariaDB blip into a restart loop. /ready is the endpoint that
	// gets to be 503, because taking a replica out of rotation is recoverable
	// and killing the process is not.
	failing := failingDependencies(clickhouseOK, mariadbOK)
	health := "ok"
	if len(failing) > 0 {
		health = "degraded"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	body := map[string]interface{}{
		// Additive: the self-telemetry shipper's own counters, so a Monitor that
		// has stopped reporting on itself can be seen from outside the process.
		// `dropped` rising here is loss; `pending` rising with `flushed` still is
		// the destination zone being down.
		"telemetry":     telemetry.Stats(),
		"status":        health,
		"enqueued":      enqueued,
		"dropped":       dropped,
		"pending":       pending,
		"clickhouse_ok": clickhouseOK,
		"mariadb_ok":    mariadbOK,
		"last_flush_at": lastFlushAt,
		"role":          string(env.MonRole),
		// `zone` is THIS PROCESS'S IDENTITY, and it is here so that a caller can
		// tell "a monitor-core answered" apart from "the monitor-core I meant
		// answered". Every zone runs this same binary and every one of them
		// answers 200, so a control plane holding a registry row whose query_url
		// points at the wrong box has no way to notice — the reads simply return
		// another tenant's data under this zone's name, with nothing invalid
		// anywhere. probe.Zone compares this value against the slug the registry
		// expected, and structs.ZoneReachabilityMismatched is what it reports when
		// they disagree; without this key the best any probe can conclude is
		// `unverified`.
		//
		// Reported alongside `role` rather than instead of it because a control
		// plane carries a zone slug too (MON_ZONE_SLUG has a default in every
		// role) and serves no events at all, so the pair is what identifies a
		// process — see probe.classify, which checks the role FIRST for exactly
		// that reason.
		//
		// Unauthenticated, like every other key here. The slug is not a secret: it
		// is in the ingest URL of every producer and in the path of every
		// dashboard link, and withholding it would only mean the one caller that
		// needs it cannot check.
		"zone":        env.ZoneSlug,
		"alerting_ok": AlertingDisabledReason == "",
	}
	// Named only when there is something to name, so the healthy shape is
	// unchanged for anything already parsing it.
	if len(failing) > 0 {
		body["failing"] = failing
	}
	_ = json.NewEncoder(w).Encode(body)
}

// IngestEventsHandler processes incoming NDJSON events
func IngestEventsHandler(w http.ResponseWriter, r *http.Request) {
	// Limit request body size
	r.Body = http.MaxBytesReader(w, r.Body, MaxRequestBodySize)

	bodyReader, err := getBodyReader(r)
	if err != nil {
		reportIngestRejection(r, &ingestRejection{Reason: "gzip_invalid", Err: err, message: err.Error()})
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer bodyReader.Close()

	// Parse + validate the ENTIRE body first. If any line is malformed we return
	// 400 having enqueued nothing, so a client retry cannot double-commit the
	// lines that preceded the bad one.
	events, err := parseEvents(bodyReader)
	if err != nil {
		reportIngestRejection(r, err)
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

	// Overflow is reported ONCE PER REQUEST, not per refused event, and
	// coalesced: a full queue refuses every event of every request until the
	// batcher drains it, and appleby-core's own telemetry is one of the producers
	// being refused. Counts only — the events belong to the tenant that sent
	// them.
	if refused := len(events) - accepted; refused > 0 {
		_, droppedTotal, pending := Queue.Stats()
		telemetry.ErrorCoalesced(r.Context(), "ingest.queue.overflow", "ingest.queue.overflow", nil, map[string]any{
			"project":        project,
			"batch_size":     len(events),
			"refused":        refused,
			"queue_pending":  pending,
			"queue_capacity": Queue.Capacity(),
			"dropped_total":  droppedTotal,
			"reason":         "queue_full",
			"outcome":        fmt.Sprintf("dropped %d of %d events; the producer was told %d were accepted", refused, len(events), accepted),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"accepted": accepted,
	})
}

// ingestRejection is why an ingest body was refused.
//
// Error() is what the PRODUCER is told, and may quote its own body back to it.
// The telemetry event gets only the classification (Reason, Line, Detail): an
// ingest body is another service's data — every Trailblaze service ingests
// through this path — and a JSON or timestamp parse error quotes it. Copying
// that into a self-telemetry event would move one zone's data into another.
type ingestRejection struct {
	Line    int
	Reason  string // invalid_json | validation_failed | body_too_large | line_too_long | body_read_failed | gzip_invalid
	Detail  string // a rule or a JSON error class — never a value
	Service string // the failing line's service, when it parsed that far
	Err     error
	message string
}

func (e *ingestRejection) Error() string { return e.message }
func (e *ingestRejection) Unwrap() error { return e.Err }

// reportIngestRejection records a refused ingest body, coalesced per reason: a
// producer with a bug sends the same bad line on every retry.
func reportIngestRejection(r *http.Request, err error) {
	ingestRejected.Add(1)

	data := map[string]any{
		"reason":      "invalid_body",
		"status_code": http.StatusBadRequest,
		"client_ip":   middleware.GetClientIPFromContext(r.Context()),
		"outcome":     "returned 400; nothing from the body was enqueued",
	}
	var rej *ingestRejection
	if errors.As(err, &rej) {
		data["reason"] = rej.Reason
		if rej.Line > 0 {
			data["line"] = rej.Line
		}
		if rej.Detail != "" {
			data["detail"] = rej.Detail
		}
		if rej.Service != "" {
			data["service"] = truncateString(rej.Service, 100)
		}
		if rej.Err != nil {
			data["error_type"] = telemetry.ErrorType(rej.Err)
		}
	}
	if project, ok := middleware.GetProject(r.Context()); ok {
		data["project"] = project
	}
	telemetry.WarnCoalesced(r.Context(), "ingest.request.rejected:"+data["reason"].(string), "ingest.request.rejected", nil, data)
}

// jsonErrorClass describes a JSON decode failure without quoting the input. A
// SyntaxError is safe (an offset), an UnmarshalTypeError names the field and
// the KIND of value, a time.ParseError would quote the timestamp and is named
// instead.
func jsonErrorClass(err error) string {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	var timeErr *time.ParseError
	switch {
	case errors.As(err, &syntaxErr):
		return fmt.Sprintf("syntax error at offset %d", syntaxErr.Offset)
	case errors.As(err, &typeErr):
		return fmt.Sprintf("field %q expects %s, got a JSON %s", typeErr.Field, typeErr.Type, typeErr.Value)
	case errors.As(err, &timeErr):
		return "timestamp is not RFC 3339"
	}
	return "malformed JSON"
}

func truncateString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
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
			return nil, &ingestRejection{
				Line:    lineNum,
				Reason:  "invalid_json",
				Detail:  jsonErrorClass(err),
				Err:     err,
				message: fmt.Sprintf("line %d: invalid JSON: %v", lineNum, err),
			}
		}

		if err := event.Validate(); err != nil {
			// Validate's messages name the rule and never the value, so they
			// are safe to report as the detail.
			return nil, &ingestRejection{
				Line:    lineNum,
				Reason:  "validation_failed",
				Detail:  err.Error(),
				Service: event.Service,
				Err:     err,
				message: fmt.Sprintf("line %d: %v", lineNum, err),
			}
		}

		events = append(events, &event)
	}

	if err := scanner.Err(); err != nil {
		reason := "body_read_failed"
		var tooLarge *http.MaxBytesError
		switch {
		case errors.As(err, &tooLarge):
			reason = "body_too_large"
		case errors.Is(err, bufio.ErrTooLong):
			reason = "line_too_long"
		}
		return nil, &ingestRejection{
			Line:    lineNum + 1,
			Reason:  reason,
			Err:     err,
			message: fmt.Sprintf("error reading body: %v", err),
		}
	}

	return events, nil
}
