package issues

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/google/uuid"
)

// Issue is the tracked error issue, now owned by MariaDB (monitor.issues) rather
// than ClickHouse. Aliased rather than redeclared so existing call sites keep
// compiling against the canonical struct.
type Issue = structs.Issue

// Fingerprint normalization regexes
var (
	uuidRegex   = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	hexRegex    = regexp.MustCompile(`\b0x[0-9a-fA-F]+\b`)
	numberRegex = regexp.MustCompile(`\b\d+\b`)
	urlRegex    = regexp.MustCompile(`https?://[^\s]+`)

	// statusCodeSuffix matches text ending in an HTTP status-code introducer, e.g.
	// "...returned status " or "...with code ". Used to look BEHIND a numeric match
	// (RE2 has no lookbehind) so status codes survive normalization — see
	// normalizeMessage.
	statusCodeSuffix = regexp.MustCompile(`(?i)\b(?:status|status_code|statuscode|code|http)[ _=:]*$`)
)

// issueNamespace seeds the deterministic issue UUIDs derived from fingerprints.
// Fixed forever: changing it re-keys every issue and orphans all history.
//
// It did NOT change when project entered the fingerprint, and that was not an
// oversight. The two are independent axes, and only one of them needed to move.
// The fingerprint string is what identifies a failure; the namespace is the
// constant that guarantees a given fingerprint always resolves to the same id.
// Re-keying through the fingerprint means new events land on new ids while every
// stored legacy fingerprint still resolves to the legacy row it always did —
// which is exactly what makes the pre-project issues findable and
// hand-resolvable. Moving the namespace as well would break that second property
// too, for no additional benefit: it would make even the old rows unreachable by
// derivation, so a stored fingerprint could no longer be checked against its own
// id. One re-key, one axis.
var issueNamespace = uuid.MustParse("6f1d8f6a-2a3e-5c47-9a4b-6d1c0f2e7b31")

// issueIDFor derives an issue's primary key from its fingerprint.
//
// This is what actually makes issue identity correct. The issues table is a
// ReplacingMergeTree(updated_at) ORDER BY (id), so ClickHouse collapses rows that
// share an id and nothing else — it cannot enforce uniqueness on fingerprint, and
// FINAL does not help. Minting uuid.New() per insert meant two workers that both
// saw "no existing issue" for one fingerprint created two permanently distinct
// issues; production accumulated four rows for a single fingerprint that way.
//
// Deriving the id from the fingerprint makes the racers write the SAME id, so the
// engine merges them on its own. That holds across processes and replicas too,
// which a process-local mutex could never guarantee.
func issueIDFor(fingerprint string) string {
	return uuid.NewSHA1(issueNamespace, []byte(fingerprint)).String()
}

// The 64-way fingerprintLocks mutex shard that used to live here is gone. It
// existed only to serialize a read-then-write against ClickHouse, and was always
// a partial fix — process-local, so it could never protect against a second
// replica. processError now folds an occurrence in a single atomic
// INSERT ... ON DUPLICATE KEY UPDATE, which is correct across processes and
// replicas alike. Do not reintroduce a lock here; if one seems necessary, the
// upsert has probably been split back into a read and a write.

// Worker-pool configuration for error tracking. Error/fatal events are enqueued
// onto trackQueue (non-blocking) and drained by a small fixed pool of workers, so
// an error storm can't spawn unbounded goroutines and shutdown is respected.
const (
	trackQueueSize = 1000
	trackWorkers   = 4
)

// trackQueue buffers error events awaiting issue tracking. Initialized in Init.
var trackQueue chan trackJob

// trackJob carries a single error event and the context under which it was
// captured for the worker pool to process.
type trackJob struct {
	event *structs.Event
}

// Init starts the error-tracking worker pool. The passed ctx is the application
// shutdown context — workers exit when it is cancelled.
//
// It no longer creates a schema. The issues table lives in MariaDB now and is
// created by db.RunMigrations (db/migrations/111_create_issues.sql), which tracks
// what it has applied — unlike the ad-hoc CREATE TABLE that used to run here on
// every boot. The old ClickHouse `issues` table is deliberately left in place,
// unread, so the backfill can be re-run if needed; drop it once a release has
// passed.
func Init(ctx context.Context) error {
	trackQueue = make(chan trackJob, trackQueueSize)
	for i := 0; i < trackWorkers; i++ {
		go trackWorker(ctx)
	}
	return nil
}

// TrackError enqueues an error event for issue tracking. It is non-blocking: if
// the worker queue is full (or not yet initialized), the event is dropped and a
// warning logged rather than blocking the ingestion path or spawning a goroutine.
//
// The event must already carry its server-stamped Project: the issue it folds
// into is derived from it, and the queue hands the caller's pointer straight to
// processError, so anything assigned after this call is a race the fingerprint
// may or may not see.
func TrackError(event *structs.Event) {
	if trackQueue == nil {
		return
	}
	select {
	case trackQueue <- trackJob{event: event}:
	default:
		log.Printf("issues: track queue full, dropping error event %s/%s", event.Service, event.Name)
	}
}

// trackWorker drains the track queue, processing one error event at a time until
// the application context is cancelled.
func trackWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-trackQueue:
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("issues: panic tracking error: %v", r)
					}
				}()
				processError(ctx, job.event)
			}()
		}
	}
}

// regressionFreshness bounds how recently an issue must have regressed for this
// event to be treated as the cause. It only avoids a redundant no-op write on an
// issue that regressed long ago — correctness comes from the dedupe key, not from
// this window.
const regressionFreshness = time.Minute

// processError folds one error event into its issue.
//
// The whole create-or-update is a single atomic statement (see
// query.UpsertIssueOccurrence). There is no read-then-write and no lock: two
// workers racing on one fingerprint both increment, and neither loses. That is
// the fix for the occurrence_count drift this function used to carry.
func processError(ctx context.Context, event *structs.Event) {
	path := extractPath(event)
	message := extractMessage(event, path)

	// Resolve the project ONCE and use that same value for both the hash and the
	// stored column, so monitor.issues.project can never disagree with the
	// project baked into the row's own fingerprint. Two derivations of the same
	// thing is how uq_issues_project_fingerprint ends up describing a row that
	// does not exist.
	project := fingerprintProject(event.Project)
	fingerprint := generateFingerprint(project, event.Service, event.Name, message, path)

	// DATETIME(3) stores milliseconds, so truncate before writing. Otherwise the
	// value read back never equals the one sent, and the regression dedupe key
	// below would differ between the racers it exists to collapse.
	seenAt := event.Timestamp.UTC().Truncate(time.Millisecond)
	if seenAt.IsZero() {
		seenAt = time.Now().UTC().Truncate(time.Millisecond)
	}

	issue, err := query.UpsertIssueOccurrence(db.SQL, query.UpsertIssueOccurrenceRequest{
		ID:          issueIDFor(fingerprint),
		Fingerprint: fingerprint,
		Project:     project,
		Service:     event.Service,
		Name:        event.Name,
		Message:     message,
		Path:        path,
		SeenAt:      seenAt,
	})
	if err != nil {
		log.Printf("issues: failed to record occurrence for fingerprint %s: %v", fingerprint, err)
		return
	}
	if issue == nil {
		return
	}

	recordRegression(issue)
}

// recordRegression appends a timeline entry when an issue has just come back
// from resolved.
//
// The entry is keyed on the stored regressed_at, so two workers that raced on the
// same recurrence compute the SAME dedupe key and collapse to one row — and
// re-emitting an older regression is a harmless no-op rather than a duplicate.
// That is why this needs no lock and no exactly-once guarantee from the caller.
//
// A failure here is logged, not propagated: losing a timeline entry must never
// cost the occurrence count that was already durably recorded.
func recordRegression(issue *structs.Issue) {
	if issue.RegressedAt == nil {
		return
	}
	if time.Since(*issue.RegressedAt) > regressionFreshness {
		return
	}

	dedupeKey := "regressed:" + issue.RegressedAt.UTC().Format(time.RFC3339Nano)
	body := fmt.Sprintf("Recurred after being resolved (regression #%d).", issue.RegressionCount)

	if _, err := query.AppendTimelineEntry(db.SQL, query.AppendTimelineEntryRequest{
		IssueID: issue.ID,
		Type:    structs.TimelineRegressed,
		Actor:   structs.SystemActor(regressionActorLabel),
		Body:    &body,
		Metadata: map[string]any{
			"regression_count": issue.RegressionCount,
			"previous_status":  string(structs.IssueStatusResolved),
			"regressed_at":     issue.RegressedAt.UTC(),
		},
		DedupeKey: &dedupeKey,
	}); err != nil {
		log.Printf("issues: failed to record regression for issue %s: %v", issue.ID, err)
	}
}

// regressionActorLabel attributes automated regressions to ingestion itself,
// since no caller is responsible for them.
const regressionActorLabel = "ingest"

// The three wrappers below take a context and resolve the tenant OUT of it with
// scope.GetProject, rather than taking a project argument the way the query
// functions they call do.
//
// That difference is deliberate and worth one note covering all three. Down in
// query/, an explicit argument is the right shape: those functions are the last
// thing before the SQL, they are called from several layers, and making the
// project impossible to omit there is what the compiler is for. Up here the
// context is already the parameter — every caller is a handler holding a
// *http.Request, and the project is on it because QueryAuthMiddleware put it
// there. Asking them to unpack it and pass it back down would be a second place
// to get it from, and a second place to get it wrong.
//
// All three refuse rather than widen when the context carries no project. Two of
// them have no callers today, and that is exactly why they are scoped: an
// unscoped exported reader sitting in this package is a leak waiting for its
// first caller, and the first caller will not read this comment before writing
// the line.

// List returns a page of issues plus the total matching the filters, within the
// project on ctx.
//
// Filtering, sorting, paginating and counting now happen in one relational query
// each, rather than against a ClickHouse ReplacingMergeTree with FINAL — where
// count(DISTINCT id) had to work around rows the engine had not yet merged.
func List(ctx context.Context, status, service string, limit, offset int) ([]Issue, int, error) {
	project, ok := scope.GetProject(ctx)
	if !ok {
		return nil, 0, scope.ErrNoProject
	}

	req := query.ListIssuesRequest{
		Project:    project,
		Sort:       query.IssueSortLastSeen,
		Descending: true,
		Limit:      limit,
		Offset:     offset,
	}
	if status != "" {
		s := structs.IssueStatus(status)
		if !s.IsValid() {
			return nil, 0, fmt.Errorf("invalid status: %s", status)
		}
		req.Status = &s
	}
	if service != "" {
		req.Service = &service
	}

	total, err := query.CountIssues(db.SQL, req)
	if err != nil {
		return nil, 0, err
	}

	list, err := query.ListIssues(db.SQL, req)
	if err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

// Get returns an issue by id, within the project on ctx.
//
// An issue belonging to another project reports "issue not found" — the same
// error as an id that does not exist anywhere. Its caller,
// routes.HandleGetIssueEvents, turns that into a 404, which is what keeps a
// foreign id from being distinguishable from a bogus one.
func Get(ctx context.Context, id string) (*Issue, error) {
	project, ok := scope.GetProject(ctx)
	if !ok {
		return nil, scope.ErrNoProject
	}

	issue, err := query.GetIssue(db.SQL, project, id)
	if err != nil {
		return nil, err
	}
	if issue == nil {
		return nil, fmt.Errorf("issue not found")
	}
	return issue, nil
}

// UpdateStatus sets an issue's status.
//
// Deprecated in spirit: it records no actor, so the change lands without a
// timeline entry naming who made it. The handler layer should call
// query.UpdateIssue with the actor from middleware.GetActor instead. Kept while
// the routes are cut over.
func UpdateStatus(ctx context.Context, id, status string) error {
	project, ok := scope.GetProject(ctx)
	if !ok {
		return scope.ErrNoProject
	}

	s := structs.IssueStatus(status)
	if !s.IsValid() {
		return fmt.Errorf("invalid status: %s (must be unresolved, in_progress, resolved, or ignored)", status)
	}

	updated, err := query.UpdateIssue(db.SQL, project, id, query.UpdateIssueRequest{Status: &s})
	if err != nil {
		return err
	}
	if updated == nil {
		return fmt.Errorf("issue not found")
	}
	return nil
}

// GetFingerprint returns the fingerprint for an issue (for event lookup).
func GetFingerprint(ctx context.Context, id string) (string, error) {
	issue, err := Get(ctx, id)
	if err != nil {
		return "", err
	}
	return issue.Fingerprint, nil
}

func extractPath(event *structs.Event) string {
	if event.Data == nil {
		return ""
	}
	if p, ok := event.Data["path"]; ok {
		if s, ok := p.(string); ok {
			return s
		}
	}
	if p, ok := event.Data["uri"]; ok {
		if s, ok := p.(string); ok {
			return s
		}
	}
	return ""
}

func extractMessage(event *structs.Event, path string) string {
	if event.Data == nil {
		return event.Name
	}

	// Get the error string
	var errStr string
	if msg, ok := event.Data["error"]; ok {
		if s, ok := msg.(string); ok {
			errStr = s
		}
	}
	if errStr == "" {
		if msg, ok := event.Data["error_message"]; ok {
			if s, ok := msg.(string); ok {
				errStr = s
			}
		}
	}
	if errStr == "" {
		if msg, ok := event.Data["message"]; ok {
			if s, ok := msg.(string); ok {
				errStr = s
			}
		}
	}

	// Build descriptive message when path and error are available
	if path != "" && errStr != "" {
		method := ""
		if m, ok := event.Data["method"]; ok {
			if s, ok := m.(string); ok {
				method = s + " "
			}
		}
		return method + path + ": " + errStr
	}

	if errStr != "" {
		return errStr
	}
	return event.Name
}

// FingerprintForEvent computes the fingerprint the given event groups under,
// using exactly the same derivation as issue creation. Callers that need to know
// whether an event belongs to an issue must compare THIS against the issue's
// fingerprint — matching on service+name alone lumps together every distinct
// failure that happens to share an event name.
//
// It reads event.Project, which is server-stamped: on the ingest path by
// routes.IngestEventsHandler, and on a read path by whatever scanned the row out
// of monitor.events. An event whose Project is not populated does NOT fall
// through to some project-agnostic fingerprint — it is fingerprinted as the
// default project (see fingerprintProject), because that is what an empty
// project means everywhere else in this codebase.
func FingerprintForEvent(event *structs.Event) string {
	path := extractPath(event)
	message := extractMessage(event, path)
	return generateFingerprint(event.Project, event.Service, event.Name, message, path)
}

// IssueIDForEvent returns the id of the issue an event belongs to, or "" if the
// event is not an error/fatal and therefore groups into no issue.
//
// Called synchronously on the ingest path so the id can be stored on the event
// row. That is deliberate and cheap — fingerprinting is a pure sha256 over a few
// regex substitutions, with no I/O. The worker pool exists for the database
// round-trip in processError, not for this.
//
// ORDERING: event.Project must already be stamped when this is called, because
// the id is now derived from it. Calling this first and assigning the project
// afterwards compiles, returns a plausible UUID, and files the event under a
// DIFFERENT project's issue than the one the row itself claims — a mismatch with
// no error and no log line, visible only as an issue whose event list is empty.
// routes.IngestEventsHandler assigns in the right order and says why.
func IssueIDForEvent(event *structs.Event) string {
	if event == nil || !isErrorLevel(event.Level) {
		return ""
	}
	return issueIDFor(FingerprintForEvent(event))
}

// isErrorLevel reports whether a level groups into issues. Kept in one place so
// the ingest stamp and the tracking hook can never disagree about which events
// belong to an issue.
func isErrorLevel(level string) bool {
	return level == "error" || level == "fatal"
}

// generateFingerprint derives the grouping key for one error event.
//
// PROJECT IS IN THE STRING, not merely in a database key, and that distinction
// is the entire point rather than a stylistic choice.
//
// The issue's primary key is uuid.NewSHA1(issueNamespace, fingerprint) — DERIVED
// from this string, not independent of it. So two projects whose service, name,
// path and normalized message matched would produce an identical fingerprint,
// therefore an identical id, therefore a PRIMARY KEY collision. Adding
// UNIQUE KEY (project, fingerprint) to monitor.issues while leaving project out
// of here would have protected nothing at all: MariaDB's
// INSERT ... ON DUPLICATE KEY UPDATE fires on the FIRST unique index the row
// violates, and that is the primary key, so project B's occurrence would be
// folded into project A's row — its counter incremented, its message
// overwritten, its resolved status reopened — before the composite key was ever
// consulted. Two tenants, one issue, one shared triage state, and no error
// anywhere to say so. The composite key in migration 118 documents the intent;
// this line is the mechanism.
//
// ENV IS DELIBERATELY NOT A COMPONENT. Production and staging failures of the
// same shape already merge into one issue today, and that is what Sentry does on
// purpose too: an error is one bug regardless of which deployment it fired in,
// and the per-event `env` column is still there to split the occurrences when
// somebody wants them split. Adding it would also be a SECOND full re-key of
// every issue, and unlike project it has no anchor to re-key onto — project has
// env.DefaultProjectSlug, whereas env is whatever string an SDK happened to
// send, including nothing at all. Recorded here so it reads as a decision rather
// than as an omission for someone to rediscover as a bug.
func generateFingerprint(project, service, name, message, path string) string {
	normalized := normalizeMessage(message)
	raw := fingerprintProject(project) + "|" + service + "|" + name + "|" + path + "|" + normalized
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])
}

// fingerprintProject resolves the project component, mapping the empty string
// onto the default project slug.
//
// Ingest never hands this an empty one: IngestAuthMiddleware resolves a project
// for every credential it accepts, the env master key included, and
// routes.IngestEventsHandler stamps it onto the event before either fingerprint
// call runs. The empty case comes from the READ side. An events row written
// before migrations/006_events_project.sql reads back as "" — adding a
// ClickHouse column is metadata-only, so existing parts are never rewritten —
// and routes/issues.go's legacy fallback scan re-derives a fingerprint from
// exactly those rows to decide whether they belong to an issue.
//
// Mapping "" onto the default is the same rule scope.ProjectPredicate already
// applies to that read: empty means "written before projects existed", not "no
// project". Doing it HERE, inside the single derivation both the write and read
// paths share, is what stops them from disagreeing — a normalization applied in
// one caller is one the other caller silently forgets, and the symptom would be
// an issue whose own events no longer match it.
func fingerprintProject(project string) string {
	if project == "" {
		return env.DefaultProjectSlug
	}
	return project
}

func normalizeMessage(message string) string {
	// Strip UUIDs
	message = uuidRegex.ReplaceAllString(message, "<UUID>")
	// Strip hex strings
	message = hexRegex.ReplaceAllString(message, "<HEX>")
	// Strip URLs
	message = urlRegex.ReplaceAllString(message, "<URL>")
	// Strip numbers — except HTTP status codes, which are a failure CLASS, not an
	// incidental identifier.
	message = replaceNumbersPreservingStatusCodes(message)
	return message
}

// replaceNumbersPreservingStatusCodes collapses digit runs to <N> the way plain
// numberRegex replacement did, but keeps a 3-digit number that is introduced as
// an HTTP status.
//
// Collapsing every number merged genuinely different failures into one issue:
// "workday returned status 502 for lowes (offset 7820)" and "... status 429 ...
// (offset 560)" both normalized to "status <N> ... (offset <N>)", so a Bad
// Gateway and a rate-limit shared a fingerprint, an occurrence count, and a
// displayed message that flip-flopped to whichever arrived last. Stripping the
// offset is right — it is noise. Stripping the status is not.
//
// RE2 has no lookbehind, so matches are located by index and the preceding text
// is inspected directly.
func replaceNumbersPreservingStatusCodes(message string) string {
	matches := numberRegex.FindAllStringIndex(message, -1)
	if matches == nil {
		return message
	}

	var b strings.Builder
	last := 0
	for _, m := range matches {
		start, end := m[0], m[1]
		b.WriteString(message[last:start])

		digits := message[start:end]
		if len(digits) == 3 && statusCodeSuffix.MatchString(message[:start]) {
			b.WriteString(digits)
		} else {
			b.WriteString("<N>")
		}
		last = end
	}
	b.WriteString(message[last:])
	return b.String()
}
