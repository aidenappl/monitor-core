package routes

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/issues"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/gorilla/mux"
)

// issueQueryParams parses the shared filter/sort surface of GET /v1/issues.
//
// Every filter is a pointer so "not supplied" stays distinguishable from a zero
// value — `assignee=0` must not silently mean "unassigned", and `service=` must
// not filter to the empty string.
func issueQueryParams(r *http.Request) (query.ListIssuesRequest, error) {
	q := r.URL.Query()
	req := query.ListIssuesRequest{
		Sort:       query.IssueSort(q.Get("sort")),
		Descending: !strings.EqualFold(q.Get("order"), "asc"),
	}

	if v := q.Get("status"); v != "" {
		status := structs.IssueStatus(v)
		if !status.IsValid() {
			return req, fmt.Errorf("invalid status %q (expected unresolved, in_progress, resolved or ignored)", v)
		}
		req.Status = &status
	}
	if v := q.Get("service"); v != "" {
		req.Service = &v
	}
	if v := q.Get("q"); v != "" {
		req.Search = &v
	}
	if v := q.Get("assignee"); v != "" {
		// "none" is how the UI asks for unassigned issues; an id cannot express
		// that, and omitting the filter means "any".
		if strings.EqualFold(v, "none") || strings.EqualFold(v, "unassigned") {
			req.Unassigned = true
		} else {
			id, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return req, fmt.Errorf("invalid assignee %q (expected a user id or \"none\")", v)
			}
			req.AssigneeUserID = &id
		}
	}
	if v := q.Get("has_pr"); v != "" {
		hasPR, err := strconv.ParseBool(v)
		if err != nil {
			return req, fmt.Errorf("invalid has_pr %q (expected true or false)", v)
		}
		req.HasPR = &hasPR
	}
	if v := q.Get("from"); v != "" {
		t, err := parseTimeParam(v)
		if err != nil {
			return req, fmt.Errorf("invalid from: %w", err)
		}
		req.From = &t
	}
	if v := q.Get("to"); v != "" {
		t, err := parseTimeParam(v)
		if err != nil {
			return req, fmt.Errorf("invalid to: %w", err)
		}
		req.To = &t
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return req, fmt.Errorf("invalid limit %q", v)
		}
		req.Limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return req, fmt.Errorf("invalid offset %q", v)
		}
		req.Offset = n
	}

	if !req.Sort.IsValid() {
		return req, fmt.Errorf("invalid sort %q (expected last_seen, first_seen or occurrences)", q.Get("sort"))
	}
	return req, nil
}

// parseTimeParam accepts RFC3339 or a unix timestamp, matching the rest of the
// query API.
func parseTimeParam(v string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t.UTC(), nil
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		return time.Unix(secs, 0).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("expected an RFC3339 timestamp or unix seconds, got %q", v)
}

// HandleListIssues returns a filtered, sorted page of issues.
//
// Rows are enriched with their links, assignee and service repository in THREE
// bulk queries regardless of page size — rendering 100 issues must not become
// 300 round trips.
func HandleListIssues(w http.ResponseWriter, r *http.Request) {
	req, err := issueQueryParams(r)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	total, err := query.CountIssues(db.SQL, req)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to count issues", err)
		return
	}

	list, err := query.ListIssues(db.SQL, req)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list issues", err)
		return
	}
	if err := enrichIssues(list); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to enrich issues", err)
		return
	}

	// ?history=true attaches the per-row activity strip. Opt-in because it costs
	// an extra ClickHouse read, and callers that only want counts (the MCP, an
	// alert rule) should not pay for it. One grouped query covers the whole page.
	if r.URL.Query().Get("history") == "true" && len(list) > 0 {
		ids := make([]string, 0, len(list))
		for _, issue := range list {
			ids = append(ids, issue.ID)
		}
		to := time.Now().UTC()
		from := to.AddDate(0, 0, -listHistoryDays)

		if byIssue, err := issues.GetOccurrenceHistoryBulk(r.Context(), ids, from, to); err != nil {
			// Best-effort: the strip is a scanning aid, and losing it must not
			// cost the listing it decorates.
			log.Printf("issues: failed to load bulk history: %v", err)
		} else {
			for i := range list {
				list[i].History = byIssue[list[i].ID]
			}
		}
	}

	responder.NewWithCount(w, list, total, "", "")
}

// enrichIssues attaches links, assignees and repositories to a page of issues.
//
// Enrichment is best-effort per concern: a failure to resolve, say, the service
// repository must not fail the whole listing, because the core issue data is
// already correct and useful without it.
func enrichIssues(list []structs.Issue) error {
	if len(list) == 0 {
		return nil
	}

	ids := make([]string, 0, len(list))
	services := make([]string, 0, len(list))
	assignees := make([]int64, 0, len(list))
	seenService := map[string]bool{}
	seenAssignee := map[int64]bool{}

	for _, issue := range list {
		ids = append(ids, issue.ID)
		if !seenService[issue.Service] {
			seenService[issue.Service] = true
			services = append(services, issue.Service)
		}
		if issue.AssigneeUserID != nil && !seenAssignee[*issue.AssigneeUserID] {
			seenAssignee[*issue.AssigneeUserID] = true
			assignees = append(assignees, *issue.AssigneeUserID)
		}
	}

	linksByIssue, err := query.ListIssueLinksForIssues(db.SQL, ids)
	if err != nil {
		return err
	}
	reposByService, err := query.ListServiceReposFor(db.SQL, services)
	if err != nil {
		return err
	}
	usersByID, err := query.ListUsersByIDs(db.SQL, assignees)
	if err != nil {
		return err
	}

	for i := range list {
		issue := &list[i]
		issue.Links = linksByIssue[issue.ID]
		if repo, ok := reposByService[issue.Service]; ok {
			r := repo
			issue.Repository = &r
		}
		if issue.AssigneeUserID != nil {
			if user, ok := usersByID[*issue.AssigneeUserID]; ok {
				u := user
				issue.Assignee = &u
			}
		}
	}
	return nil
}

// HandleGetIssue returns one issue with everything the detail view renders:
// links, assignee, repository, comment count and the occurrence sparkline.
func HandleGetIssue(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	issue, err := query.GetIssue(db.SQL, id)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to fetch issue", err)
		return
	}
	if issue == nil {
		responder.Error(w, http.StatusNotFound, "issue not found")
		return
	}

	one := []structs.Issue{*issue}
	if err := enrichIssues(one); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to enrich issue", err)
		return
	}
	issue = &one[0]

	commentType := structs.TimelineComment
	if count, err := query.CountTimeline(db.SQL, query.ListTimelineRequest{
		IssueID: id, Type: &commentType,
	}); err == nil {
		issue.CommentCount = &count
	}

	// The sparkline reads the no-TTL rollup, so it still has shape for an issue
	// whose raw events have long since expired.
	to := time.Now().UTC()
	from := to.AddDate(0, 0, -defaultHistoryDays)
	if history, err := issues.GetOccurrenceHistory(r.Context(), id, from, to); err == nil {
		issue.History = history
	} else {
		log.Printf("issues: failed to load history for %s: %v", id, err)
	}

	responder.New(w, issue)
}

// defaultHistoryDays is the sparkline window on the detail view.
const defaultHistoryDays = 30

// maxIssueBody caps an issue mutation body. Titles and comments are the largest
// fields and are bounded well below this.
const maxIssueBody = 1 << 20

// HandleUpdateIssue applies a partial update and records each changed field as
// its own timeline entry, attributed to the caller.
//
// This is where the actor plumbing pays off: a status change made by monitor-mcp
// is recorded against "monitor-mcp", not left anonymous as it was before.
func HandleUpdateIssue(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	actor, ok := middleware.GetActor(r.Context())
	if !ok {
		responder.Error(w, http.StatusUnauthorized, "authentication required")
		return
	}

	// The body is read once and decoded twice: into a typed struct for values,
	// and into a raw map to tell an explicit JSON null from an omitted key. A
	// single pointer cannot express both "leave alone" and "clear", and the
	// difference matters — {"priority": null} means unset it, while omitting the
	// key must not.
	rawBody, err := io.ReadAll(io.LimitReader(r.Body, maxIssueBody))
	if err != nil {
		responder.Error(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	var body struct {
		Status         *string `json:"status"`
		Priority       *string `json:"priority"`
		Title          *string `json:"title"`
		AssigneeUserID *int64  `json:"assignee_user_id"`
	}
	if err := json.Unmarshal(rawBody, &body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	present := map[string]json.RawMessage{}
	if err := json.Unmarshal(rawBody, &present); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	isNull := func(key string) bool {
		v, ok := present[key]
		return ok && string(v) == "null"
	}

	before, err := query.GetIssue(db.SQL, id)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to fetch issue", err)
		return
	}
	if before == nil {
		responder.Error(w, http.StatusNotFound, "issue not found")
		return
	}

	req := query.UpdateIssueRequest{}

	if body.Status != nil {
		status := structs.IssueStatus(*body.Status)
		if !status.IsValid() {
			responder.Error(w, http.StatusBadRequest,
				fmt.Sprintf("invalid status %q (expected unresolved, in_progress, resolved or ignored)", *body.Status))
			return
		}
		req.Status = &status
	}
	req.ClearPriority = isNull("priority")
	req.ClearTitle = isNull("title")
	req.ClearAssignee = isNull("assignee_user_id")

	if body.Priority != nil {
		priority := structs.IssuePriority(*body.Priority)
		if !priority.IsValid() {
			responder.Error(w, http.StatusBadRequest,
				fmt.Sprintf("invalid priority %q (expected low, medium, high or critical)", *body.Priority))
			return
		}
		req.Priority = &priority
	}
	if body.Title != nil {
		req.Title = body.Title
	}
	if body.AssigneeUserID != nil {
		req.AssigneeUserID = body.AssigneeUserID
	}

	if req.IsEmpty() {
		responder.Error(w, http.StatusBadRequest, "no updatable fields supplied")
		return
	}

	updated, err := query.UpdateIssue(db.SQL, id, req)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusBadRequest, "failed to update issue", err)
		return
	}
	if updated == nil {
		responder.Error(w, http.StatusNotFound, "issue not found")
		return
	}

	appendUpdateTimeline(id, actor, before, updated)
	responder.New(w, updated, "issue updated")
}

// appendUpdateTimeline records one entry per field that actually changed.
//
// Comparing before/after rather than trusting the request body means a no-op
// write (setting status to what it already was) leaves no entry, so the timeline
// reflects changes rather than attempts. Failures are logged, never propagated:
// the update is already durable, and losing an entry must not report the whole
// operation as failed.
func appendUpdateTimeline(issueID string, actor *structs.Actor, before, after *structs.Issue) {
	type entry struct {
		typ      structs.TimelineEntryType
		body     string
		metadata map[string]any
	}
	var entries []entry

	if before.Status != after.Status {
		entries = append(entries, entry{
			typ:      structs.TimelineStatusChanged,
			body:     fmt.Sprintf("Status changed from %s to %s.", before.Status, after.Status),
			metadata: map[string]any{"from": string(before.Status), "to": string(after.Status)},
		})
	}
	if !equalPriority(before.Priority, after.Priority) {
		entries = append(entries, entry{
			typ:      structs.TimelinePriorityChanged,
			body:     fmt.Sprintf("Priority changed from %s to %s.", priorityLabel(before.Priority), priorityLabel(after.Priority)),
			metadata: map[string]any{"from": priorityLabel(before.Priority), "to": priorityLabel(after.Priority)},
		})
	}
	if !equalStringPtr(before.Title, after.Title) {
		entries = append(entries, entry{
			typ:      structs.TimelineTitleChanged,
			body:     "Title updated.",
			metadata: map[string]any{"from": derefString(before.Title), "to": derefString(after.Title)},
		})
	}
	if !equalInt64Ptr(before.AssigneeUserID, after.AssigneeUserID) {
		if after.AssigneeUserID == nil {
			entries = append(entries, entry{
				typ:      structs.TimelineUnassigned,
				body:     "Unassigned.",
				metadata: map[string]any{"from": before.AssigneeUserID},
			})
		} else {
			entries = append(entries, entry{
				typ:      structs.TimelineAssigned,
				body:     "Assigned.",
				metadata: map[string]any{"to": *after.AssigneeUserID},
			})
		}
	}

	for _, e := range entries {
		if _, err := query.AppendTimelineEntry(db.SQL, query.AppendTimelineEntryRequest{
			IssueID:  issueID,
			Type:     e.typ,
			Actor:    actor,
			Body:     &e.body,
			Metadata: e.metadata,
		}); err != nil {
			log.Printf("issues: failed to record %s on %s: %v", e.typ, issueID, err)
		}
	}
}

func equalStringPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func equalInt64Ptr(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func equalPriority(a, b *structs.IssuePriority) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func priorityLabel(p *structs.IssuePriority) string {
	if p == nil {
		return "none"
	}
	return string(*p)
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func HandleGetIssueEvents(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	// Get the issue to find its fingerprint
	issue, err := issues.Get(r.Context(), id)
	if err != nil {
		responder.Error(w, http.StatusNotFound, "issue not found")
		return
	}

	// Query events matching this issue's service and name, with error/fatal level
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}
	if limit > 500 {
		limit = 500
	}

	// Fast path: events ingested since 004_events_issue_id.sql carry the issue id
	// on the row, so membership is an indexed equality match — exact, and not
	// bounded by any scan window.
	events, err := queryEventsByIssueID(r.Context(), issue.ID, limit)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to query events", err)
		return
	}
	if len(events) >= limit {
		responder.New(w, events)
		return
	}

	// Fall back to the pre-column scan for the remainder of the retention window.
	// Rows written before that migration have an empty issue_id, so the fast path
	// alone would silently under-report them. REMOVE THIS once 30 days have passed
	// since deploy — the events TTL will have aged out every unstamped row by then.
	//
	// service + name + (path) is only a cheap PRE-FILTER that narrows the scan —
	// it is a superset, never the answer. All three feed the fingerprint, so an
	// event differing in any of them cannot belong to this issue; but many
	// distinct issues share one service+name (every `scraper.run.failed` across
	// every tenant, say), so membership is decided by recomputing each candidate's
	// fingerprint below. Filtering on the pre-filter alone previously returned
	// other tenants' failures as though they were this issue's occurrences.
	//
	// The scan is restricted to issue_id = '' so it can only ever return rows the
	// fast path could not have: without that, a stamped event would be returned
	// twice once both paths run.
	var query string
	var args []interface{}
	if issue.Path != nil && *issue.Path != "" {
		query = "SELECT timestamp, service, env, job_id, request_id, trace_id, user_id, name, level, data FROM " + db.Database + ".events WHERE issue_id = '' AND service = ? AND name = ? AND level IN ('error', 'fatal') AND (JSONExtractString(data, 'path') = ? OR JSONExtractString(data, 'uri') = ?) ORDER BY timestamp DESC LIMIT ?"
		args = []interface{}{issue.Service, issue.Name, *issue.Path, *issue.Path, candidateScanLimit(limit)}
	} else {
		query = "SELECT timestamp, service, env, job_id, request_id, trace_id, user_id, name, level, data FROM " + db.Database + ".events WHERE issue_id = '' AND service = ? AND name = ? AND level IN ('error', 'fatal') ORDER BY timestamp DESC LIMIT ?"
		args = []interface{}{issue.Service, issue.Name, candidateScanLimit(limit)}
	}
	rows, err := db.Conn.Query(r.Context(), query, args...)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to query events", err)
		return
	}
	defer rows.Close()

	scanned := 0
	for rows.Next() {
		if len(events) >= limit {
			break
		}
		var e structs.Event
		var dataStr string
		if err := rows.Scan(&e.Timestamp, &e.Service, &e.Env, &e.JobID, &e.RequestID, &e.TraceID, &e.UserID, &e.Name, &e.Level, &dataStr); err != nil {
			responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to scan event", err)
			return
		}
		if dataStr != "" && dataStr != "{}" {
			json.Unmarshal([]byte(dataStr), &e.Data)
		}
		scanned++
		if issues.FingerprintForEvent(&e) != issue.Fingerprint {
			continue
		}
		events = append(events, &e)
	}

	// A sparse issue buried among high-volume siblings can exhaust the candidate
	// window before filling the page. Say so rather than letting an incomplete
	// page read as "that's all there is". Only the legacy scan can hit this — the
	// indexed path above has no such window.
	if len(events) < limit && scanned >= candidateScanLimit(limit) {
		log.Printf("issues: legacy event scan window exhausted for issue %s (%d matched of %d scanned); older occurrences may exist", id, len(events), scanned)
	}

	// Both paths order newest-first independently, so a merged page has to be
	// re-sorted before it is truncated.
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Timestamp.After(events[j].Timestamp)
	})
	if len(events) > limit {
		events = events[:limit]
	}

	responder.New(w, events)
}

// queryEventsByIssueID returns an issue's events by indexed equality on the
// stamped issue_id. This is the path that replaces scan-and-recompute: exact
// membership, no candidate window, and no fingerprinting in Go.
func queryEventsByIssueID(ctx context.Context, issueID string, limit int) ([]*structs.Event, error) {
	const q = "SELECT timestamp, service, env, job_id, request_id, trace_id, user_id, name, level, data FROM %s.events WHERE issue_id = ? ORDER BY timestamp DESC LIMIT ?"

	rows, err := db.Conn.Query(ctx, fmt.Sprintf(q, db.Database), issueID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := []*structs.Event{}
	for rows.Next() {
		var e structs.Event
		var dataStr string
		if err := rows.Scan(&e.Timestamp, &e.Service, &e.Env, &e.JobID, &e.RequestID, &e.TraceID, &e.UserID, &e.Name, &e.Level, &dataStr); err != nil {
			return nil, err
		}
		if dataStr != "" && dataStr != "{}" {
			json.Unmarshal([]byte(dataStr), &e.Data)
		}
		e.IssueID = issueID
		events = append(events, &e)
	}
	return events, rows.Err()
}

// listHistoryDays is the activity-strip window on the list. Shorter than the
// detail view's window because a row is a few pixels tall and a longer span
// would compress each day past the point of being readable.
const listHistoryDays = 14

// candidateScanLimit sizes the pre-filter window. Events for one issue can be
// heavily diluted by siblings sharing its service+name, so scan well past the
// requested page size — bounded, so a pathological issue can't scan the table.
func candidateScanLimit(limit int) int {
	const maxScan = 10000
	scan := limit * 20
	if scan > maxScan {
		scan = maxScan
	}
	return scan
}
