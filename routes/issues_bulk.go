package routes

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/telemetry"
)

// bulkFilterKeys is the subset of GET /v1/issues parameters a bulk filter may
// carry. sort, order, limit and offset are deliberately absent: they shape a
// page, and a bulk write is over the whole match set — accepting `limit` would
// read as "only touch the first N" while meaning nothing of the kind.
var bulkFilterKeys = map[string]bool{
	"status": true, "service": true, "q": true, "assignee": true,
	"has_pr": true, "from": true, "to": true,
}

// HandleBulkUpdateIssues sets status, priority and/or assignee on many issues.
//
//	POST /v1/issues/bulk
//	{"status": "resolved", "ids": ["…", "…"]}
//	{"status": "resolved", "filter": {"service": "scraper-service", "q": "429"}, "dry_run": true}
//	{"priority": "high", "assignee_user_id": 7, "ids": ["…"]}
//	{"priority": null, "ids": ["…"]}          // explicit null clears; an omitted key leaves it
//
// At least one of status, priority or assignee_user_id is required. Rows
// already at every requested value are not targets (not counted, not written).
//
// Exactly one of ids or filter. A filter selects the same rows GET /v1/issues
// would for the same parameters, minus those already at the target status, and
// must narrow by at least one field — an empty filter is refused rather than
// taken to mean the whole project. Each pass changes at most
// query.MAX_BULK_ISSUES issues; `remaining` in the response says how many a
// repeat of the same call would still move.
//
// Every changed issue gets the same status_changed timeline entry the single
// update writes, attributed to the caller.
func HandleBulkUpdateIssues(w http.ResponseWriter, r *http.Request) {
	actor, ok := middleware.GetActor(r.Context())
	if !ok {
		responder.Error(w, http.StatusUnauthorized, "authentication required")
		return
	}

	rawBody, err := io.ReadAll(io.LimitReader(r.Body, maxIssueBody))
	if err != nil {
		responder.Error(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	var body struct {
		Status         string         `json:"status"`
		Priority       *string        `json:"priority"`
		AssigneeUserID *int64         `json:"assignee_user_id"`
		IDs            []string       `json:"ids"`
		Filter         map[string]any `json:"filter"`
		DryRun         bool           `json:"dry_run"`
	}
	if err := json.Unmarshal(rawBody, &body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	// Presence, not value, decides whether priority/assignee are being set:
	// {"priority": null} clears it, a missing key leaves it alone.
	present := map[string]json.RawMessage{}
	if err := json.Unmarshal(rawBody, &present); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	_, setPriority := present["priority"]
	_, setAssignee := present["assignee_user_id"]

	status := structs.IssueStatus(body.Status)
	if body.Status != "" && !status.IsValid() {
		responder.Error(w, http.StatusBadRequest,
			fmt.Sprintf("invalid status %q (expected unresolved, in_progress, resolved or ignored)", body.Status))
		return
	}
	var priority *structs.IssuePriority
	if body.Priority != nil {
		p := structs.IssuePriority(*body.Priority)
		if !p.IsValid() {
			responder.Error(w, http.StatusBadRequest,
				fmt.Sprintf("invalid priority %q (expected low, medium, high or critical)", *body.Priority))
			return
		}
		priority = &p
	}
	if body.Status == "" && !setPriority && !setAssignee {
		responder.Error(w, http.StatusBadRequest, "set at least one of status, priority or assignee_user_id")
		return
	}

	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	req := query.BulkUpdateIssueStatusRequest{
		Project:        project,
		Status:         status,
		SetPriority:    setPriority,
		Priority:       priority,
		SetAssignee:    setAssignee,
		AssigneeUserID: body.AssigneeUserID,
		DryRun:         body.DryRun,
	}
	switch {
	case body.IDs != nil && body.Filter != nil:
		responder.Error(w, http.StatusBadRequest, "supply ids or filter, not both")
		return
	case body.IDs != nil:
		if len(body.IDs) == 0 {
			responder.Error(w, http.StatusBadRequest, "ids is empty")
			return
		}
		req.IDs = body.IDs
	case body.Filter != nil:
		filter, err := bulkFilter(body.Filter)
		if err != nil {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		filter.Project = project
		req.Filter = &filter
	default:
		responder.Error(w, http.StatusBadRequest, "one of ids or filter is required")
		return
	}

	result, err := query.BulkUpdateIssueStatus(db.SQL, req)
	if errors.Is(err, query.ErrBulkTooManyIDs) || errors.Is(err, query.ErrBulkNoTarget) || errors.Is(err, query.ErrBulkNothingToSet) {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to update issues", err)
		return
	}

	for _, c := range result.Changes {
		before := &structs.Issue{Status: c.From, Priority: c.FromPriority, AssigneeUserID: c.FromAssignee}
		after := &structs.Issue{Status: c.From, Priority: c.FromPriority, AssigneeUserID: c.FromAssignee}
		if status != "" {
			after.Status = status
		}
		if setPriority {
			after.Priority = priority
		}
		if setAssignee {
			after.AssigneeUserID = body.AssigneeUserID
		}
		appendUpdateTimeline(r.Context(), c.ID, actor, before, after)
	}
	if result.Updated > 0 {
		mode := "ids"
		if req.Filter != nil {
			mode = "filter"
		}
		telemetry.Info(r.Context(), "issue.status.bulk_updated", map[string]any{
			"project":      project,
			"to":           string(status),
			"set_priority": setPriority,
			"set_assignee": setAssignee,
			"mode":         mode,
			"updated":      result.Updated,
			"remaining":    result.Remaining,
		})
	}

	msg := "issues updated"
	if req.DryRun {
		msg = "dry run — nothing changed"
	}
	responder.New(w, result, msg)
}

// bulkFilter converts a JSON filter object into the list endpoint's request,
// through the list endpoint's own parser. Values may be JSON strings, numbers or
// booleans (`"has_pr": true` and `"has_pr": "true"` both work); unknown keys and
// an empty object are refused.
func bulkFilter(raw map[string]any) (query.ListIssuesRequest, error) {
	if len(raw) == 0 {
		return query.ListIssuesRequest{}, errors.New("filter must narrow by at least one field")
	}
	values := url.Values{}
	var unknown []string
	for k, v := range raw {
		if !bulkFilterKeys[k] {
			unknown = append(unknown, k)
			continue
		}
		switch tv := v.(type) {
		case string:
			if strings.TrimSpace(tv) == "" {
				continue
			}
			values.Set(k, tv)
		case bool:
			values.Set(k, fmt.Sprint(tv))
		case float64:
			values.Set(k, fmt.Sprint(int64(tv)))
		default:
			return query.ListIssuesRequest{}, fmt.Errorf("filter.%s must be a string, number or boolean", k)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return query.ListIssuesRequest{}, fmt.Errorf("unsupported filter field(s): %s (allowed: status, service, q, assignee, has_pr, from, to)",
			strings.Join(unknown, ", "))
	}
	// Checked after the loop, not on the raw object: {"q": ""} is non-empty JSON
	// that narrows nothing, and would otherwise select the whole project.
	if len(values) == 0 {
		return query.ListIssuesRequest{}, errors.New("filter must narrow by at least one field")
	}
	return parseIssueFilters(values)
}
