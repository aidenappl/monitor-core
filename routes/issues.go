package routes

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/issues"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/gorilla/mux"
)

func HandleListIssues(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	service := r.URL.Query().Get("service")
	limit := 50
	offset := 0

	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil {
			offset = parsed
		}
	}

	list, total, err := issues.List(r.Context(), status, service, limit, offset)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list issues", err)
		return
	}
	if list == nil {
		list = []issues.Issue{}
	}
	responder.NewWithCount(w, list, total, "", "")
}

func HandleGetIssue(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	issue, err := issues.Get(r.Context(), id)
	if err != nil {
		responder.Error(w, http.StatusNotFound, "issue not found")
		return
	}

	responder.New(w, issue)
}

func HandleUpdateIssue(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Status == "" {
		responder.Error(w, http.StatusBadRequest, "status is required")
		return
	}

	if err := issues.UpdateStatus(r.Context(), id, body.Status); err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	responder.New(w, nil, "issue status updated")
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

	// service + name + (path) is only a cheap PRE-FILTER that narrows the scan —
	// it is a superset, never the answer. All three feed the fingerprint, so an
	// event differing in any of them cannot belong to this issue; but many
	// distinct issues share one service+name (every `scraper.run.failed` across
	// every tenant, say), so membership is decided by recomputing each candidate's
	// fingerprint below. Filtering on the pre-filter alone previously returned
	// other tenants' failures as though they were this issue's occurrences.
	var query string
	var args []interface{}
	if issue.Path != "" {
		query = "SELECT timestamp, service, env, job_id, request_id, trace_id, user_id, name, level, data FROM " + db.Database + ".events WHERE service = ? AND name = ? AND level IN ('error', 'fatal') AND (JSONExtractString(data, 'path') = ? OR JSONExtractString(data, 'uri') = ?) ORDER BY timestamp DESC LIMIT ?"
		args = []interface{}{issue.Service, issue.Name, issue.Path, issue.Path, candidateScanLimit(limit)}
	} else {
		query = "SELECT timestamp, service, env, job_id, request_id, trace_id, user_id, name, level, data FROM " + db.Database + ".events WHERE service = ? AND name = ? AND level IN ('error', 'fatal') ORDER BY timestamp DESC LIMIT ?"
		args = []interface{}{issue.Service, issue.Name, candidateScanLimit(limit)}
	}
	rows, err := db.Conn.Query(r.Context(), query, args...)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to query events", err)
		return
	}
	defer rows.Close()

	events := []*structs.Event{}
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
	// page read as "that's all there is".
	if len(events) < limit && scanned >= candidateScanLimit(limit) {
		log.Printf("issues: event scan window exhausted for issue %s (%d matched of %d scanned); older occurrences may exist", id, len(events), scanned)
	}

	responder.New(w, events)
}

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
