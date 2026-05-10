package routes

import (
	"encoding/json"
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

	query := "SELECT timestamp, service, env, job_id, request_id, trace_id, user_id, name, level, data FROM " + db.Database + ".events WHERE service = ? AND name = ? AND level IN ('error', 'fatal') ORDER BY timestamp DESC LIMIT ?"
	rows, err := db.Conn.Query(r.Context(), query, issue.Service, issue.Name, limit)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to query events", err)
		return
	}
	defer rows.Close()

	var events []*structs.Event
	for rows.Next() {
		var e structs.Event
		var dataStr string
		if err := rows.Scan(&e.Timestamp, &e.Service, &e.Env, &e.JobID, &e.RequestID, &e.TraceID, &e.UserID, &e.Name, &e.Level, &dataStr); err != nil {
			responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to scan event", err)
			return
		}
		if dataStr != "" && dataStr != "{}" {
			json.Unmarshal([]byte(dataStr), &e.Data)
		}
		events = append(events, &e)
	}

	if events == nil {
		events = []*structs.Event{}
	}

	responder.New(w, events)
}
