package routes

import (
	"encoding/json"
	"net/http"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/gorilla/mux"
)

// Dashboards moved from ClickHouse to MariaDB in migration 123, so these
// handlers now call the query layer directly against db.SQL. The `dashboards`
// package they used to go through held nothing but the forwarding.

func HandleListDashboards(w http.ResponseWriter, r *http.Request) {
	list, err := query.ListDashboards(db.SQL)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list dashboards", err)
		return
	}
	responder.New(w, list)
}

func HandleCreateDashboard(w http.ResponseWriter, r *http.Request) {
	var body query.CreateDashboardRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Name == "" {
		responder.Error(w, http.StatusBadRequest, "name is required")
		return
	}

	dashboard, err := query.CreateDashboard(db.SQL, body)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to create dashboard", err)
		return
	}

	responder.New(w, dashboard)
}

func HandleGetDashboard(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	dashboard, err := query.GetDashboard(db.SQL, id)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to fetch dashboard", err)
		return
	}
	if dashboard == nil {
		responder.Error(w, http.StatusNotFound, "dashboard not found")
		return
	}

	responder.New(w, dashboard)
}

func HandleUpdateDashboard(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	var body query.UpdateDashboardRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	dashboard, err := query.UpdateDashboard(db.SQL, id, body)
	if err != nil {
		responder.Error(w, http.StatusNotFound, "dashboard not found")
		return
	}

	responder.New(w, dashboard)
}

func HandleDeleteDashboard(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	if _, err := query.DeleteDashboard(db.SQL, id); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete dashboard", err)
		return
	}

	responder.New(w, nil, "dashboard deleted")
}
