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
//
// Every handler here resolves its tenant through requireProject (routes/issues.go),
// the SAME helper the issue routes use, rather than a second one of its own. Two
// helpers would be two chances to disagree about what a missing project means,
// and the answer — a 500, because /v1 sits behind QueryAuthMiddleware and a
// project is therefore always present unless the route was mis-registered — is a
// statement about how the router is wired, not about dashboards.

func HandleListDashboards(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	list, err := query.ListDashboards(db.SQL, project)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list dashboards", err)
		return
	}
	responder.New(w, list)
}

func HandleCreateDashboard(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	var body query.CreateDashboardRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Name == "" {
		responder.Error(w, http.StatusBadRequest, "name is required")
		return
	}

	// The project is passed alongside the decoded body, never read out of it:
	// the credential decides which tenant a dashboard lands in, so no JSON field
	// can file one into a project the caller cannot read back.
	dashboard, err := query.CreateDashboard(db.SQL, project, body)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to create dashboard", err)
		return
	}

	responder.New(w, dashboard)
}

func HandleGetDashboard(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	// A dashboard belonging to another project comes back nil and 404s here, so
	// a foreign id is indistinguishable from one that never existed.
	dashboard, err := query.GetDashboard(db.SQL, project, id)
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
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

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

	dashboard, err := query.UpdateDashboard(db.SQL, project, id, body)
	if err != nil {
		responder.Error(w, http.StatusNotFound, "dashboard not found")
		return
	}

	responder.New(w, dashboard)
}

func HandleDeleteDashboard(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	if _, err := query.DeleteDashboard(db.SQL, project, id); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete dashboard", err)
		return
	}

	responder.New(w, nil, "dashboard deleted")
}
