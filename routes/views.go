package routes

import (
	"encoding/json"
	"net/http"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/gorilla/mux"
)

// Saved views moved from ClickHouse to MariaDB in migration 124, so these
// handlers now call the query layer directly against db.SQL. The `views` package
// they used to go through held nothing but the forwarding.
//
// The tenant comes from requireProject (routes/issues.go) — the same helper the
// issue and dashboard routes use — and never from the request body or a query
// parameter of these routes' own.

func HandleListViews(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	// ?page= stays optional and ?project= is not read here: the page is a filter
	// the caller may omit, while the tenant is decided by the credential. Reading
	// a project off this query string would let a session pick a project the
	// middleware never validated.
	page := r.URL.Query().Get("page")

	list, err := query.ListSavedViews(db.SQL, project, page)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list views", err)
		return
	}
	responder.New(w, list)
}

func HandleCreateView(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	var body query.CreateSavedViewRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Name == "" {
		responder.Error(w, http.StatusBadRequest, "name is required")
		return
	}

	view, err := query.CreateSavedView(db.SQL, project, body)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to create view", err)
		return
	}

	responder.New(w, view)
}

func HandleDeleteView(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	// The project reaches the DELETE's own WHERE (query.DeleteSavedView), so an
	// id lifted from another project's response deletes nothing rather than
	// deleting their view and reporting success.
	if _, err := query.DeleteSavedView(db.SQL, project, id); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete view", err)
		return
	}

	responder.New(w, nil, "view deleted")
}
