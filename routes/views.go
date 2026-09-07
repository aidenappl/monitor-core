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

func HandleListViews(w http.ResponseWriter, r *http.Request) {
	page := r.URL.Query().Get("page")

	list, err := query.ListSavedViews(db.SQL, page)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list views", err)
		return
	}
	responder.New(w, list)
}

func HandleCreateView(w http.ResponseWriter, r *http.Request) {
	var body query.CreateSavedViewRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Name == "" {
		responder.Error(w, http.StatusBadRequest, "name is required")
		return
	}

	view, err := query.CreateSavedView(db.SQL, body)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to create view", err)
		return
	}

	responder.New(w, view)
}

func HandleDeleteView(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	if _, err := query.DeleteSavedView(db.SQL, id); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete view", err)
		return
	}

	responder.New(w, nil, "view deleted")
}
