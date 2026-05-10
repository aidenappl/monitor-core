package routes

import (
	"encoding/json"
	"net/http"

	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/views"
	"github.com/gorilla/mux"
)

func HandleListViews(w http.ResponseWriter, r *http.Request) {
	page := r.URL.Query().Get("page")

	list, err := views.List(r.Context(), page)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list views", err)
		return
	}
	if list == nil {
		list = []views.View{}
	}
	responder.New(w, list)
}

func HandleCreateView(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name        string `json:"name"`
		QueryParams string `json:"query_params"`
		Page        string `json:"page"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Name == "" {
		responder.Error(w, http.StatusBadRequest, "name is required")
		return
	}

	view, err := views.Create(r.Context(), body.Name, body.QueryParams, body.Page)
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

	if err := views.Delete(r.Context(), id); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete view", err)
		return
	}

	responder.New(w, nil, "view deleted")
}
