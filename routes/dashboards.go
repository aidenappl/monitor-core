package routes

import (
	"encoding/json"
	"net/http"

	"github.com/aidenappl/monitor-core/dashboards"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/gorilla/mux"
)

func HandleListDashboards(w http.ResponseWriter, r *http.Request) {
	list, err := dashboards.List(r.Context())
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list dashboards", err)
		return
	}
	if list == nil {
		list = []dashboards.Dashboard{}
	}
	responder.New(w, list)
}

func HandleCreateDashboard(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Config      string `json:"config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Name == "" {
		responder.Error(w, http.StatusBadRequest, "name is required")
		return
	}

	dashboard, err := dashboards.Create(r.Context(), body.Name, body.Description, body.Config)
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

	dashboard, err := dashboards.Get(r.Context(), id)
	if err != nil {
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

	var body struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Config      string `json:"config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	dashboard, err := dashboards.Update(r.Context(), id, body.Name, body.Description, body.Config)
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

	if err := dashboards.Delete(r.Context(), id); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete dashboard", err)
		return
	}

	responder.New(w, nil, "dashboard deleted")
}
