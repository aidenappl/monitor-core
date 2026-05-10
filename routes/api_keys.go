package routes

import (
	"encoding/json"
	"net/http"

	"github.com/aidenappl/monitor-core/apikeys"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/gorilla/mux"
)

func HandleListAPIKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := apikeys.List(r.Context())
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list api keys", err)
		return
	}
	if keys == nil {
		keys = []apikeys.APIKey{}
	}
	responder.New(w, keys)
}

func HandleCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name  string `json:"name"`
		Scope string `json:"scope"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Name == "" {
		responder.Error(w, http.StatusBadRequest, "name is required")
		return
	}

	scope := apikeys.Scope(body.Scope)
	if scope == "" {
		scope = apikeys.ScopeAdmin
	}

	result, err := apikeys.Create(r.Context(), body.Name, scope)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to create api key", err)
		return
	}

	responder.New(w, result)
}

func HandleDeleteAPIKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	if err := apikeys.Delete(r.Context(), id); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete api key", err)
		return
	}

	responder.New(w, nil, "api key deleted")
}
