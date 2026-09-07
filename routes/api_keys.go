package routes

import (
	"encoding/json"
	"errors"
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
	// ProjectSlug names the tenant the new key will file events under. Omitting
	// it falls back to env.DefaultProjectSlug inside apikeys.Create — the same
	// project every pre-existing key was backfilled to — so the existing admin UI
	// and monitor-mcp, neither of which sends one yet, keep working unchanged.
	var body struct {
		Name        string `json:"name"`
		Scope       string `json:"scope"`
		ProjectSlug string `json:"project_slug"`
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

	result, err := apikeys.Create(r.Context(), body.Name, scope, body.ProjectSlug)
	if err != nil {
		// Both of these are the caller's to fix, so they must not read as 500s.
		// Naming a key twice within one project became possible to get wrong in
		// migration 117, which added UNIQUE(project_id, name) where nothing had
		// constrained name before.
		switch {
		case errors.Is(err, apikeys.ErrDuplicateKeyName):
			responder.Error(w, http.StatusConflict, err.Error())
		case errors.Is(err, apikeys.ErrUnknownProject):
			responder.Error(w, http.StatusBadRequest, err.Error())
		default:
			responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to create api key", err)
		}
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
