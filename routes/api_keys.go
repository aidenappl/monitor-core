package routes

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-core/apikeys"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/telemetry"
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

	// The key's id, scope and project — never the key, which this response is
	// the one and only place to see.
	telemetry.Info(r.Context(), "api_key.created", map[string]any{
		"key_id":  result.ID,
		"scope":   string(result.Scope),
		"project": result.ProjectSlug,
	})
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
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete api key", err, map[string]any{"key_id": id})
		return
	}

	telemetry.Info(r.Context(), "api_key.deleted", map[string]any{"key_id": id})
	responder.New(w, nil, "api key deleted")
}

// HandleUpdateAPIKey renames a key. PUT /v1/api-keys/{id}, body {"name": "..."}.
//
// Name only, deliberately: scope and project are what the key AUTHORISES and
// WHERE its events land, and the honest way to change either is a new key — an
// edit would silently re-home every event its producer already sends.
func HandleUpdateAPIKey(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	body.Name = strings.TrimSpace(body.Name)
	if body.Name == "" {
		responder.Error(w, http.StatusBadRequest, "name is required")
		return
	}

	key, err := apikeys.Rename(r.Context(), id, body.Name)
	if err != nil {
		switch {
		case errors.Is(err, apikeys.ErrKeyNotFound):
			responder.Error(w, http.StatusNotFound, "api key not found")
		case errors.Is(err, apikeys.ErrDuplicateKeyName):
			responder.Error(w, http.StatusConflict, err.Error())
		default:
			responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to rename api key", err, map[string]any{"key_id": id})
		}
		return
	}

	telemetry.Info(r.Context(), "api_key.renamed", map[string]any{"key_id": id})
	responder.New(w, key)
}
