package routes

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/gorilla/mux"
)

// Service→repository mappings are per project as of migration 133: the primary
// key is (project, service), because a service name is unique only inside one
// project's event stream. Every handler here therefore resolves its tenant with
// requireProject (routes/issues.go) and threads it into the query, so `api` in
// one project and `api` in another are two mappings rather than one contested
// row.

// HandleListServiceRepos returns this project's service→repository mappings.
func HandleListServiceRepos(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	repos, err := query.ListServiceRepos(db.SQL, project)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list service repositories", err)
		return
	}
	responder.New(w, repos)
}

// HandleGetServiceRepo returns one service's mapping in this project.
//
// A mapping another tenant made for the same service name reads as absent and
// 404s, which is the same answer an unmapped service gives — the caller learns
// nothing about what other projects have mapped.
func HandleGetServiceRepo(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	service := mux.Vars(r)["service"]
	if service == "" {
		responder.Error(w, http.StatusBadRequest, "service is required")
		return
	}

	repo, err := query.GetServiceRepo(db.SQL, project, service)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to fetch service repository", err)
		return
	}
	if repo == nil {
		responder.Error(w, http.StatusNotFound, "service is not mapped to a repository")
		return
	}
	responder.New(w, repo)
}

type upsertServiceRepoBody struct {
	// Repository accepts either "owner/repo" or a full GitHub URL, so a mapping
	// can be pasted straight from the browser.
	Repository    string  `json:"repository"`
	Owner         string  `json:"owner"`
	Repo          string  `json:"repo"`
	DefaultBranch *string `json:"default_branch"`
}

// HandleUpsertServiceRepo creates or replaces a service's mapping in this
// project.
//
// The upsert now collides on (project, service) rather than on `service` alone,
// so mapping `api` here can no longer silently rewrite another tenant's mapping
// for a service that merely shares its name — which is what migration 133 exists
// to stop.
func HandleUpsertServiceRepo(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	service := mux.Vars(r)["service"]
	if service == "" {
		responder.Error(w, http.StatusBadRequest, "service is required")
		return
	}

	var body upsertServiceRepoBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.ErrorWithCause(w, http.StatusBadRequest, "invalid request body", err)
		return
	}

	owner, repo := body.Owner, body.Repo
	if body.Repository != "" {
		var err error
		owner, repo, err = parseRepository(body.Repository)
		if err != nil {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if owner == "" || repo == "" {
		responder.Error(w, http.StatusBadRequest, "repository (or owner and repo) is required")
		return
	}

	saved, err := query.UpsertServiceRepo(db.SQL, project, query.UpsertServiceRepoRequest{
		Service:       service,
		Owner:         owner,
		Repo:          repo,
		DefaultBranch: body.DefaultBranch,
	})
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to save service repository", err)
		return
	}
	responder.New(w, saved)
}

// HandleDeleteServiceRepo removes this project's mapping for a service.
//
// Deleting is scoped for the same reason reading is: unmapping `api` must not
// unmap it for every other project that runs a service by that name, which would
// strip their issues of repository links with nothing to explain it.
func HandleDeleteServiceRepo(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	service := mux.Vars(r)["service"]
	if service == "" {
		responder.Error(w, http.StatusBadRequest, "service is required")
		return
	}

	deleted, err := query.DeleteServiceRepo(db.SQL, project, service)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete service repository", err)
		return
	}
	if !deleted {
		responder.Error(w, http.StatusNotFound, "service is not mapped to a repository")
		return
	}
	responder.New(w, nil, "service repository mapping removed")
}

// parseRepository accepts "owner/repo" or any github.com URL naming a repo.
//
// Trailing path segments are ignored so a URL copied from anywhere in the repo —
// a file view, a PR, the issues tab — resolves to the repository it belongs to.
func parseRepository(input string) (owner, repo string, err error) {
	s := strings.TrimSpace(input)
	s = strings.TrimPrefix(s, "https://")
	s = strings.TrimPrefix(s, "http://")
	s = strings.TrimPrefix(s, "www.")
	s = strings.TrimPrefix(s, "github.com/")
	s = strings.TrimSuffix(strings.Trim(s, "/"), ".git")

	parts := strings.Split(s, "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf(`repository must be "owner/repo" or a github.com URL`)
	}
	return parts[0], parts[1], nil
}
