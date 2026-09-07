package routes

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/gorilla/mux"
)

// HandleListProjects returns the active projects inside one zone — the list the
// project switcher offers, and the set middleware.withSessionProject validates a
// ?project selector against.
//
// The zone is a PATH segment because a project slug is unique only WITHIN its
// zone. A route that listed projects by slug alone would work today, when there
// is one zone, and start returning another zone's rows the moment there are two —
// the same failure query.GetProjectBySlug takes a zone id to prevent.
//
// A read for any authenticated session, with no create/update/delete
// counterpart, for the reason on HandleListZones: slugs are immutable and never
// reusable, so minting one is not a thing to expose behind a form in this phase.
//
// ⚠️ Do not append the ?project selector when calling this route. It runs through
// QueryAuthMiddleware like every other /v1 path, so a stale selection would 400
// the very request the client needs in order to discover a valid one. Answering
// the registry without a selection is what keeps a bad selection recoverable.
func HandleListProjects(w http.ResponseWriter, r *http.Request) {
	zoneSlug := strings.TrimSpace(mux.Vars(r)["zone"])
	if zoneSlug == "" {
		responder.Error(w, http.StatusBadRequest, "zone is required")
		return
	}

	limit, offset, ok := registryListPage(w, r)
	if !ok {
		return
	}

	// query.GetZoneBySlug returns retired zones too, and that is wanted here: a
	// bookmark into a zone an operator has since retired should still be able to
	// render its switcher and let the user move somewhere live, rather than
	// dead-ending on a 404. The projects themselves are still filtered to active.
	zone, err := query.GetZoneBySlug(db.SQL, zoneSlug)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to look up zone", err)
		return
	}
	if zone == nil {
		responder.Error(w, http.StatusNotFound, fmt.Sprintf("unknown zone %q", zoneSlug))
		return
	}

	// Same opt-in as the zone list, for the same reason: the admin registry page
	// manages retirement and must be able to see a spent slug, while the switcher
	// must not offer one. See registryIncludeDeleted in HandleListZones.router.go.
	includeDeleted, ok := registryIncludeDeleted(w, r)
	if !ok {
		return
	}

	projects, err := query.ListProjects(db.SQL, zone.ID, query.ListProjectsRequest{
		IncludeDeleted: includeDeleted,
		Limit:          limit,
		Offset:         offset,
	})
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list projects", err)
		return
	}
	if projects == nil {
		projects = []structs.Project{}
	}

	// Which of these an unset ?project resolves to. Without it the switcher has
	// no way to know, so it renders the install default TWICE — once as the
	// "nothing selected" row and once as an ordinary project — two rows selecting
	// the same tenant by two different URLs, with two checked states that can
	// never both be right.
	//
	// Sent as a sibling field rather than a flag on the Project row because it is
	// a property of the INSTALL (env.DefaultProjectSlug), not of the project: the
	// same row stops being the default the moment that variable changes, and
	// nothing about the row itself would have to change with it.
	responder.New(w, ListProjectsResponse{
		Projects:           projects,
		DefaultProjectSlug: env.DefaultProjectSlug,
	})
}

// ListProjectsResponse is the payload of GET /v1/zones/{zone}/projects.
type ListProjectsResponse struct {
	Projects           []structs.Project `json:"projects"`
	DefaultProjectSlug string            `json:"default_project_slug"`
}
