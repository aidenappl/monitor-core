package routes

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/tools"
)

// The project half of the registry WRITE surface: POST /admin/zones/{id}/projects,
// PUT /admin/projects/{id} and POST /admin/projects/{id}/retire.
//
// Control plane only and admin only, registered alongside the zone handlers — see
// the header of HandleAdminZones.router.go, which also owns the shared helpers
// (registryWriteError, registryPathID, decodeJSONBody) these use.
//
// CREATE HANGS OFF THE ZONE and update/retire do not, and that asymmetry follows
// the identifiers. A project slug is unique only WITHIN its zone, so creating one
// needs the zone in the path to be unambiguous; an existing project already has a
// globally unique id, and routing its edits through a zone segment would invite a
// URL whose two halves disagree — /admin/zones/2/projects/7 where project 7 lives
// in zone 1 — with a handler that has to decide which half to believe.

// createProjectBody is the POST /admin/zones/{id}/projects payload. The zone
// comes from the path, not the body, so there is no way to write a request whose
// two zone references disagree.
type createProjectBody struct {
	Slug        string `json:"slug"`
	DisplayName string `json:"display_name"`
}

// HandleCreateProject mints a project inside a zone —
// POST /admin/zones/{id}/projects.
//
// The slug is validated before anything is written because this is the LAST
// moment it is negotiable: it is immutable afterwards and its name is never
// reusable, even once the project is retired. A reserved slug matters more here
// than anywhere else — the reserved list guards the frontend's static route
// table, and a project called `settings` would be shadowed by the app's own
// /settings page and simply unreachable, with the row present, the API serving it
// and nothing at all to indicate why the link lands on the wrong screen.
func HandleCreateProject(w http.ResponseWriter, r *http.Request) {
	zoneID, ok := registryPathID(w, r)
	if !ok {
		return
	}

	var body createProjectBody
	if !decodeJSONBody(w, r, &body) {
		return
	}

	slug := strings.TrimSpace(body.Slug)
	if err := tools.ValidateSlug(slug); err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(body.DisplayName) == "" {
		responder.Error(w, http.StatusBadRequest, "display_name is required")
		return
	}

	// Resolved so a bad zone id is a 404 on the path rather than errno 1452
	// wrapped in a 500. query.CreateProject deliberately leaves the guarantee to
	// the foreign key — a pre-check still races — so this is for the message, not
	// for the safety, and registryWriteError still maps the FK violation if the
	// race happens.
	zone, err := query.GetZone(db.SQL, zoneID)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to look up zone", err)
		return
	}
	if zone == nil {
		responder.Error(w, http.StatusNotFound, fmt.Sprintf("zone %d not found", zoneID))
		return
	}
	// A retired zone is kept forever so its slug can never be reused, but it is
	// not a place to put new tenants: the project would be live inside a zone that
	// no switcher offers, which is the same "live data nobody can see" outcome
	// query.RetireZone refuses in the other direction.
	if zone.Status != structs.ZoneStatusActive {
		responder.Error(w, http.StatusConflict,
			fmt.Sprintf("zone %q is retired — a new project cannot be created inside it", zone.Slug))
		return
	}

	project, err := query.CreateProject(db.SQL, query.CreateProjectRequest{
		ZoneID:      zone.ID,
		Slug:        slug,
		DisplayName: body.DisplayName,
	})
	if err != nil {
		registryWriteError(w, err, "failed to create project")
		return
	}

	responder.New(w, project, "project created")
}

// updateProjectBody is the PUT /admin/projects/{id} payload. Slug, ZoneID and
// Status are present ONLY so they can be refused — see HandleUpdateProject.
type updateProjectBody struct {
	DisplayName *string `json:"display_name"`

	Slug   *string `json:"slug"`
	ZoneID *int64  `json:"zone_id"`
	Status *string `json:"status"`
}

// HandleUpdateProject edits the mutable fields of a project —
// PUT /admin/projects/{id}. Today that is exactly one field, display_name, and
// the list of what it refuses is longer than the list of what it accepts.
//
// ⚠️ THREE EXPLICIT REFUSALS, EACH LOUD RATHER THAN SILENT:
//
//   - SLUG. Immutable, at the API and not merely in the UI. The query layer has
//     no Slug field so the write is already impossible; what this adds is that
//     the caller is TOLD. A handler that dropped the key would answer 200 to a
//     rename that did not happen, and the operator would then use the new name
//     everywhere while every event kept filing under the old one.
//   - ZONE_ID. Moving a project between zones would re-point every event already
//     filed under it — the rows do not move, so the history would simply detach.
//   - STATUS. Retirement is POST /admin/projects/{id}/retire, which returns the
//     retired row and reports "already retired" as a conflict. Two ways to retire
//     is one way to retire plus one way to bypass whatever the first one checks.
func HandleUpdateProject(w http.ResponseWriter, r *http.Request) {
	id, ok := registryPathID(w, r)
	if !ok {
		return
	}

	var body updateProjectBody
	if !decodeJSONBody(w, r, &body) {
		return
	}

	if body.Slug != nil {
		responder.Error(w, http.StatusBadRequest,
			"a project slug is immutable and can never be reused — create a new project and retire this one")
		return
	}
	if body.ZoneID != nil {
		responder.Error(w, http.StatusBadRequest,
			"a project cannot be moved between zones — its events are already filed under this one")
		return
	}
	if body.Status != nil {
		responder.Error(w, http.StatusBadRequest,
			"status is not editable here — retire the project with POST /admin/projects/{id}/retire")
		return
	}

	if body.DisplayName != nil && strings.TrimSpace(*body.DisplayName) == "" {
		responder.Error(w, http.StatusBadRequest, "display_name cannot be blank")
		return
	}

	// Resolved first so a missing row is a 404 rather than an UPDATE that matched
	// nothing and a `"data": null` that reads as success.
	existing, err := query.GetProject(db.SQL, id)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to look up project", err)
		return
	}
	if existing == nil {
		responder.Error(w, http.StatusNotFound, fmt.Sprintf("project %d not found", id))
		return
	}

	project, err := query.UpdateProject(db.SQL, id, query.UpdateProjectRequest{
		DisplayName: body.DisplayName,
	})
	if err != nil {
		registryWriteError(w, err, "failed to update project")
		return
	}

	responder.New(w, project, "project updated")
}

// HandleRetireProject retires a project — POST /admin/projects/{id}/retire.
//
// A soft delete, like the zone side, and for the sharper version of the same
// reason: the project dimension is what every event is filed under, so a reused
// project slug would reattach the previous owner's surviving events (30-day TTL)
// and its permanent daily rollup to the new owner. Keeping the row is what makes
// that impossible.
//
// Unlike a zone, a project can always be retired — it strands nothing beneath it —
// so the only refusals are "no such row" (404) and "already retired" (409).
//
// ⚠️ THIS DOES NOT STOP INGESTION. API keys pointing at the project keep
// authenticating and their events keep landing; retiring is a statement about
// what an operator should be OFFERED, not a kill switch on a credential. Revoke
// the keys separately — a retire that silently began dropping accepted events
// would be the lossy failure the whole tenancy model is built to avoid.
func HandleRetireProject(w http.ResponseWriter, r *http.Request) {
	id, ok := registryPathID(w, r)
	if !ok {
		return
	}

	project, err := query.RetireProject(db.SQL, id)
	if err != nil {
		registryWriteError(w, err, "failed to retire project")
		return
	}

	responder.New(w, project, "project retired")
}
