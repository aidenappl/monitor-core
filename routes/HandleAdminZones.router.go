package routes

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/probe"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/tools"
	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

// The zone half of the registry WRITE surface: POST /admin/zones,
// PUT /admin/zones/{id}, POST /admin/zones/{id}/retire and
// POST /admin/zones/{id}/probe.
//
// ⚠️ CONTROL PLANE ONLY, AND ADMIN ONLY. router.go registers these inside
// `role.RunsControlPlane()` behind SessionMiddleware + RequireAdmin, next to the
// SSO provider CRUD, and that is the whole of the access model — there is no
// second check in here to keep in step with it. A zone process must not be able
// to mint zones: the registry is the map, and a box that can add itself to the
// map can point a row anywhere.
//
// THE READS ARE SOMEWHERE ELSE ON PURPOSE. GET /v1/zones is
// HandleListZones.router.go, is available to any authenticated session, and is
// what the project switcher populates from. Writing is a different privilege from
// looking, and putting them on one route would mean either exposing creation to
// every session or hiding the switcher behind an admin role.
//
// NOTE WHAT IS ABSENT: there is no DELETE verb anywhere on this surface, for
// zones or for projects. Retirement is POST .../retire — an UPDATE that keeps the
// row forever so the UNIQUE key on slug makes reuse structurally impossible (see
// query.RetireZone). Spelling it as DELETE would be one refactor away from a
// handler that actually deletes, and a recycled slug silently reattaches up to a
// month of the previous owner's events plus a permanent daily rollup to the new
// one, with every reference still valid and nothing logged. The verb that does
// not exist cannot be reached for.

// MYSQL_ERR_FOREIGN_KEY is MariaDB's ER_NO_REFERENCED_ROW_2 — the error a
// projects INSERT raises when its zone_id names a zone that is not there.
const MYSQL_ERR_FOREIGN_KEY = 1452

// registryWriteError maps a registry write failure onto a status code.
//
// EVERY BRANCH HERE IS THE CALLER'S MISTAKE, and the default is not. Collapsing
// them into one 500 would tell an operator who reused a spent slug that the
// server is broken, and would tell an operator whose zone still has live tenants
// exactly the same thing — while genuinely broken and merely-refused are the two
// states an admin surface most needs to keep apart. The pattern is the one
// routes/api_keys.go uses: typed sentinels and errors.Is, never message parsing,
// because messages get reworded and error identities do not.
func registryWriteError(w http.ResponseWriter, err error, context string) {
	var mysqlErr *mysql.MySQLError

	switch {
	case errors.Is(err, query.ErrZoneNotFound), errors.Is(err, query.ErrProjectNotFound):
		responder.Error(w, http.StatusNotFound, err.Error())

	// Retiring something twice is a conflict rather than a 404: the row is right
	// there, and the second caller needs to know the state changed under them —
	// not to go looking for a row that does not exist.
	case errors.Is(err, query.ErrZoneAlreadyRetired), errors.Is(err, query.ErrProjectAlreadyRetired):
		responder.Error(w, http.StatusConflict, err.Error())

	// The refusal this registry exists to make: a zone with live tenants inside
	// it. The message carries the count, which is the actionable half.
	case errors.Is(err, query.ErrZoneHasActiveProjects):
		responder.Error(w, http.StatusConflict, err.Error())

	case errors.As(err, &mysqlErr) && mysqlErr.Number == MYSQL_ERR_DUPLICATE_SLUG:
		// A UNIQUE violation on this table is ALWAYS a slug that is already spent,
		// and "spent" includes retired rows — that is the entire point of keeping
		// them. Reported as 409 with the plain fact rather than the driver's text,
		// which names an index and not a decision.
		responder.Error(w, http.StatusConflict,
			"that slug is already taken — slugs are never reused, including by retired rows, so pick another")

	case errors.As(err, &mysqlErr) && mysqlErr.Number == MYSQL_ERR_FOREIGN_KEY:
		// Only reachable as a race: the handlers resolve the zone first and 404
		// there. Getting here means the zone was retired-and-removed (which cannot
		// happen) or the id stopped existing between two statements, so the caller
		// is still the one who can act on it.
		responder.Error(w, http.StatusConflict, "the zone this refers to no longer exists")

	default:
		responder.ErrorWithCause(w, http.StatusInternalServerError, context, err)
	}
}

// MYSQL_ERR_DUPLICATE_SLUG is MariaDB's ER_DUP_ENTRY. Named for what it means on
// THIS surface — the zones and projects tables carry exactly one unique index
// each, on the slug — rather than for the generic condition.
const MYSQL_ERR_DUPLICATE_SLUG = 1062

// registryPathID parses the {id} path variable, writing the refusal and
// reporting false on anything that is not a positive integer.
//
// A non-numeric id is a 400 rather than a 404 because the request is malformed,
// not merely pointed at nothing: /admin/zones/undefined is a frontend bug
// stringifying a missing value into a URL, and answering "not found" would send
// whoever is debugging it looking for a row.
func registryPathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	raw := strings.TrimSpace(mux.Vars(r)["id"])
	if raw == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return 0, false
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		responder.Error(w, http.StatusBadRequest, "invalid id "+strconv.Quote(raw))
		return 0, false
	}
	return id, true
}

// decodeJSONBody decodes a request body, writing a 400 and reporting false on
// malformed JSON. DisallowUnknownFields is deliberately NOT set: a field this
// server does not know is refused explicitly where it matters (slug, status —
// see below), and silently rejecting every unrecognised key would break the
// first client to send one for a reason nobody could read from the message.
func decodeJSONBody(w http.ResponseWriter, r *http.Request, into interface{}) bool {
	if err := json.NewDecoder(r.Body).Decode(into); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return false
	}
	return true
}

// createZoneBody is the POST /admin/zones payload. Both URLs are required, and
// that is the invariant the whole registry rests on: a zone row RECORDS
// infrastructure that already exists, and infrastructure with no address is not
// something there is anything to record about.
type createZoneBody struct {
	Slug        string `json:"slug"`
	DisplayName string `json:"display_name"`
	IngestURL   string `json:"ingest_url"`
	QueryURL    string `json:"query_url"`
}

// HandleCreateZone mints a zone — POST /admin/zones.
//
// ⚠️ THIS RECORDS INFRASTRUCTURE; IT DOES NOT CREATE ANY. The stack, its
// ClickHouse, its MariaDB, the DNS record and the certificate are all
// provisioned by hand before this call, and nothing downstream reconciles the row
// against reality. A row whose query_url points at another zone is syntactically
// perfect and semantically catastrophic, which is why the probe exists and why
// the response below carries reachability=unknown rather than a green tick: the
// row is a claim, and POST /admin/zones/{id}/probe is what checks it.
func HandleCreateZone(w http.ResponseWriter, r *http.Request) {
	var body createZoneBody
	if !decodeJSONBody(w, r, &body) {
		return
	}

	// Pre-validated here so a caller's mistake is a 400 that names the field.
	// query.CreateZone validates again and IS the enforcement — these calls
	// delegate to the same one definition in tools/, so there is no second rule to
	// drift, only a second (earlier) place it is applied.
	slug := strings.TrimSpace(body.Slug)
	if err := tools.ValidateSlug(slug); err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(body.DisplayName) == "" {
		responder.Error(w, http.StatusBadRequest, "display_name is required")
		return
	}
	if _, err := tools.NormalizeEndpointURL(body.IngestURL); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid ingest_url: "+err.Error())
		return
	}
	if _, err := tools.NormalizeEndpointURL(body.QueryURL); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid query_url: "+err.Error())
		return
	}

	zone, err := query.CreateZone(db.SQL, query.CreateZoneRequest{
		Slug:        slug,
		DisplayName: body.DisplayName,
		IngestURL:   body.IngestURL,
		QueryURL:    body.QueryURL,
	})
	if err != nil {
		registryWriteError(w, err, "failed to create zone")
		return
	}

	responder.New(w, zone, "zone created")
}

// updateZoneBody is the PUT /admin/zones/{id} payload. Every field is a pointer:
// absent means "leave it alone", which is what makes a partial update expressible
// at all.
//
// Slug and Status are HERE ONLY SO THEY CAN BE REFUSED. See HandleUpdateZone.
type updateZoneBody struct {
	DisplayName *string `json:"display_name"`
	IngestURL   *string `json:"ingest_url"`
	QueryURL    *string `json:"query_url"`

	Slug   *string `json:"slug"`
	Status *string `json:"status"`
}

// HandleUpdateZone edits the mutable fields of a zone — PUT /admin/zones/{id}.
//
// ⚠️ A SLUG CHANGE IS REFUSED HERE, NOT IGNORED, and the two are not close. The
// query layer already makes the change impossible — UpdateZoneRequest has no Slug
// field, so there is no code path that could write one — but a handler that
// simply dropped the key would answer 200 to "rename this zone to X" having
// renamed nothing, and the operator would go on believing the new name is live.
// The mistake is invisible on this side and permanent on the other: the events
// already filed under the old slug keep arriving there, and every reference the
// operator now writes points at a name that does not exist.
//
// Status is refused for the same reason and one more: retirement has a guard
// (RetireZone refuses while live projects remain), and a status edit that slipped
// through here would be a second way to retire that skips it.
func HandleUpdateZone(w http.ResponseWriter, r *http.Request) {
	id, ok := registryPathID(w, r)
	if !ok {
		return
	}

	var body updateZoneBody
	if !decodeJSONBody(w, r, &body) {
		return
	}

	if body.Slug != nil {
		responder.Error(w, http.StatusBadRequest,
			"a zone slug is immutable and can never be reused — create a new zone and retire this one")
		return
	}
	if body.Status != nil {
		responder.Error(w, http.StatusBadRequest,
			"status is not editable here — retire the zone with POST /admin/zones/{id}/retire, which refuses while it still has active projects")
		return
	}

	// Pre-validated for the same reason as on create: a 400 naming the field beats
	// a 500 carrying a wrapped driver error.
	if body.DisplayName != nil && strings.TrimSpace(*body.DisplayName) == "" {
		responder.Error(w, http.StatusBadRequest, "display_name cannot be blank")
		return
	}
	if body.IngestURL != nil {
		if _, err := tools.NormalizeEndpointURL(*body.IngestURL); err != nil {
			responder.Error(w, http.StatusBadRequest, "invalid ingest_url: "+err.Error())
			return
		}
	}
	if body.QueryURL != nil {
		if _, err := tools.NormalizeEndpointURL(*body.QueryURL); err != nil {
			responder.Error(w, http.StatusBadRequest, "invalid query_url: "+err.Error())
			return
		}
	}

	// Resolved first so a missing row is a 404 rather than an UPDATE that matches
	// nothing and reports success — query.UpdateZone re-reads and would return a
	// nil zone with no error, which serialises as `"data": null` and reads as "it
	// worked".
	existing, err := query.GetZone(db.SQL, id)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to look up zone", err)
		return
	}
	if existing == nil {
		responder.Error(w, http.StatusNotFound, fmt.Sprintf("zone %d not found", id))
		return
	}

	zone, err := query.UpdateZone(db.SQL, id, query.UpdateZoneRequest{
		DisplayName: body.DisplayName,
		IngestURL:   body.IngestURL,
		QueryURL:    body.QueryURL,
	})
	if err != nil {
		registryWriteError(w, err, "failed to update zone")
		return
	}

	responder.New(w, zone, "zone updated")
}

// HandleRetireZone retires a zone — POST /admin/zones/{id}/retire.
//
// A SOFT DELETE, AND THE ONLY ONE. query.RetireZone sets status='deleted' and
// keeps the row forever; nothing on this surface can remove it. The refusal that
// matters — a zone that still owns active projects — arrives as
// query.ErrZoneHasActiveProjects and becomes a 409 naming the count, because the
// alternative is a retired zone whose tenants are still ingesting: live data
// nobody can reach and nobody can see.
func HandleRetireZone(w http.ResponseWriter, r *http.Request) {
	id, ok := registryPathID(w, r)
	if !ok {
		return
	}

	zone, err := query.RetireZone(db.SQL, id)
	if err != nil {
		registryWriteError(w, err, "failed to retire zone")
		return
	}

	responder.New(w, zone, "zone retired")
}

// probeZoneResponse is what POST /admin/zones/{id}/probe returns: the verdict,
// and the row it was just written to.
//
// Both, rather than one or the other. The verdict alone loses the timestamp and
// the persistence (a caller could not tell a fresh probe from a replayed one),
// and the row alone would make the caller re-read the enum to find out what just
// happened.
type probeZoneResponse struct {
	Zone         *structs.Zone            `json:"zone"`
	Reachability structs.ZoneReachability `json:"reachability"`
	Detail       string                   `json:"reachability_detail"`
	ReportedZone string                   `json:"reported_zone"`
}

// HandleProbeZone probes a zone now and persists the verdict —
// POST /admin/zones/{id}/probe.
//
// ⚠️ AN UNREACHABLE ZONE IS A 200 HERE. The probe SUCCEEDED; what it found was a
// zone that is down, or worse, a zone that is somebody else. Reporting that as a
// 500 would throw away the one answer the operator asked for and leave the admin
// page unable to distinguish "the probe broke" from "the zone is broken" — the
// same shape as an errors page rendering "no issues" for a failed query, which is
// the most misleading possible reading of a 500.
//
// It is a POST rather than a GET because it has an effect: it makes an outbound
// request to a third party and writes the result. A GET would also be cached and
// prefetched, and a probe that fires on link hover is a probe that lies about
// when it looked.
func HandleProbeZone(w http.ResponseWriter, r *http.Request) {
	id, ok := registryPathID(w, r)
	if !ok {
		return
	}

	zone, err := query.GetZone(db.SQL, id)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to look up zone", err)
		return
	}
	if zone == nil {
		responder.Error(w, http.StatusNotFound, fmt.Sprintf("zone %d not found", id))
		return
	}

	// Bounded by probe.ZONE_PROBE_TIMEOUT inside the probe itself, so a wedged
	// zone cannot hold this request open for the server's WriteTimeout.
	result := probe.Zone(r.Context(), *zone)

	if err := query.RecordZoneProbe(db.SQL, zone.ID, query.RecordZoneProbeRequest{
		Reachability: result.Reachability,
		Detail:       result.Detail,
		ReportedZone: result.ReportedZone,
		ProbedAt:     result.ProbedAt,
	}); err != nil {
		// The verdict is real and the storage of it failed — two different
		// problems, and this is the second one. Reported as a 500 rather than
		// returning the verdict anyway, because a caller that got a verdict would
		// reasonably assume the row now carries it, and the next page load would
		// silently disagree.
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to record the probe result", err)
		return
	}

	// Re-read so the caller gets the row as it now stands, last_probe_at included —
	// the field that makes the reachability readable rather than an undated claim.
	refreshed, err := query.GetZone(db.SQL, zone.ID)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to re-read the zone", err)
		return
	}

	responder.New(w, probeZoneResponse{
		Zone:         refreshed,
		Reachability: result.Reachability,
		Detail:       result.Detail,
		ReportedZone: result.ReportedZone,
	}, "zone probed")
}
