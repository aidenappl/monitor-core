package routes

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
)

// HandleListZones returns the zones this install knows about.
//
// It is a READ available to any authenticated session. The WRITES live on the
// control-plane-only, admin-only /admin surface (routes/HandleAdminZones.router.go);
// looking and changing are different privileges, which is why they are not the
// same route.
//
// Phase 1 is single-zone, so this returns exactly ONE row today. That is the
// expected and correct output, not a bug to hunt: the switcher showing a single
// entry is what a single-zone install looks like.
//
// Retired zones are excluded BY DEFAULT, because this list is what the switcher
// offers and a retired zone is not a choice. The lookup behind
// GET /v1/zones/{zone}/projects still resolves one, so a bookmark into a retired
// zone keeps working rather than 404ing.
//
// ?include_deleted=true asks for them back, and the ADMIN REGISTRY PAGE IS WHY IT
// EXISTS. That page is where retirement is managed, and hiding retired rows there
// would make a spent slug look free — which is the exact impression the
// never-reuse rule exists to prevent. An operator who cannot see that `atlas` was
// retired last month will try to create it again, read the 409 as a bug, and go
// looking for the row that is "missing". The flag is opt-in rather than the
// default so no existing caller starts offering retired zones as destinations.
func HandleListZones(w http.ResponseWriter, r *http.Request) {
	limit, offset, ok := registryListPage(w, r)
	if !ok {
		return
	}

	includeDeleted, ok := registryIncludeDeleted(w, r)
	if !ok {
		return
	}

	zones, err := query.ListZones(db.SQL, query.ListZonesRequest{
		IncludeDeleted: includeDeleted,
		Limit:          limit,
		Offset:         offset,
	})
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list zones", err)
		return
	}
	if zones == nil {
		zones = []structs.Zone{}
	}

	responder.New(w, zones)
}

// registryListPage parses the optional limit/offset on a registry listing,
// writing the refusal and reporting false on a malformed value. Shared with
// HandleListProjects — the two routes are one feature and should page alike.
//
// An ABSENT limit asks for db.MAX_LIMIT rather than letting query.ListZones /
// query.ListProjects clamp to db.DEFAULT_LIMIT (50). That default is right for a
// page of events and wrong here: these two routes are what populates the project
// switcher, and a 51st project that silently failed to appear is a tenant the
// operator simply cannot reach, with nothing logged and no symptom pointing at
// pagination. The registry is a handful of rows by construction — a zone is a
// whole ClickHouse instance — so asking for the ceiling costs nothing, and an
// explicit limit is still honoured for a caller that wants to page.
func registryListPage(w http.ResponseWriter, r *http.Request) (limit, offset int, ok bool) {
	limit = db.MAX_LIMIT

	// An out-of-range limit is REFUSED, not clamped. query.ListZones silently
	// rewrites anything outside (0, MAX_LIMIT] to DEFAULT_LIMIT (50), so a caller
	// asking for 1000 projects would get 50 with no error and nothing logged —
	// reintroducing precisely the truncation cliff the MAX_LIMIT default above
	// exists to avoid, and doing it to the caller who was most explicit about not
	// wanting it. Refusing is louder and cannot mislead.
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			responder.Error(w, http.StatusBadRequest, "invalid limit "+strconv.Quote(v))
			return 0, 0, false
		}
		if n <= 0 || n > db.MAX_LIMIT {
			responder.Error(w, http.StatusBadRequest, fmt.Sprintf("limit must be between 1 and %d", db.MAX_LIMIT))
			return 0, 0, false
		}
		limit = n
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			responder.Error(w, http.StatusBadRequest, "invalid offset "+strconv.Quote(v))
			return 0, 0, false
		}
		if n < 0 {
			responder.Error(w, http.StatusBadRequest, "offset must not be negative")
			return 0, 0, false
		}
		offset = n
	}

	return limit, offset, true
}

// registryIncludeDeleted reads the ?include_deleted selector shared by both
// registry list routes, writing the refusal and reporting false on a value that
// is not a boolean.
//
// A BAD VALUE IS A 400, NOT A SILENT FALSE. `?include_deleted=yes` treated as
// "no" answers 200 with a list that is missing exactly the rows the caller asked
// for, and the caller's next move is to conclude the retired zone is gone — which
// is the one thing the never-reuse rule needs an operator not to believe.
func registryIncludeDeleted(w http.ResponseWriter, r *http.Request) (include bool, ok bool) {
	raw := strings.TrimSpace(r.URL.Query().Get("include_deleted"))
	if raw == "" {
		return false, true
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid include_deleted "+strconv.Quote(raw)+" (expected true or false)")
		return false, false
	}
	return v, true
}
