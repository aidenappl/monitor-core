package query

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/tools"
)

// zonesTable is UNQUALIFIED, unlike issuesTable. The registry lives in the DSN's
// default database (monitor_auth) alongside users and api_keys, not in the
// `monitor` schema — see db/migrations/116_create_registry.sql for why.
const zonesTable = "zones"

// REPORTED_ZONE_MAX_LENGTH matches the VARCHAR(64) on zones.reported_zone.
//
// The column holds what a REMOTE box claimed to be, so its content is untrusted
// and can be any length at all. RecordZoneProbe truncates to fit rather than
// failing: losing the tail of a bogus identity costs nothing, while losing the
// whole probe result because the identity was bogus would throw away the one
// answer the operator asked for.
const REPORTED_ZONE_MAX_LENGTH = 64

// Retirement refusals, as sentinel errors so the handler can map each to the
// right status code instead of parsing a message. ErrZoneHasActiveProjects is
// the load-bearing one: a retired zone whose projects are still live is a tenant
// nobody can reach and nobody can see.
var (
	ErrZoneNotFound          = errors.New("zone not found")
	ErrZoneAlreadyRetired    = errors.New("zone is already retired")
	ErrZoneHasActiveProjects = errors.New("zone still has active projects")
)

// zoneColumns and scanZone are a POSITIONAL PAIR. Adding a column to one and not
// the other is a silent mis-scan — a URL landing in reachability_detail, say —
// not a compile error, which is why query/registry_query_test.go writes its
// fixture rows out in full rather than deriving them from this slice.
var zoneColumns = []string{
	"id", "slug", "display_name", "status",
	"ingest_url", "query_url",
	"reachability", "reachability_detail", "reported_zone", "last_probe_at",
	"created_at", "updated_at",
}

type zoneScanner interface {
	Scan(dest ...interface{}) error
}

func scanZone(row zoneScanner) (*structs.Zone, error) {
	var z structs.Zone
	var status, reachability string

	if err := row.Scan(
		&z.ID, &z.Slug, &z.DisplayName, &status,
		&z.IngestURL, &z.QueryURL,
		&reachability, &z.ReachabilityDetail, &z.ReportedZone, &z.LastProbeAt,
		&z.CreatedAt, &z.UpdatedAt,
	); err != nil {
		return nil, err
	}
	z.Status = structs.ZoneStatus(status)
	z.Reachability = structs.ZoneReachability(reachability)
	return &z, nil
}

// GetZone resolves a zone by primary key. Returns (nil, nil) when absent.
func GetZone(engine db.Queryable, id int64) (*structs.Zone, error) {
	query, args, err := sq.Select(zoneColumns...).From(zonesTable).
		Where(sq.Eq{"id": id}).Limit(1).ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	z, err := scanZone(engine.QueryRow(query, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get zone: %w", err)
	}
	return z, nil
}

// GetZoneBySlug resolves a zone by its (unique) slug. Returns (nil, nil) when
// absent — an unknown slug is a normal state, not an error.
//
// Retired zones are returned like any other. Filtering them out here would make
// a deleted slug look free, and the one thing this registry must never do is
// suggest a spent name is available.
func GetZoneBySlug(engine db.Queryable, slug string) (*structs.Zone, error) {
	query, args, err := sq.Select(zoneColumns...).From(zonesTable).
		Where(sq.Eq{"slug": slug}).Limit(1).ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	z, err := scanZone(engine.QueryRow(query, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get zone by slug: %w", err)
	}
	return z, nil
}

// ListZonesRequest filters the zone list.
type ListZonesRequest struct {
	// IncludeDeleted returns retired zones as well. Off by default: the listing
	// is what an operator picks from, and a retired zone is not a choice.
	//
	// ⚠️ The ADMIN listing turns it ON. That surface is where retirement is
	// managed, and hiding retired rows there would make a spent slug look free —
	// the exact impression the never-reuse rule exists to prevent. See
	// routes/HandleAdminZones.router.go.
	IncludeDeleted bool
	Limit          int
	Offset         int
}

// ListZones returns zones ordered by slug.
//
// The table holds one row today and will hold a handful, but the limit is
// applied anyway — an unbounded list function is the kind of thing that is only
// ever noticed by the query that eventually times out.
func ListZones(engine db.Queryable, req ListZonesRequest) ([]structs.Zone, error) {
	if req.Limit <= 0 || req.Limit > db.MAX_LIMIT {
		req.Limit = db.DEFAULT_LIMIT
	}

	q := sq.Select(zoneColumns...).From(zonesTable)
	if !req.IncludeDeleted {
		q = q.Where(sq.Eq{"status": string(structs.ZoneStatusActive)})
	}
	q = q.OrderBy("slug ASC").Limit(uint64(req.Limit))
	if req.Offset > 0 {
		q = q.Offset(uint64(req.Offset))
	}

	query, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := engine.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("list zones: %w", err)
	}
	defer rows.Close()

	zones := []structs.Zone{}
	for rows.Next() {
		z, err := scanZone(rows)
		if err != nil {
			return nil, fmt.Errorf("scan zone: %w", err)
		}
		zones = append(zones, *z)
	}
	return zones, rows.Err()
}

// CreateZoneRequest mints a new zone. There is no Status field: a zone is always
// born active, and creating one pre-retired is not a state anything needs.
//
// BOTH URLs ARE REQUIRED, and that is the invariant this struct is shaped
// around: a zone row RECORDS infrastructure that already exists, and
// infrastructure with no address is not something there is anything to record
// about. The DDL cannot require them — the pre-existing row has none and SQL
// cannot read the environment to invent one (see the header of
// db/migrations/125_zone_endpoints.sql) — so the only place the rule can live is
// here, at the one moment a new row is created.
type CreateZoneRequest struct {
	Slug        string
	DisplayName string
	IngestURL   string
	QueryURL    string
}

// CreateZone validates the slug and the endpoints, then inserts the zone,
// returning the hydrated row.
//
// This is the ONLY moment the name is negotiable — it is immutable afterwards
// and never reusable — so validation is strict here and nowhere else. The
// surrounding whitespace trim is the single normalization allowed on the slug:
// env vars and pasted values collect it, and it is the one correction that
// cannot change which name the operator meant. Case is NOT folded; `Payments` is
// refused, not silently rewritten to `payments` and then frozen that way forever.
//
// The URLs get one further normalization (a trailing slash is stripped) because
// unlike a slug they are composed with, not compared against — see
// tools.NormalizeEndpointURL.
func CreateZone(engine db.Queryable, req CreateZoneRequest) (*structs.Zone, error) {
	req.Slug = strings.TrimSpace(req.Slug)
	req.DisplayName = strings.TrimSpace(req.DisplayName)

	if err := tools.ValidateSlug(req.Slug); err != nil {
		return nil, fmt.Errorf("invalid zone slug: %w", err)
	}
	if req.DisplayName == "" {
		return nil, fmt.Errorf("display_name is required")
	}

	ingestURL, err := tools.NormalizeEndpointURL(req.IngestURL)
	if err != nil {
		return nil, fmt.Errorf("invalid ingest_url: %w", err)
	}
	queryURL, err := tools.NormalizeEndpointURL(req.QueryURL)
	if err != nil {
		return nil, fmt.Errorf("invalid query_url: %w", err)
	}

	// reachability is left at its 'unknown' default rather than being probed
	// inline. A create that blocked on an outbound HTTP call would fail — or
	// worse, half-fail — because of a zone that is merely not up yet, and the row
	// is a record of intent that a probe then checks, not a claim the probe has
	// to confirm before the record may exist.
	query, args, err := sq.Insert(zonesTable).
		Columns("slug", "display_name", "status", "ingest_url", "query_url").
		Values(req.Slug, req.DisplayName, string(structs.ZoneStatusActive), ingestURL, queryURL).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	result, err := engine.Exec(query, args...)
	if err != nil {
		return nil, fmt.Errorf("create zone %q: %w", req.Slug, err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("last insert id: %w", err)
	}
	return GetZone(engine, id)
}

// UpdateZoneRequest is a partial update of the fields an operator may edit.
//
// TWO ABSENCES ARE THE ENFORCEMENT, not oversights:
//
//   - There is no Slug field, so there is no code path that can edit a slug and
//     nothing has to remember not to. A slug is chosen once and is never handed
//     to a different owner.
//   - There is no Status field either, and that one is newer. Retirement now
//     goes through RetireZone, which refuses while the zone still has active
//     projects. Leaving a settable Status here would be a second way to retire
//     that skips the guard entirely — and it is the way a handler reaches for
//     first, because it is the generic one. A guard with a bypass beside it is
//     not a guard.
//
// There is deliberately no DeleteZone anywhere: removing the row would free the
// slug for reuse, and a reused slug silently reattaches the previous owner's
// surviving events (30-day TTL) and its permanent daily rollup (no TTL) to the
// new one.
type UpdateZoneRequest struct {
	DisplayName *string
	IngestURL   *string
	QueryURL    *string
}

// UpdateZone applies a partial update and returns the refreshed row.
//
// A nil field is "leave it alone"; a present-but-blank one is refused rather
// than treated as a clear. Clearing an endpoint would move the zone back to
// 'unconfigured' — a state that exists only for the row that predates the
// columns — and the operator who blanked a field by accident would get a zone
// that reads as never-configured rather than as broken.
func UpdateZone(engine db.Queryable, id int64, req UpdateZoneRequest) (*structs.Zone, error) {
	q := sq.Update(zonesTable).Where(sq.Eq{"id": id})

	hasUpdate := false
	if req.DisplayName != nil {
		name := strings.TrimSpace(*req.DisplayName)
		if name == "" {
			return nil, fmt.Errorf("display_name cannot be blank")
		}
		q = q.Set("display_name", name)
		hasUpdate = true
	}
	if req.IngestURL != nil {
		url, err := tools.NormalizeEndpointURL(*req.IngestURL)
		if err != nil {
			return nil, fmt.Errorf("invalid ingest_url: %w", err)
		}
		q = q.Set("ingest_url", url)
		hasUpdate = true
	}
	if req.QueryURL != nil {
		url, err := tools.NormalizeEndpointURL(*req.QueryURL)
		if err != nil {
			return nil, fmt.Errorf("invalid query_url: %w", err)
		}
		q = q.Set("query_url", url)
		hasUpdate = true
	}

	if !hasUpdate {
		return GetZone(engine, id)
	}

	query, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	if _, err := engine.Exec(query, args...); err != nil {
		return nil, fmt.Errorf("update zone: %w", err)
	}
	return GetZone(engine, id)
}

// CountActiveProjects returns how many live tenants a zone still owns.
//
// Used for the retirement refusal message and by the admin surface to say WHY a
// zone cannot be retired. It is NOT what enforces the rule — that predicate is
// inside RetireZone's UPDATE, for the reason spelled out there.
func CountActiveProjects(engine db.Queryable, zoneID int64) (int, error) {
	query, args, err := sq.Select("COUNT(*)").From(projectsTable).
		Where(sq.Eq{"zone_id": zoneID}).
		Where(sq.Eq{"status": string(structs.ProjectStatusActive)}).
		ToSql()
	if err != nil {
		return 0, fmt.Errorf("build query: %w", err)
	}

	var count int
	if err := engine.QueryRow(query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count active projects in zone %d: %w", zoneID, err)
	}
	return count, nil
}

// RetireZone moves a zone to status='deleted' and returns the refreshed row.
//
// THIS IS THE ONLY RETIREMENT PATH, and it is an UPDATE. There is no DELETE
// here or anywhere else in the registry: the row is kept forever so the UNIQUE
// key on slug makes reuse structurally impossible. A recycled slug would
// reattach up to a month of the previous owner's events (30-day TTL) plus a
// permanent daily rollup (no TTL) to the new one, with every reference still
// syntactically valid — nothing errors, nothing logs, and the only symptom is a
// dashboard quietly reporting someone else's numbers.
//
// ⚠️ THE ACTIVE-PROJECT GUARD IS IN THE STATEMENT, NOT IN THE CALLER, and that
// is the whole design of this function. Counting first and then updating is two
// round trips with a gap between them: a project created in that gap lands in a
// zone that is on its way to retired, and the result is a tenant whose events
// keep arriving while the zone that would list it is hidden from every switcher —
// live data nobody can reach and nobody can see. The NOT EXISTS makes the check
// and the write one atomic operation, so there is no gap to lose.
//
// The count below runs only to EXPLAIN a refusal that has already happened. It
// is not the enforcement, and a future edit that "simplifies" by hoisting it
// above the UPDATE has removed the guard while leaving the error message intact —
// which is the worst possible shape for this to fail in.
func RetireZone(engine db.Queryable, id int64) (*structs.Zone, error) {
	active := string(structs.ZoneStatusActive)

	query, args, err := sq.Update(zonesTable).
		Set("status", string(structs.ZoneStatusDeleted)).
		Where(sq.Eq{"id": id}).
		Where(sq.Eq{"status": active}).
		Where("NOT EXISTS (SELECT 1 FROM "+projectsTable+" WHERE "+projectsTable+".zone_id = "+zonesTable+".id AND "+projectsTable+".status = ?)",
			string(structs.ProjectStatusActive)).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	result, err := engine.Exec(query, args...)
	if err != nil {
		return nil, fmt.Errorf("retire zone %d: %w", id, err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("rows affected: %w", err)
	}
	if affected == 0 {
		return nil, explainRetireZoneRefusal(engine, id)
	}

	return GetZone(engine, id)
}

// explainRetireZoneRefusal turns "the UPDATE matched nothing" into the reason.
//
// Three states produce zero affected rows and they need three different answers:
// the zone does not exist (404), it was already retired (a no-op an operator can
// safely ignore), or it still owns live tenants (409, and the count is the
// actionable part). Reporting one message for all three would send an operator
// hunting for projects on a zone that simply is not there.
func explainRetireZoneRefusal(engine db.Queryable, id int64) error {
	zone, err := GetZone(engine, id)
	if err != nil {
		return fmt.Errorf("retire zone %d: %w", id, err)
	}
	if zone == nil {
		return fmt.Errorf("retire zone %d: %w", id, ErrZoneNotFound)
	}
	if zone.Status != structs.ZoneStatusActive {
		return fmt.Errorf("retire zone %q: %w", zone.Slug, ErrZoneAlreadyRetired)
	}

	count, err := CountActiveProjects(engine, id)
	if err != nil {
		return fmt.Errorf("retire zone %q: %w", zone.Slug, err)
	}
	return fmt.Errorf("retire zone %q: %w (%d active) — retire them first, or a live tenant is left with no zone to list it",
		zone.Slug, ErrZoneHasActiveProjects, count)
}

// RecordZoneProbeRequest is one probe's verdict, as it is persisted.
type RecordZoneProbeRequest struct {
	Reachability structs.ZoneReachability
	Detail       string
	// ReportedZone is what the remote box claimed to be — untrusted text, kept
	// verbatim (truncated to fit) rather than normalised, because the whole value
	// of the field is that it is what was actually said.
	ReportedZone string
	ProbedAt     time.Time
}

// RecordZoneProbe stores the last reachability verdict for a zone.
//
// Split from UpdateZone rather than folded into it, for the same reason
// UpdateZoneRequest has no Status field: these columns are written by the
// SYSTEM, from a measurement, and the ones UpdateZone writes are edited by a
// PERSON, from intent. One function that could do both would be one function a
// handler could use to fake a health verdict, and one an operator could use to
// clobber a probe result by saving a display name.
//
// ⚠️ `updated_at` IS EXPLICITLY HELD. The column carries ON UPDATE
// CURRENT_TIMESTAMP, so without this line every probe would restamp it and
// "when was this registry row last changed" would permanently read as "seconds
// ago" on every zone — destroying the only signal an operator has for spotting a
// row somebody edited. Assigning a column its own value is how MariaDB is told
// not to fire the automatic update; there is no flag for it.
func RecordZoneProbe(engine db.Queryable, id int64, req RecordZoneProbeRequest) error {
	if !req.Reachability.IsValid() {
		return fmt.Errorf("invalid zone reachability %q", req.Reachability)
	}
	if req.ProbedAt.IsZero() {
		req.ProbedAt = time.Now().UTC()
	}

	reported := req.ReportedZone
	if len(reported) > REPORTED_ZONE_MAX_LENGTH {
		reported = reported[:REPORTED_ZONE_MAX_LENGTH]
	}
	detail := req.Detail
	if len(detail) > 500 {
		detail = detail[:500]
	}

	query, args, err := sq.Update(zonesTable).
		Set("reachability", string(req.Reachability)).
		Set("reachability_detail", detail).
		Set("reported_zone", reported).
		Set("last_probe_at", req.ProbedAt.UTC()).
		Set("updated_at", sq.Expr("updated_at")).
		Where(sq.Eq{"id": id}).
		ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}

	if _, err := engine.Exec(query, args...); err != nil {
		return fmt.Errorf("record zone probe for %d: %w", id, err)
	}
	return nil
}
