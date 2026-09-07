package query

import (
	"database/sql"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/tools"
)

// zonesTable is UNQUALIFIED, unlike issuesTable. The registry lives in the DSN's
// default database (monitor_auth) alongside users and api_keys, not in the
// `monitor` schema — see db/migrations/116_create_registry.sql for why.
const zonesTable = "zones"

var zoneColumns = []string{
	"id", "slug", "display_name", "status", "created_at", "updated_at",
}

type zoneScanner interface {
	Scan(dest ...interface{}) error
}

func scanZone(row zoneScanner) (*structs.Zone, error) {
	var z structs.Zone
	var status string

	if err := row.Scan(&z.ID, &z.Slug, &z.DisplayName, &status, &z.CreatedAt, &z.UpdatedAt); err != nil {
		return nil, err
	}
	z.Status = structs.ZoneStatus(status)
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
type CreateZoneRequest struct {
	Slug        string
	DisplayName string
}

// CreateZone validates the slug and inserts the zone, returning the hydrated row.
//
// This is the ONLY moment the name is negotiable — it is immutable afterwards
// and never reusable — so validation is strict here and nowhere else. The
// surrounding whitespace trim is the single normalization allowed: env vars and
// pasted values collect it, and it is the one correction that cannot change
// which name the operator meant. Case is NOT folded; `Payments` is refused, not
// silently rewritten to `payments` and then frozen that way forever.
func CreateZone(engine db.Queryable, req CreateZoneRequest) (*structs.Zone, error) {
	req.Slug = strings.TrimSpace(req.Slug)
	req.DisplayName = strings.TrimSpace(req.DisplayName)

	if err := tools.ValidateSlug(req.Slug); err != nil {
		return nil, fmt.Errorf("invalid zone slug: %w", err)
	}
	if req.DisplayName == "" {
		return nil, fmt.Errorf("display_name is required")
	}

	query, args, err := sq.Insert(zonesTable).
		Columns("slug", "display_name", "status").
		Values(req.Slug, req.DisplayName, string(structs.ZoneStatusActive)).
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

// UpdateZoneRequest is a partial update. The absence of a Slug field is the
// enforcement of immutability, not an oversight — there is no code path that can
// edit a slug, so nothing has to remember not to.
//
// Setting Status to ZoneStatusDeleted is how a zone is retired. There is
// deliberately no DeleteZone: removing the row would free the slug for reuse,
// and a reused slug silently reattaches the previous owner's surviving events
// and its permanent daily rollup to the new one.
type UpdateZoneRequest struct {
	DisplayName *string
	Status      *structs.ZoneStatus
}

// UpdateZone applies a partial update and returns the refreshed row.
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
	if req.Status != nil {
		if !req.Status.IsValid() {
			return nil, fmt.Errorf("invalid zone status %q", *req.Status)
		}
		q = q.Set("status", string(*req.Status))
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
