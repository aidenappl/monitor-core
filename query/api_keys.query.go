package query

import (
	"database/sql"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

// apiKeyColumns is table-qualified because every read joins projects to resolve
// the slug. Unqualified `id` would be ambiguous the moment the join is present,
// and MariaDB reports that as a query error rather than picking one — a failure
// that would take down authentication wholesale, since refreshCache runs this.
var apiKeyColumns = []string{
	"api_keys.id", "api_keys.name", "api_keys.key_hash", "api_keys.key_prefix",
	"api_keys.scope", "api_keys.project_id", "projects.slug",
	"api_keys.created_at", "api_keys.last_used_at",
}

// apiKeysWithProject is the FROM clause every read shares.
//
// INNER JOIN, not LEFT. project_id is NOT NULL with a foreign key to projects
// (migration 117), so the join cannot drop a row — and if a future schema change
// ever broke that, an inner join makes the key vanish from the cache and stop
// authenticating, whereas a left join would keep it working with an empty
// project slug and stamp every one of its events with a blank tenant. Failing
// closed on a binding that should be impossible is worth more than a resilience
// that quietly mis-files data.
//
// ⚠️ ZONES IS JOINED BECAUSE A PROJECT SLUG IS NOT UNIQUE. The constraint is
// `uq_projects_zone_slug (zone_id, slug)` (migration 116) — `default` names one
// project PER ZONE, and the control plane's projects table carries rows for
// every zone it knows about. A predicate on projects.slug alone therefore names
// a SET, not a row. Every scoped read and the delete below pair it with
// zones.slug, which together are unique by that constraint.
const apiKeysWithProject = "api_keys " +
	"JOIN projects ON projects.id = api_keys.project_id " +
	"JOIN zones ON zones.id = projects.zone_id"

type apiKeyScanner interface {
	Scan(dest ...interface{}) error
}

func scanAPIKey(row apiKeyScanner) (*structs.APIKey, error) {
	var k structs.APIKey
	var scope string
	if err := row.Scan(&k.ID, &k.Name, &k.KeyHash, &k.KeyPrefix, &scope,
		&k.ProjectID, &k.ProjectSlug, &k.CreatedAt, &k.LastUsedAt); err != nil {
		return nil, err
	}
	k.Scope = structs.Scope(scope)
	return &k, nil
}

// ListAllAPIKeys returns every key across every project.
//
// This exists for exactly ONE caller: the apikeys cache refresh. Validation must
// resolve a presented key whatever project it belongs to — the cache is the
// authentication path, not a view — so scoping it would make keys outside the
// refresher's notion of "current project" silently stop authenticating, and the
// refresher has no request and therefore no project at all.
//
// Every other reader wants ListAPIKeys. If you are adding a second caller here,
// that is the signal you want the scoped one.
func ListAllAPIKeys(engine db.Queryable) ([]structs.APIKey, error) {
	return listAPIKeys(engine, "", nil)
}

// ListAPIKeys returns the keys belonging to one project.
//
// Scoped rather than global because the page that renders it carries a project
// selector: showing every project's keys under a control that says "atlas" is
// the same class of lie as showing every project's events would be. A caller
// wanting the whole inventory switches project, exactly as it would to see
// another project's issues.
func ListAPIKeys(engine db.Queryable, zoneSlug, projectSlug string) ([]structs.APIKey, error) {
	return listAPIKeys(engine, zoneSlug, &projectSlug)
}

// listAPIKeys is the one builder both readers share. A nil projectSlug means
// every project — expressible only here, never from outside the package, so the
// unscoped read cannot be reached by passing an empty string by accident.
func listAPIKeys(engine db.Queryable, zoneSlug string, projectSlug *string) ([]structs.APIKey, error) {
	q := sq.Select(apiKeyColumns...).From(apiKeysWithProject)
	// The zone is applied for the SCOPED read only. The unscoped one is the
	// auth cache, which loads whatever this process's own database holds — and
	// that database IS this zone's. Narrowing it by a slug read from env would
	// make every credential in the install stop authenticating the moment
	// MON_ZONE_SLUG were mistyped, which is a far worse failure than the one it
	// would prevent, and one apikeys.Create already makes impossible by
	// resolving zone-then-project before it binds.
	if projectSlug != nil {
		q = q.Where(sq.Eq{"zones.slug": zoneSlug, "projects.slug": *projectSlug})
	}

	query, args, err := q.
		OrderBy("api_keys.created_at DESC").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := engine.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("list api keys: %w", err)
	}
	defer rows.Close()

	keys := []structs.APIKey{}
	for rows.Next() {
		k, err := scanAPIKey(rows)
		if err != nil {
			return nil, fmt.Errorf("scan api key: %w", err)
		}
		keys = append(keys, *k)
	}
	return keys, rows.Err()
}

// GetAPIKeyByID returns one key, scoped to a project.
//
// The project is part of the LOOKUP, not a check applied after it: a key
// belonging to another tenant must read as absent, so a caller cannot learn that
// an id exists — or act on it — outside its own project.
func GetAPIKeyByID(engine db.Queryable, zoneSlug, projectSlug, id string) (*structs.APIKey, error) {
	query, args, err := sq.Select(apiKeyColumns...).
		From(apiKeysWithProject).
		Where(sq.Eq{
			"api_keys.id":   id,
			"zones.slug":    zoneSlug,
			"projects.slug": projectSlug,
		}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	k, err := scanAPIKey(engine.QueryRow(query, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get api key: %w", err)
	}
	return k, nil
}

// CreateAPIKeyRequest is a fully-resolved row. ProjectID is an id, not a slug:
// resolving the slug is the caller's job (apikeys.Create), so this layer never
// has to guess which zone a bare slug belonged to.
type CreateAPIKeyRequest struct {
	ID        string
	Name      string
	KeyHash   string
	KeyPrefix string
	Scope     structs.Scope
	ProjectID int64
	CreatedAt time.Time
}

// CreateAPIKey inserts a new key row. The raw key/prefix/hash are generated by
// the caller (apikeys package).
//
// ProjectID is required and is not defaulted here. A zero would fail
// fk_api_keys_project anyway, but it would fail as errno 1452 naming a
// constraint rather than as a sentence naming the missing field, and this is the
// layer that knows which field it was.
func CreateAPIKey(engine db.Queryable, req CreateAPIKeyRequest) error {
	if req.ProjectID <= 0 {
		return fmt.Errorf("project_id is required")
	}

	query, args, err := sq.Insert("api_keys").
		Columns("id", "name", "key_hash", "key_prefix", "scope", "project_id", "created_at").
		Values(req.ID, req.Name, req.KeyHash, req.KeyPrefix, string(req.Scope), req.ProjectID, req.CreatedAt).
		ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}

	if _, err := engine.Exec(query, args...); err != nil {
		return fmt.Errorf("create api key: %w", err)
	}
	return nil
}

// DeleteAPIKey removes one key, scoped to a project.
//
// The project is in the DELETE's own WHERE rather than trusted from the read
// that preceded it. A mutation has to be defended independently of the handler
// that called it: the read-then-write pair is not atomic, and a future caller
// that skips the read — or reorders it — would otherwise delete across tenants
// with nothing in this function to stop it. Same reasoning the issues upsert
// records for its own scoping.
//
// The join is required because the predicate names projects.slug; api_keys
// carries only project_id.
func DeleteAPIKey(engine db.Queryable, zoneSlug, projectSlug, id string) error {
	query, args, err := sq.Delete("api_keys").
		Where(sq.Expr(
			"id = ? AND project_id = ("+
				"SELECT p.id FROM projects p "+
				"JOIN zones z ON z.id = p.zone_id "+
				"WHERE z.slug = ? AND p.slug = ?)",
			id, zoneSlug, projectSlug,
		)).ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}

	if _, err := engine.Exec(query, args...); err != nil {
		return fmt.Errorf("delete api key: %w", err)
	}
	return nil
}
