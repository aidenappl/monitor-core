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

// projectsTable is unqualified for the same reason zonesTable is — the registry
// lives in the DSN's default database (monitor_auth).
const projectsTable = "projects"

var projectColumns = []string{
	"id", "zone_id", "slug", "display_name", "status", "created_at", "updated_at",
}

type projectScanner interface {
	Scan(dest ...interface{}) error
}

func scanProject(row projectScanner) (*structs.Project, error) {
	var p structs.Project
	var status string

	if err := row.Scan(&p.ID, &p.ZoneID, &p.Slug, &p.DisplayName, &status, &p.CreatedAt, &p.UpdatedAt); err != nil {
		return nil, err
	}
	p.Status = structs.ProjectStatus(status)
	return &p, nil
}

// GetProject resolves a project by primary key. Returns (nil, nil) when absent.
func GetProject(engine db.Queryable, id int64) (*structs.Project, error) {
	query, args, err := sq.Select(projectColumns...).From(projectsTable).
		Where(sq.Eq{"id": id}).Limit(1).ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	p, err := scanProject(engine.QueryRow(query, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get project: %w", err)
	}
	return p, nil
}

// GetProjectBySlug resolves a project within one zone. Returns (nil, nil) when
// absent.
//
// The zone id is a required argument rather than an optional filter because a
// project slug is only unique inside its zone. A lookup by slug alone would
// match whichever zone's row happened to sort first — correct today, when there
// is one zone, and silently wrong the moment there are two. Like GetZoneBySlug,
// this returns retired rows too: a spent slug must never read as free.
func GetProjectBySlug(engine db.Queryable, zoneID int64, slug string) (*structs.Project, error) {
	query, args, err := sq.Select(projectColumns...).From(projectsTable).
		Where(sq.Eq{"zone_id": zoneID, "slug": slug}).Limit(1).ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	p, err := scanProject(engine.QueryRow(query, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get project by slug: %w", err)
	}
	return p, nil
}

// ListProjectsRequest filters the project list within a zone.
type ListProjectsRequest struct {
	// IncludeDeleted returns retired projects as well. Off by default, for the
	// same reason as ListZonesRequest.
	IncludeDeleted bool
	Limit          int
	Offset         int
}

// ListProjects returns the projects in one zone, ordered by slug.
func ListProjects(engine db.Queryable, zoneID int64, req ListProjectsRequest) ([]structs.Project, error) {
	if req.Limit <= 0 || req.Limit > db.MAX_LIMIT {
		req.Limit = db.DEFAULT_LIMIT
	}

	q := sq.Select(projectColumns...).From(projectsTable).
		Where(sq.Eq{"zone_id": zoneID})
	if !req.IncludeDeleted {
		q = q.Where(sq.Eq{"status": string(structs.ProjectStatusActive)})
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
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()

	projects := []structs.Project{}
	for rows.Next() {
		p, err := scanProject(rows)
		if err != nil {
			return nil, fmt.Errorf("scan project: %w", err)
		}
		projects = append(projects, *p)
	}
	return projects, rows.Err()
}

// CreateProjectRequest mints a new project inside a zone.
type CreateProjectRequest struct {
	ZoneID      int64
	Slug        string
	DisplayName string
}

// CreateProject validates the slug and inserts the project, returning the
// hydrated row. Same strictness and same single whitespace trim as CreateZone —
// this is the last moment the name can be changed.
//
// The zone's existence is left to the fk_projects_zone foreign key rather than
// pre-checked with a SELECT. A pre-check is a round trip that still races
// against a concurrent write, so it would buy a nicer error message and no
// actual guarantee; the error is wrapped with the zone id instead so errno 1452
// is still readable.
func CreateProject(engine db.Queryable, req CreateProjectRequest) (*structs.Project, error) {
	req.Slug = strings.TrimSpace(req.Slug)
	req.DisplayName = strings.TrimSpace(req.DisplayName)

	if req.ZoneID <= 0 {
		return nil, fmt.Errorf("zone_id is required")
	}
	if err := tools.ValidateSlug(req.Slug); err != nil {
		return nil, fmt.Errorf("invalid project slug: %w", err)
	}
	if req.DisplayName == "" {
		return nil, fmt.Errorf("display_name is required")
	}

	query, args, err := sq.Insert(projectsTable).
		Columns("zone_id", "slug", "display_name", "status").
		Values(req.ZoneID, req.Slug, req.DisplayName, string(structs.ProjectStatusActive)).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	result, err := engine.Exec(query, args...)
	if err != nil {
		return nil, fmt.Errorf("create project %q in zone %d: %w", req.Slug, req.ZoneID, err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("last insert id: %w", err)
	}
	return GetProject(engine, id)
}

// UpdateProjectRequest is a partial update. As with UpdateZoneRequest, there is
// no Slug field and no ZoneID field: the slug is immutable, and moving a project
// between zones would re-point every event already filed under it. Retire with
// Status = ProjectStatusDeleted; there is no DeleteProject.
type UpdateProjectRequest struct {
	DisplayName *string
	Status      *structs.ProjectStatus
}

// UpdateProject applies a partial update and returns the refreshed row.
func UpdateProject(engine db.Queryable, id int64, req UpdateProjectRequest) (*structs.Project, error) {
	q := sq.Update(projectsTable).Where(sq.Eq{"id": id})

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
			return nil, fmt.Errorf("invalid project status %q", *req.Status)
		}
		q = q.Set("status", string(*req.Status))
		hasUpdate = true
	}

	if !hasUpdate {
		return GetProject(engine, id)
	}

	query, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	if _, err := engine.Exec(query, args...); err != nil {
		return nil, fmt.Errorf("update project: %w", err)
	}
	return GetProject(engine, id)
}
