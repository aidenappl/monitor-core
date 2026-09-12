package query

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/google/uuid"
)

const dashboardsTable = "monitor.dashboards"

var dashboardColumns = []string{
	"monitor.dashboards.id",
	"monitor.dashboards.project",
	"monitor.dashboards.name",
	"monitor.dashboards.description",
	"monitor.dashboards.config",
	"monitor.dashboards.created_at",
	"monitor.dashboards.updated_at",
}

// ErrNoDashboardProject is returned by every read and write here that was handed
// an empty project. It is the counterpart of ErrNoIssueProject and exists for
// the same reason: "I do not know whose dashboards these are" must be an error
// the caller has to handle, never a query that quietly returns all of them.
var ErrNoDashboardProject = errors.New("no project supplied for a dashboard read")

// scopeDashboards attaches the tenancy predicate to a read of monitor.dashboards.
//
// Named for its table rather than written as one generic helper, mirroring
// scopeIssues: a helper that took the table as a string would build a predicate
// on monitor.dashboards for a saved-views query the moment someone copied a call
// site, and MariaDB reports that as "unknown column" only if the tables differ —
// which is exactly the case where the mistake is invisible on a JOIN.
//
// WHY THE PREDICATE IS NEEDED AT ALL, given a dashboard's panels were already
// scoped. The panels resolve through services/analytics.go, so another project's
// dashboard rendered EMPTY rather than leaking data — but its name and
// description are the leak: "Payments p99 — Acme migration" tells a reader who
// the other tenants are and what they are working on, out of the list endpoint
// the dashboards page is built on.
func scopeDashboards(q sq.SelectBuilder, project string) (sq.SelectBuilder, error) {
	if project == "" {
		return q, ErrNoDashboardProject
	}
	return q.Where(sq.Eq{"monitor.dashboards.project": project}), nil
}

type dashboardScanner interface {
	Scan(dest ...interface{}) error
}

func scanDashboard(row dashboardScanner) (*structs.Dashboard, error) {
	var d structs.Dashboard
	if err := row.Scan(&d.ID, &d.Project, &d.Name, &d.Description, &d.Config, &d.CreatedAt, &d.UpdatedAt); err != nil {
		return nil, err
	}
	return &d, nil
}

// CreateDashboardRequest is the POST /v1/dashboards body.
//
// The project is NOT in it. It is resolved from the request's credential by the
// handler and passed alongside, so a caller cannot file a dashboard into another
// tenant by adding a field to its JSON.
type CreateDashboardRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Config      string `json:"config"`
}

// CreateDashboard inserts one saved layout into a project.
//
// The project is refused here rather than left to the database. `project` is NOT
// NULL with no default (migration 131), so an omitted value fails as errno 1364
// naming a column — this layer knows it was the tenant and says so, and the
// check runs before a row can be written that no project could ever see.
//
// `config` is NOT validated as JSON, unlike every JSON-shaped field in the alert
// tables. It is an opaque client-owned blob stored in a LONGTEXT — migration 123
// argues the split — and the handler has never required it, so an empty string
// is a legitimate stored value here.
func CreateDashboard(engine db.Queryable, project string, req CreateDashboardRequest) (*structs.Dashboard, error) {
	if project == "" {
		return nil, ErrNoDashboardProject
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	now := time.Now().UTC()
	d := structs.Dashboard{
		ID:          uuid.New().String(),
		Project:     project,
		Name:        req.Name,
		Description: req.Description,
		Config:      req.Config,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	qStr, args, err := sq.Insert(dashboardsTable).
		Columns("id", "project", "name", "description", "config", "created_at", "updated_at").
		Values(d.ID, d.Project, d.Name, d.Description, d.Config, d.CreatedAt, d.UpdatedAt).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to insert dashboard: %w", err)
	}
	return &d, nil
}

// ListDashboards returns one project's dashboards, newest first.
func ListDashboards(engine db.Queryable, project string) ([]structs.Dashboard, error) {
	q := sq.Select(dashboardColumns...).From(dashboardsTable).
		OrderBy("monitor.dashboards.created_at DESC", "monitor.dashboards.id ASC")
	q, err := scopeDashboards(q, project)
	if err != nil {
		return nil, err
	}

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	rows, err := engine.Query(qStr, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql query: %w", err)
	}
	defer rows.Close()

	dashboards := []structs.Dashboard{}
	for rows.Next() {
		d, err := scanDashboard(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan dashboard: %w", err)
		}
		dashboards = append(dashboards, *d)
	}
	return dashboards, rows.Err()
}

// GetDashboard returns one dashboard by id, or (nil, nil) when this project has
// none with that id.
//
// The project is part of the LOOKUP, not a check applied after it: a dashboard
// belonging to another tenant reads as ABSENT, so the 404 a foreign id produces
// is indistinguishable from the 404 a nonexistent one produces. 123 declined a
// unique key on name and that stands, so an id is the only handle — and ids
// travel, into monitor-web URLs and shared links.
func GetDashboard(engine db.Queryable, project, id string) (*structs.Dashboard, error) {
	q := sq.Select(dashboardColumns...).From(dashboardsTable).
		Where(sq.Eq{"monitor.dashboards.id": id}).Limit(1)
	q, err := scopeDashboards(q, project)
	if err != nil {
		return nil, err
	}

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	d, err := scanDashboard(engine.QueryRow(qStr, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan dashboard: %w", err)
	}
	return d, nil
}

// UpdateDashboardRequest is the PUT /v1/dashboards/{id} body.
//
// "Non-empty wins", preserved from the ClickHouse implementation: a dashboard's
// description cannot be cleared through this endpoint. See
// UpdateServiceGroupRequest for why the pointer treatment is not applied here.
//
// There is no Project field, and adding one would be a tenant MOVE dressed as an
// edit. A dashboard changes project by being recreated in the other one.
type UpdateDashboardRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Config      string `json:"config"`
}

// UpdateDashboard applies the request to one of a project's dashboards and
// returns it.
func UpdateDashboard(engine db.Queryable, project, id string, req UpdateDashboardRequest) (*structs.Dashboard, error) {
	if project == "" {
		return nil, ErrNoDashboardProject
	}

	existing, err := GetDashboard(engine, project, id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("dashboard not found")
	}

	u := sq.Update(dashboardsTable)
	changed := false
	if req.Name != "" {
		u = u.Set("name", req.Name)
		changed = true
	}
	if req.Description != "" {
		u = u.Set("description", req.Description)
		changed = true
	}
	if req.Config != "" {
		u = u.Set("config", req.Config)
		changed = true
	}
	if !changed {
		return existing, nil
	}

	// The project is in the UPDATE's OWN where clause rather than trusted from
	// the read above. The read-then-write pair is not atomic, and a future
	// caller that skips the read — or reorders it — would otherwise rewrite
	// another tenant's dashboard with nothing in this function to stop it. Same
	// reasoning DeleteAPIKey records for its own scoping.
	qStr, args, err := u.Where(sq.Eq{"id": id, "project": project}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to update dashboard: %w", err)
	}
	return GetDashboard(engine, project, id)
}

// DeleteDashboard removes one of a project's dashboards and reports whether it
// existed.
//
// The project is in the DELETE's own WHERE for the reason UpdateDashboard gives:
// a mutation must be defended independently of whatever read preceded it.
func DeleteDashboard(engine db.Queryable, project, id string) (bool, error) {
	if project == "" {
		return false, ErrNoDashboardProject
	}

	qStr, args, err := sq.Delete(dashboardsTable).
		Where(sq.Eq{"id": id, "project": project}).ToSql()
	if err != nil {
		return false, fmt.Errorf("failed to build sql query: %w", err)
	}

	res, err := engine.Exec(qStr, args...)
	if err != nil {
		return false, fmt.Errorf("failed to delete dashboard: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, nil
	}
	return affected > 0, nil
}
