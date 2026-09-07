package query

import (
	"database/sql"
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
	"monitor.dashboards.name",
	"monitor.dashboards.description",
	"monitor.dashboards.config",
	"monitor.dashboards.created_at",
	"monitor.dashboards.updated_at",
}

type dashboardScanner interface {
	Scan(dest ...interface{}) error
}

func scanDashboard(row dashboardScanner) (*structs.Dashboard, error) {
	var d structs.Dashboard
	if err := row.Scan(&d.ID, &d.Name, &d.Description, &d.Config, &d.CreatedAt, &d.UpdatedAt); err != nil {
		return nil, err
	}
	return &d, nil
}

// CreateDashboardRequest is the POST /v1/dashboards body.
type CreateDashboardRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Config      string `json:"config"`
}

// CreateDashboard inserts one saved layout.
//
// `config` is NOT validated as JSON, unlike every JSON-shaped field in the alert
// tables. It is an opaque client-owned blob stored in a LONGTEXT — migration 123
// argues the split — and the handler has never required it, so an empty string
// is a legitimate stored value here.
func CreateDashboard(engine db.Queryable, req CreateDashboardRequest) (*structs.Dashboard, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	now := time.Now().UTC()
	d := structs.Dashboard{
		ID:          uuid.New().String(),
		Name:        req.Name,
		Description: req.Description,
		Config:      req.Config,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	qStr, args, err := sq.Insert(dashboardsTable).
		Columns("id", "name", "description", "config", "created_at", "updated_at").
		Values(d.ID, d.Name, d.Description, d.Config, d.CreatedAt, d.UpdatedAt).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to insert dashboard: %w", err)
	}
	return &d, nil
}

// ListDashboards returns every dashboard, newest first.
func ListDashboards(engine db.Queryable) ([]structs.Dashboard, error) {
	q := sq.Select(dashboardColumns...).From(dashboardsTable).
		OrderBy("monitor.dashboards.created_at DESC", "monitor.dashboards.id ASC")

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

// GetDashboard returns one dashboard by id, or (nil, nil) when there is none.
func GetDashboard(engine db.Queryable, id string) (*structs.Dashboard, error) {
	q := sq.Select(dashboardColumns...).From(dashboardsTable).
		Where(sq.Eq{"monitor.dashboards.id": id}).Limit(1)

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
type UpdateDashboardRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Config      string `json:"config"`
}

// UpdateDashboard applies the request to one dashboard and returns it.
func UpdateDashboard(engine db.Queryable, id string, req UpdateDashboardRequest) (*structs.Dashboard, error) {
	existing, err := GetDashboard(engine, id)
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

	qStr, args, err := u.Where(sq.Eq{"id": id}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to update dashboard: %w", err)
	}
	return GetDashboard(engine, id)
}

// DeleteDashboard removes one dashboard and reports whether it existed.
func DeleteDashboard(engine db.Queryable, id string) (bool, error) {
	qStr, args, err := sq.Delete(dashboardsTable).Where(sq.Eq{"id": id}).ToSql()
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
