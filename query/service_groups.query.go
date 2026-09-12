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

const serviceGroupsTable = "monitor.service_groups"

// serviceGroupsProjectColumn is the one string every scoped read of this table
// binds against. Named once because it appears in three builders.
const serviceGroupsProjectColumn = "monitor.service_groups.project"

var serviceGroupColumns = []string{
	"monitor.service_groups.id",
	"monitor.service_groups.project",
	"monitor.service_groups.name",
	"monitor.service_groups.description",
	"monitor.service_groups.services",
	"monitor.service_groups.created_at",
	"monitor.service_groups.updated_at",
}

type serviceGroupScanner interface {
	Scan(dest ...interface{}) error
}

func scanServiceGroup(row serviceGroupScanner) (*structs.ServiceGroup, error) {
	var sg structs.ServiceGroup
	if err := row.Scan(&sg.ID, &sg.Project, &sg.Name, &sg.Description, &sg.Services, &sg.CreatedAt, &sg.UpdatedAt); err != nil {
		return nil, err
	}
	return &sg, nil
}

// CreateServiceGroupRequest is the POST /v1/service-groups body.
type CreateServiceGroupRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Services    string `json:"services"`
}

// CreateServiceGroup inserts one named set of services into a project.
//
// The project is a leading parameter rather than a request field, for the reason
// CreateAlertRule gives — and it matters more here than it looks: the group's
// MEMBERS are service names, which are unique only within one project's event
// stream, so a group filed into the wrong tenant would resolve its members
// against the wrong traffic.
func CreateServiceGroup(engine db.Queryable, project string, req CreateServiceGroupRequest) (*structs.ServiceGroup, error) {
	// Checked first because the column is NOT NULL with no default (migration
	// 130): an empty project reaches MariaDB as errno 1364 naming the column,
	// where this names the request that had no tenant.
	if project == "" {
		return nil, ErrNoAlertingProject
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	now := time.Now().UTC()
	sg := structs.ServiceGroup{
		ID:          uuid.New().String(),
		Project:     project,
		Name:        req.Name,
		Description: req.Description,
		Services:    req.Services,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if sg.Services == "" {
		sg.Services = "[]"
	}
	if err := requireJSONText("services", sg.Services); err != nil {
		return nil, err
	}

	qStr, args, err := sq.Insert(serviceGroupsTable).
		Columns("id", "project", "name", "description", "services", "created_at", "updated_at").
		Values(sg.ID, sg.Project, sg.Name, sg.Description, sg.Services, sg.CreatedAt, sg.UpdatedAt).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to insert service group: %w", err)
	}
	return &sg, nil
}

// ListServiceGroups returns one project's groups, by name.
//
// alerts.ResolveServiceGroups calls this on EVERY routed alert and then filters
// in Go, because the membership test is "does this JSON array contain this
// service" and the array is a blob. A `JSON_CONTAINS(services, …)` predicate
// would push that into SQL, but it cannot use an index either — MariaDB has no
// indexes on JSON array elements — so it would trade a readable Go loop over a
// handful of rows for a full scan expressed in SQL. Left as it is; revisit if
// groups ever number in the thousands.
func ListServiceGroups(engine db.Queryable, project string) ([]structs.ServiceGroup, error) {
	q := sq.Select(serviceGroupColumns...).From(serviceGroupsTable).
		OrderBy("monitor.service_groups.name ASC")

	q, err := scopeAlerting(q, serviceGroupsProjectColumn, project)
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

	groups := []structs.ServiceGroup{}
	for rows.Next() {
		sg, err := scanServiceGroup(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan service group: %w", err)
		}
		groups = append(groups, *sg)
	}
	return groups, rows.Err()
}

// GetServiceGroup returns one group by id within a project, or (nil, nil) when
// there is none. The project is part of the lookup, so a group belonging to
// another tenant reads as ABSENT rather than as forbidden.
func GetServiceGroup(engine db.Queryable, project, id string) (*structs.ServiceGroup, error) {
	q := sq.Select(serviceGroupColumns...).From(serviceGroupsTable).
		Where(sq.Eq{"monitor.service_groups.id": id}).Limit(1)

	q, err := scopeAlerting(q, serviceGroupsProjectColumn, project)
	if err != nil {
		return nil, err
	}

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	sg, err := scanServiceGroup(engine.QueryRow(qStr, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan service group: %w", err)
	}
	return sg, nil
}

// UpdateServiceGroupRequest is the PUT /v1/service-groups/{id} body.
//
// "Non-empty wins", preserved verbatim from the ClickHouse implementation: a
// group's member list cannot be emptied through this endpoint, because an empty
// `services` is indistinguishable from an omitted one. The pointer treatment
// UpdateAlertRuleRequest uses is the fix, and it is a behaviour change that
// belongs in its own commit rather than inside a store move.
type UpdateServiceGroupRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Services    string `json:"services"`
}

// UpdateServiceGroup applies the request to one group within a project and
// returns it.
func UpdateServiceGroup(engine db.Queryable, project, id string, req UpdateServiceGroupRequest) (*structs.ServiceGroup, error) {
	existing, err := GetServiceGroup(engine, project, id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("service group not found")
	}

	u := sq.Update(serviceGroupsTable)
	changed := false
	if req.Name != "" {
		u = u.Set("name", req.Name)
		changed = true
	}
	if req.Description != "" {
		u = u.Set("description", req.Description)
		changed = true
	}
	if req.Services != "" {
		if err := requireJSONText("services", req.Services); err != nil {
			return nil, err
		}
		u = u.Set("services", req.Services)
		changed = true
	}
	// squirrel refuses to build an UPDATE with no SET clause, and a request that
	// names nothing is a no-op rather than an error — the ClickHouse version
	// rewrote the row unchanged and returned it.
	if !changed {
		return existing, nil
	}

	// `project` is in the UPDATE's own WHERE rather than trusted from the read
	// above, for the reason DeleteAPIKey records: read-then-write is not atomic
	// and a future caller may skip the read.
	qStr, args, err := u.Where(sq.Eq{"id": id, "project": project}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to update service group: %w", err)
	}
	return GetServiceGroup(engine, project, id)
}

// DeleteServiceGroup removes one group and reports whether it existed.
//
// A policy whose matchers name this group is left pointing at nothing, and
// matchPolicy then simply never matches it — the same dangling-reference
// situation DeleteNotificationChannel describes, for the same reason: the
// reference lives inside a JSON blob, so there is no foreign key to enforce.
// The project is in the DELETE's own WHERE, with no read in front of it.
func DeleteServiceGroup(engine db.Queryable, project, id string) (bool, error) {
	if project == "" {
		return false, ErrNoAlertingProject
	}

	qStr, args, err := sq.Delete(serviceGroupsTable).
		Where(sq.Eq{"id": id, "project": project}).ToSql()
	if err != nil {
		return false, fmt.Errorf("failed to build sql query: %w", err)
	}

	res, err := engine.Exec(qStr, args...)
	if err != nil {
		return false, fmt.Errorf("failed to delete service group: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, nil
	}
	return affected > 0, nil
}
