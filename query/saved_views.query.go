package query

import (
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/google/uuid"
)

const savedViewsTable = "monitor.saved_views"

var savedViewColumns = []string{
	"monitor.saved_views.id",
	"monitor.saved_views.project",
	"monitor.saved_views.name",
	"monitor.saved_views.query_params",
	"monitor.saved_views.page",
	"monitor.saved_views.created_at",
}

// ErrNoSavedViewProject is returned by every read and write here that was handed
// an empty project, for the reason ErrNoIssueProject documents.
var ErrNoSavedViewProject = errors.New("no project supplied for a saved view read")

// scopeSavedViews attaches the tenancy predicate to a read of
// monitor.saved_views. Table-specific rather than generic, mirroring scopeIssues.
//
// A saved view is a stored query string, so the leak this closes is sharper than
// a name: `query_params` carries another tenant's service names, paths, hosts and
// search terms — the filters someone built while debugging — served out of the
// same list the views dropdown is populated from.
//
// The predicate leads with the project because migration 132 reindexed the table
// to (project, page): the page filter below is now a suffix of that key rather
// than an index of its own.
func scopeSavedViews(q sq.SelectBuilder, project string) (sq.SelectBuilder, error) {
	if project == "" {
		return q, ErrNoSavedViewProject
	}
	return q.Where(sq.Eq{"monitor.saved_views.project": project}), nil
}

type savedViewScanner interface {
	Scan(dest ...interface{}) error
}

func scanSavedView(row savedViewScanner) (*structs.SavedView, error) {
	var v structs.SavedView
	if err := row.Scan(&v.ID, &v.Project, &v.Name, &v.QueryParams, &v.Page, &v.CreatedAt); err != nil {
		return nil, err
	}
	return &v, nil
}

// CreateSavedViewRequest is the POST /v1/views body.
//
// No project field: it comes from the credential, not from the body, so a caller
// cannot file a view into a tenant it cannot read.
type CreateSavedViewRequest struct {
	Name        string `json:"name"`
	QueryParams string `json:"query_params"`
	Page        string `json:"page"`
}

// CreateSavedView inserts one saved filter into a project.
//
// The empty project is refused before the INSERT: `project` is NOT NULL with no
// default (migration 132), so the database would reject it as errno 1364 naming
// a column, and the caller deserves to be told it was the tenant that was
// missing.
//
// `query_params` is an opaque client-owned blob and is not validated, for the
// reason CreateDashboard gives about `config`.
func CreateSavedView(engine db.Queryable, project string, req CreateSavedViewRequest) (*structs.SavedView, error) {
	if project == "" {
		return nil, ErrNoSavedViewProject
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	v := structs.SavedView{
		ID:          uuid.New().String(),
		Project:     project,
		Name:        req.Name,
		QueryParams: req.QueryParams,
		Page:        req.Page,
		CreatedAt:   time.Now().UTC(),
	}
	if v.Page == "" {
		v.Page = "events"
	}

	qStr, args, err := sq.Insert(savedViewsTable).
		Columns("id", "project", "name", "query_params", "page", "created_at").
		Values(v.ID, v.Project, v.Name, v.QueryParams, v.Page, v.CreatedAt).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to insert saved view: %w", err)
	}
	return &v, nil
}

// ListSavedViews returns one project's saved views, newest first, optionally
// filtered to one page.
//
// The two parameters are NOT the same kind of thing and must not be confused: an
// empty page means every page — the contract the handler has always had, where
// `?page=` absent lists everything — while an empty project is an error. A
// filter the caller may omit sits next to a tenancy the caller may not.
func ListSavedViews(engine db.Queryable, project, page string) ([]structs.SavedView, error) {
	q := sq.Select(savedViewColumns...).From(savedViewsTable).
		OrderBy("monitor.saved_views.created_at DESC", "monitor.saved_views.id ASC")
	q, err := scopeSavedViews(q, project)
	if err != nil {
		return nil, err
	}
	if page != "" {
		q = q.Where(sq.Eq{"monitor.saved_views.page": page})
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

	views := []structs.SavedView{}
	for rows.Next() {
		v, err := scanSavedView(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan saved view: %w", err)
		}
		views = append(views, *v)
	}
	return views, rows.Err()
}

// DeleteSavedView removes one of a project's saved views and reports whether it
// existed.
//
// The project is in the DELETE's own WHERE. There is no read in front of this
// one to borrow a tenant from — the handler deletes by id alone — so if the
// predicate were not here, an id lifted from another project's response would
// delete that project's view and report success.
//
// This is the delete that used to come back. The ClickHouse table was a plain
// MergeTree, so `ALTER TABLE … DELETE` was an asynchronous mutation and a list
// issued straight afterwards could still return the row. Here the row is gone
// when the statement returns.
func DeleteSavedView(engine db.Queryable, project, id string) (bool, error) {
	if project == "" {
		return false, ErrNoSavedViewProject
	}

	qStr, args, err := sq.Delete(savedViewsTable).
		Where(sq.Eq{"id": id, "project": project}).ToSql()
	if err != nil {
		return false, fmt.Errorf("failed to build sql query: %w", err)
	}

	res, err := engine.Exec(qStr, args...)
	if err != nil {
		return false, fmt.Errorf("failed to delete saved view: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, nil
	}
	return affected > 0, nil
}
