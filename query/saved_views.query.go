package query

import (
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
	"monitor.saved_views.name",
	"monitor.saved_views.query_params",
	"monitor.saved_views.page",
	"monitor.saved_views.created_at",
}

type savedViewScanner interface {
	Scan(dest ...interface{}) error
}

func scanSavedView(row savedViewScanner) (*structs.SavedView, error) {
	var v structs.SavedView
	if err := row.Scan(&v.ID, &v.Name, &v.QueryParams, &v.Page, &v.CreatedAt); err != nil {
		return nil, err
	}
	return &v, nil
}

// CreateSavedViewRequest is the POST /v1/views body.
type CreateSavedViewRequest struct {
	Name        string `json:"name"`
	QueryParams string `json:"query_params"`
	Page        string `json:"page"`
}

// CreateSavedView inserts one saved filter.
//
// `query_params` is an opaque client-owned blob and is not validated, for the
// reason CreateDashboard gives about `config`.
func CreateSavedView(engine db.Queryable, req CreateSavedViewRequest) (*structs.SavedView, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	v := structs.SavedView{
		ID:          uuid.New().String(),
		Name:        req.Name,
		QueryParams: req.QueryParams,
		Page:        req.Page,
		CreatedAt:   time.Now().UTC(),
	}
	if v.Page == "" {
		v.Page = "events"
	}

	qStr, args, err := sq.Insert(savedViewsTable).
		Columns("id", "name", "query_params", "page", "created_at").
		Values(v.ID, v.Name, v.QueryParams, v.Page, v.CreatedAt).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to insert saved view: %w", err)
	}
	return &v, nil
}

// ListSavedViews returns saved views, newest first, optionally filtered to one
// page. An empty page means every page — the contract the handler has always had,
// where `?page=` absent lists everything.
func ListSavedViews(engine db.Queryable, page string) ([]structs.SavedView, error) {
	q := sq.Select(savedViewColumns...).From(savedViewsTable).
		OrderBy("monitor.saved_views.created_at DESC", "monitor.saved_views.id ASC")
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

// DeleteSavedView removes one saved view and reports whether it existed.
//
// This is the delete that used to come back. The ClickHouse table was a plain
// MergeTree, so `ALTER TABLE … DELETE` was an asynchronous mutation and a list
// issued straight afterwards could still return the row. Here the row is gone
// when the statement returns.
func DeleteSavedView(engine db.Queryable, id string) (bool, error) {
	qStr, args, err := sq.Delete(savedViewsTable).Where(sq.Eq{"id": id}).ToSql()
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
