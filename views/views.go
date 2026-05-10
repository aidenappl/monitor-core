package views

import (
	"context"
	"fmt"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/google/uuid"
)

// View represents a saved query/filter view
type View struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	QueryParams string    `json:"query_params"`
	Page        string    `json:"page"`
	CreatedAt   time.Time `json:"created_at"`
}

// Init creates the saved_views table if it doesn't exist
func Init(ctx context.Context) error {
	return db.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+db.Database+`.saved_views (
			id String,
			name String,
			query_params String,
			page String DEFAULT 'events',
			created_at DateTime64(3, 'UTC') DEFAULT now64(3)
		) ENGINE = MergeTree
		ORDER BY (id)
	`)
}

// Create creates a new saved view
func Create(ctx context.Context, name, queryParams, page string) (*View, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if page == "" {
		page = "events"
	}

	id := uuid.New().String()
	now := time.Now().UTC()

	err := db.Conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.saved_views (id, name, query_params, page, created_at) VALUES (?, ?, ?, ?, ?)",
		db.Database,
	), id, name, queryParams, page, now)
	if err != nil {
		return nil, fmt.Errorf("failed to insert view: %w", err)
	}

	return &View{
		ID:          id,
		Name:        name,
		QueryParams: queryParams,
		Page:        page,
		CreatedAt:   now,
	}, nil
}

// List returns all saved views, optionally filtered by page
func List(ctx context.Context, page string) ([]View, error) {
	var query string
	var args []interface{}

	if page != "" {
		query = fmt.Sprintf(
			"SELECT id, name, query_params, page, created_at FROM %s.saved_views WHERE page = ? ORDER BY created_at DESC",
			db.Database,
		)
		args = append(args, page)
	} else {
		query = fmt.Sprintf(
			"SELECT id, name, query_params, page, created_at FROM %s.saved_views ORDER BY created_at DESC",
			db.Database,
		)
	}

	rows, err := db.Conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list views: %w", err)
	}
	defer rows.Close()

	var views []View
	for rows.Next() {
		var v View
		if err := rows.Scan(&v.ID, &v.Name, &v.QueryParams, &v.Page, &v.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan view: %w", err)
		}
		views = append(views, v)
	}
	return views, nil
}

// Delete removes a saved view by ID
func Delete(ctx context.Context, id string) error {
	err := db.Conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.saved_views DELETE WHERE id = ?", db.Database,
	), id)
	if err != nil {
		return fmt.Errorf("failed to delete view: %w", err)
	}
	return nil
}
