package dashboards

import (
	"context"
	"fmt"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/google/uuid"
)

// Dashboard represents a saved dashboard configuration
type Dashboard struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Config      string    `json:"config"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Init creates the dashboards table if it doesn't exist
func Init(ctx context.Context) error {
	return db.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+db.Database+`.dashboards (
			id String,
			name String,
			description String DEFAULT '',
			config String,
			created_at DateTime64(3, 'UTC') DEFAULT now64(3),
			updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
		) ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (id)
	`)
}

// Create creates a new dashboard
func Create(ctx context.Context, name, description, config string) (*Dashboard, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}

	id := uuid.New().String()
	now := time.Now().UTC()

	err := db.Conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.dashboards (id, name, description, config, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		db.Database,
	), id, name, description, config, now, now)
	if err != nil {
		return nil, fmt.Errorf("failed to insert dashboard: %w", err)
	}

	return &Dashboard{
		ID:          id,
		Name:        name,
		Description: description,
		Config:      config,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

// List returns all dashboards
func List(ctx context.Context) ([]Dashboard, error) {
	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT id, name, description, config, created_at, updated_at FROM %s.dashboards FINAL ORDER BY created_at DESC",
		db.Database,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to list dashboards: %w", err)
	}
	defer rows.Close()

	var dashboards []Dashboard
	for rows.Next() {
		var d Dashboard
		if err := rows.Scan(&d.ID, &d.Name, &d.Description, &d.Config, &d.CreatedAt, &d.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan dashboard: %w", err)
		}
		dashboards = append(dashboards, d)
	}
	return dashboards, nil
}

// Get returns a dashboard by ID
func Get(ctx context.Context, id string) (*Dashboard, error) {
	row := db.Conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT id, name, description, config, created_at, updated_at FROM %s.dashboards FINAL WHERE id = ?",
		db.Database,
	), id)

	var d Dashboard
	if err := row.Scan(&d.ID, &d.Name, &d.Description, &d.Config, &d.CreatedAt, &d.UpdatedAt); err != nil {
		return nil, fmt.Errorf("dashboard not found")
	}
	return &d, nil
}

// Update updates an existing dashboard
func Update(ctx context.Context, id, name, description, config string) (*Dashboard, error) {
	// Verify it exists
	existing, err := Get(ctx, id)
	if err != nil {
		return nil, err
	}

	if name == "" {
		name = existing.Name
	}
	if description == "" {
		description = existing.Description
	}
	if config == "" {
		config = existing.Config
	}

	now := time.Now().UTC()

	err = db.Conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.dashboards (id, name, description, config, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		db.Database,
	), id, name, description, config, existing.CreatedAt, now)
	if err != nil {
		return nil, fmt.Errorf("failed to update dashboard: %w", err)
	}

	return &Dashboard{
		ID:          id,
		Name:        name,
		Description: description,
		Config:      config,
		CreatedAt:   existing.CreatedAt,
		UpdatedAt:   now,
	}, nil
}

// Delete removes a dashboard by ID
func Delete(ctx context.Context, id string) error {
	err := db.Conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.dashboards DELETE WHERE id = ?", db.Database,
	), id)
	if err != nil {
		return fmt.Errorf("failed to delete dashboard: %w", err)
	}
	return nil
}
