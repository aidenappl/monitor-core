package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/google/uuid"
)

type ServiceGroup struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Services    string    `json:"services"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func InitServiceGroups(ctx context.Context) error {
	err := db.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+db.Database+`.service_groups (
			id String,
			name String,
			description String DEFAULT '',
			services String DEFAULT '[]',
			created_at DateTime64(3, 'UTC') DEFAULT now64(3),
			updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
		) ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (id)
	`)
	if err != nil {
		return fmt.Errorf("failed to create service_groups table: %w", err)
	}
	return nil
}

func CreateServiceGroup(ctx context.Context, sg ServiceGroup) (*ServiceGroup, error) {
	if sg.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	sg.ID = uuid.New().String()
	now := time.Now().UTC()
	sg.CreatedAt = now
	sg.UpdatedAt = now

	if sg.Services == "" {
		sg.Services = "[]"
	}

	err := db.Conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.service_groups (id, name, description, services, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
		db.Database,
	), sg.ID, sg.Name, sg.Description, sg.Services, sg.CreatedAt, sg.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to insert service group: %w", err)
	}

	return &sg, nil
}

func ListServiceGroups(ctx context.Context) ([]ServiceGroup, error) {
	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT id, name, description, services, created_at, updated_at FROM %s.service_groups FINAL ORDER BY name ASC",
		db.Database,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to list service groups: %w", err)
	}
	defer rows.Close()

	var groups []ServiceGroup
	for rows.Next() {
		var sg ServiceGroup
		if err := rows.Scan(&sg.ID, &sg.Name, &sg.Description, &sg.Services, &sg.CreatedAt, &sg.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan service group: %w", err)
		}
		groups = append(groups, sg)
	}
	return groups, nil
}

func GetServiceGroup(ctx context.Context, id string) (*ServiceGroup, error) {
	row := db.Conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT id, name, description, services, created_at, updated_at FROM %s.service_groups FINAL WHERE id = ?",
		db.Database,
	), id)

	var sg ServiceGroup
	if err := row.Scan(&sg.ID, &sg.Name, &sg.Description, &sg.Services, &sg.CreatedAt, &sg.UpdatedAt); err != nil {
		return nil, fmt.Errorf("service group not found")
	}
	return &sg, nil
}

func UpdateServiceGroup(ctx context.Context, id string, sg ServiceGroup) (*ServiceGroup, error) {
	existing, err := GetServiceGroup(ctx, id)
	if err != nil {
		return nil, err
	}

	if sg.Name != "" {
		existing.Name = sg.Name
	}
	if sg.Description != "" {
		existing.Description = sg.Description
	}
	if sg.Services != "" {
		existing.Services = sg.Services
	}

	existing.UpdatedAt = time.Now().UTC()

	err = db.Conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.service_groups (id, name, description, services, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
		db.Database,
	), existing.ID, existing.Name, existing.Description, existing.Services, existing.CreatedAt, existing.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to update service group: %w", err)
	}

	return existing, nil
}

func DeleteServiceGroup(ctx context.Context, id string) error {
	err := db.Conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.service_groups DELETE WHERE id = ?", db.Database,
	), id)
	if err != nil {
		return fmt.Errorf("failed to delete service group: %w", err)
	}
	return nil
}

func ResolveServiceGroups(ctx context.Context, service string) ([]string, error) {
	groups, err := ListServiceGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve service groups: %w", err)
	}

	var matchingIDs []string
	for _, sg := range groups {
		var services []string
		if err := json.Unmarshal([]byte(sg.Services), &services); err != nil {
			continue
		}
		for _, s := range services {
			if s == service {
				matchingIDs = append(matchingIDs, sg.ID)
				break
			}
		}
	}
	return matchingIDs, nil
}
