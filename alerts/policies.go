package alerts

import (
	"context"
	"fmt"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/google/uuid"
)

type PolicyMatchers struct {
	Priority     string   `json:"priority,omitempty"`
	Services     []string `json:"services,omitempty"`
	ServiceGroup string   `json:"service_group,omitempty"`
	Status       string   `json:"status,omitempty"`
	Env          string   `json:"env,omitempty"`
	RuleName     string   `json:"rule_name,omitempty"`
}

type NotificationPolicy struct {
	ID                    string    `json:"id"`
	Name                  string    `json:"name"`
	Description           string    `json:"description"`
	Position              uint32    `json:"position"`
	Matchers              string    `json:"matchers"`
	ChannelIDs            string    `json:"channel_ids"`
	ContinueMatching      bool      `json:"continue_matching"`
	RepeatIntervalSeconds uint32    `json:"repeat_interval_seconds"`
	Enabled               bool      `json:"enabled"`
	IsDefault             bool      `json:"is_default"`
	CreatedAt             time.Time `json:"created_at"`
	UpdatedAt             time.Time `json:"updated_at"`
}

func InitPolicies(ctx context.Context) error {
	err := db.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+db.Database+`.notification_policies (
			id String,
			name String,
			description String DEFAULT '',
			position UInt32,
			matchers String DEFAULT '{}',
			channel_ids String DEFAULT '[]',
			continue_matching UInt8 DEFAULT 0,
			repeat_interval_seconds UInt32 DEFAULT 0,
			enabled UInt8 DEFAULT 1,
			is_default UInt8 DEFAULT 0,
			created_at DateTime64(3, 'UTC') DEFAULT now64(3),
			updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
		) ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (id)
	`)
	if err != nil {
		return fmt.Errorf("failed to create notification_policies table: %w", err)
	}

	return seedDefaultPolicies(ctx)
}

func seedDefaultPolicies(ctx context.Context) error {
	var count uint64
	row := db.Conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT count() FROM %s.notification_policies FINAL WHERE is_default = 1",
		db.Database,
	))
	if err := row.Scan(&count); err != nil {
		return fmt.Errorf("failed to check default policies: %w", err)
	}
	if count > 0 {
		return nil
	}

	now := time.Now().UTC()

	defaults := []NotificationPolicy{
		{
			ID:         uuid.New().String(),
			Name:       "Critical (P0) — All Channels",
			Position:   1,
			Matchers:   `{"priority":"P0"}`,
			ChannelIDs: "[]",
			IsDefault:  true,
			Enabled:    true,
			CreatedAt:  now,
			UpdatedAt:  now,
		},
		{
			ID:         uuid.New().String(),
			Name:       "High (P1) — PagerDuty + Email",
			Position:   2,
			Matchers:   `{"priority":"P1"}`,
			ChannelIDs: "[]",
			IsDefault:  true,
			Enabled:    true,
			CreatedAt:  now,
			UpdatedAt:  now,
		},
		{
			ID:         uuid.New().String(),
			Name:       "Medium (P2) — Email Only",
			Position:   3,
			Matchers:   `{"priority":"P2"}`,
			ChannelIDs: "[]",
			IsDefault:  true,
			Enabled:    true,
			CreatedAt:  now,
			UpdatedAt:  now,
		},
		{
			ID:         uuid.New().String(),
			Name:       "Low (P3) — Web Only",
			Position:   4,
			Matchers:   `{"priority":"P3"}`,
			ChannelIDs: "[]",
			IsDefault:  true,
			Enabled:    true,
			CreatedAt:  now,
			UpdatedAt:  now,
		},
	}

	for _, p := range defaults {
		enabled := uint8(1)
		isDefault := uint8(1)

		err := db.Conn.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s.notification_policies (id, name, description, position, matchers, channel_ids, continue_matching, repeat_interval_seconds, enabled, is_default, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			db.Database,
		), p.ID, p.Name, p.Description, p.Position, p.Matchers, p.ChannelIDs, uint8(0), uint32(0), enabled, isDefault, p.CreatedAt, p.UpdatedAt)
		if err != nil {
			return fmt.Errorf("failed to seed default policy %q: %w", p.Name, err)
		}
	}

	return nil
}

func CreatePolicy(ctx context.Context, p NotificationPolicy) (*NotificationPolicy, error) {
	if p.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	p.ID = uuid.New().String()
	now := time.Now().UTC()
	p.CreatedAt = now
	p.UpdatedAt = now

	pos, err := getNextPosition(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to determine position: %w", err)
	}
	p.Position = pos

	if p.Matchers == "" {
		p.Matchers = "{}"
	}
	if p.ChannelIDs == "" {
		p.ChannelIDs = "[]"
	}

	enabled := uint8(0)
	if p.Enabled {
		enabled = 1
	}
	continueMatching := uint8(0)
	if p.ContinueMatching {
		continueMatching = 1
	}
	isDefault := uint8(0)
	if p.IsDefault {
		isDefault = 1
	}

	err = db.Conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.notification_policies (id, name, description, position, matchers, channel_ids, continue_matching, repeat_interval_seconds, enabled, is_default, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		db.Database,
	), p.ID, p.Name, p.Description, p.Position, p.Matchers, p.ChannelIDs, continueMatching, p.RepeatIntervalSeconds, enabled, isDefault, p.CreatedAt, p.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to insert notification policy: %w", err)
	}

	return &p, nil
}

func ListPolicies(ctx context.Context) ([]NotificationPolicy, error) {
	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT id, name, description, position, matchers, channel_ids, continue_matching, repeat_interval_seconds, enabled, is_default, created_at, updated_at FROM %s.notification_policies FINAL ORDER BY position ASC",
		db.Database,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to list notification policies: %w", err)
	}
	defer rows.Close()

	var policies []NotificationPolicy
	for rows.Next() {
		var p NotificationPolicy
		var enabled, isDefault, continueMatching uint8
		if err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.Position, &p.Matchers, &p.ChannelIDs, &continueMatching, &p.RepeatIntervalSeconds, &enabled, &isDefault, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan notification policy: %w", err)
		}
		p.Enabled = enabled == 1
		p.IsDefault = isDefault == 1
		p.ContinueMatching = continueMatching == 1
		policies = append(policies, p)
	}
	return policies, nil
}

func GetPolicy(ctx context.Context, id string) (*NotificationPolicy, error) {
	row := db.Conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT id, name, description, position, matchers, channel_ids, continue_matching, repeat_interval_seconds, enabled, is_default, created_at, updated_at FROM %s.notification_policies FINAL WHERE id = ?",
		db.Database,
	), id)

	var p NotificationPolicy
	var enabled, isDefault, continueMatching uint8
	if err := row.Scan(&p.ID, &p.Name, &p.Description, &p.Position, &p.Matchers, &p.ChannelIDs, &continueMatching, &p.RepeatIntervalSeconds, &enabled, &isDefault, &p.CreatedAt, &p.UpdatedAt); err != nil {
		return nil, fmt.Errorf("notification policy not found")
	}
	p.Enabled = enabled == 1
	p.IsDefault = isDefault == 1
	p.ContinueMatching = continueMatching == 1
	return &p, nil
}

func UpdatePolicy(ctx context.Context, id string, p NotificationPolicy) (*NotificationPolicy, error) {
	existing, err := GetPolicy(ctx, id)
	if err != nil {
		return nil, err
	}

	if p.Name != "" {
		existing.Name = p.Name
	}
	if p.Description != "" {
		existing.Description = p.Description
	}
	if p.Matchers != "" {
		existing.Matchers = p.Matchers
	}
	if p.ChannelIDs != "" {
		existing.ChannelIDs = p.ChannelIDs
	}
	if p.RepeatIntervalSeconds != 0 {
		existing.RepeatIntervalSeconds = p.RepeatIntervalSeconds
	}
	existing.ContinueMatching = p.ContinueMatching
	existing.Enabled = p.Enabled

	now := time.Now().UTC()
	existing.UpdatedAt = now

	enabled := uint8(0)
	if existing.Enabled {
		enabled = 1
	}
	continueMatching := uint8(0)
	if existing.ContinueMatching {
		continueMatching = 1
	}
	isDefault := uint8(0)
	if existing.IsDefault {
		isDefault = 1
	}

	err = db.Conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.notification_policies (id, name, description, position, matchers, channel_ids, continue_matching, repeat_interval_seconds, enabled, is_default, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		db.Database,
	), existing.ID, existing.Name, existing.Description, existing.Position, existing.Matchers, existing.ChannelIDs, continueMatching, existing.RepeatIntervalSeconds, enabled, isDefault, existing.CreatedAt, existing.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to update notification policy: %w", err)
	}

	return existing, nil
}

func DeletePolicy(ctx context.Context, id string) error {
	existing, err := GetPolicy(ctx, id)
	if err != nil {
		return err
	}
	if existing.IsDefault {
		return fmt.Errorf("cannot delete default policy")
	}

	err = db.Conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.notification_policies DELETE WHERE id = ?", db.Database,
	), id)
	if err != nil {
		return fmt.Errorf("failed to delete notification policy: %w", err)
	}
	return nil
}

func ReorderPolicies(ctx context.Context, orderedIDs []string) error {
	for i, id := range orderedIDs {
		existing, err := GetPolicy(ctx, id)
		if err != nil {
			return fmt.Errorf("policy %s not found: %w", id, err)
		}

		existing.Position = uint32(i + 1)
		existing.UpdatedAt = time.Now().UTC()

		enabled := uint8(0)
		if existing.Enabled {
			enabled = 1
		}
		continueMatching := uint8(0)
		if existing.ContinueMatching {
			continueMatching = 1
		}
		isDefault := uint8(0)
		if existing.IsDefault {
			isDefault = 1
		}

		err = db.Conn.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s.notification_policies (id, name, description, position, matchers, channel_ids, continue_matching, repeat_interval_seconds, enabled, is_default, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			db.Database,
		), existing.ID, existing.Name, existing.Description, existing.Position, existing.Matchers, existing.ChannelIDs, continueMatching, existing.RepeatIntervalSeconds, enabled, isDefault, existing.CreatedAt, existing.UpdatedAt)
		if err != nil {
			return fmt.Errorf("failed to reorder policy %s: %w", id, err)
		}
	}
	return nil
}

func getNextPosition(ctx context.Context) (uint32, error) {
	var maxPos uint32
	row := db.Conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT max(position) FROM %s.notification_policies FINAL",
		db.Database,
	))
	if err := row.Scan(&maxPos); err != nil {
		return 1, nil
	}
	return maxPos + 1, nil
}
