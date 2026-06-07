package alerts

import (
	"context"
	"fmt"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/google/uuid"
)

// Priority levels
const (
	PriorityCritical = "P0"
	PriorityHigh     = "P1"
	PriorityMedium   = "P2"
	PriorityLow      = "P3"
)

var ValidPriorities = map[string]bool{
	PriorityCritical: true,
	PriorityHigh:     true,
	PriorityMedium:   true,
	PriorityLow:      true,
}

// Rule represents an alert rule
type Rule struct {
	ID                     string    `json:"id"`
	Name                   string    `json:"name"`
	Description            string    `json:"description"`
	Type                   string    `json:"type"`
	Priority               string    `json:"priority"`
	QueryFilters           string    `json:"query_filters"`
	Metric                 string    `json:"metric"`
	Field                  string    `json:"field"`
	Condition              string    `json:"condition"`
	Threshold              float64   `json:"threshold"`
	EvaluationIntervalSecs uint32    `json:"evaluation_interval_seconds"`
	ForSeconds             uint32    `json:"for_seconds"`
	CooldownSeconds        uint32    `json:"cooldown_seconds"`
	NotificationChannelIDs string    `json:"notification_channel_ids"`
	Enabled                bool      `json:"enabled"`
	CreatedAt              time.Time `json:"created_at"`
	UpdatedAt              time.Time `json:"updated_at"`
}

// RuleWithState combines a rule with its current state
type RuleWithState struct {
	Rule
	State *State `json:"state,omitempty"`
}

// State represents the current state of an alert rule
type State struct {
	RuleID         string     `json:"rule_id"`
	Status         string     `json:"status"`
	Value          float64    `json:"value"`
	FiredAt        *time.Time `json:"fired_at,omitempty"`
	ResolvedAt     *time.Time `json:"resolved_at,omitempty"`
	LastNotifiedAt *time.Time `json:"last_notified_at,omitempty"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// HistoryEntry represents an alert history entry
type HistoryEntry struct {
	ID        string    `json:"id"`
	RuleID    string    `json:"rule_id"`
	RuleName  string    `json:"rule_name"`
	Status    string    `json:"status"`
	Value     float64   `json:"value"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

// Channel represents a notification channel
type Channel struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	Config    string    `json:"config"`
	CreatedAt time.Time `json:"created_at"`
}

// Init creates all alert-related tables
func Init(ctx context.Context) error {
	err := db.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+db.Database+`.alert_rules (
			id String,
			name String,
			description String DEFAULT '',
			type String,
			priority String DEFAULT 'P2',
			query_filters String,
			metric String DEFAULT 'count',
			field String DEFAULT '',
			condition String,
			threshold Float64,
			evaluation_interval_seconds UInt32 DEFAULT 60,
			for_seconds UInt32 DEFAULT 0,
			cooldown_seconds UInt32 DEFAULT 300,
			notification_channel_ids String DEFAULT '[]',
			enabled UInt8 DEFAULT 1,
			created_at DateTime64(3, 'UTC') DEFAULT now64(3),
			updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
		) ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (id)
	`)
	if err != nil {
		return fmt.Errorf("failed to create alert_rules table: %w", err)
	}

	// Migration: add priority column for existing tables
	_ = db.Conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.alert_rules ADD COLUMN IF NOT EXISTS priority String DEFAULT 'P2'",
		db.Database,
	))

	err = db.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+db.Database+`.alert_states (
			rule_id String,
			status String DEFAULT 'ok',
			value Float64 DEFAULT 0,
			fired_at Nullable(DateTime64(3, 'UTC')),
			resolved_at Nullable(DateTime64(3, 'UTC')),
			last_notified_at Nullable(DateTime64(3, 'UTC')),
			updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
		) ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (rule_id)
	`)
	if err != nil {
		return fmt.Errorf("failed to create alert_states table: %w", err)
	}

	err = db.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+db.Database+`.alert_history (
			id String,
			rule_id String,
			rule_name String,
			status String,
			value Float64,
			message String DEFAULT '',
			created_at DateTime64(3, 'UTC') DEFAULT now64(3)
		) ENGINE = MergeTree
		ORDER BY (created_at, rule_id)
		TTL toDate(created_at) + INTERVAL 90 DAY
	`)
	if err != nil {
		return fmt.Errorf("failed to create alert_history table: %w", err)
	}

	err = db.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+db.Database+`.notification_channels (
			id String,
			name String,
			type String,
			config String,
			created_at DateTime64(3, 'UTC') DEFAULT now64(3)
		) ENGINE = MergeTree
		ORDER BY (id)
	`)
	if err != nil {
		return fmt.Errorf("failed to create notification_channels table: %w", err)
	}

	if err := InitServiceGroups(ctx); err != nil {
		return fmt.Errorf("failed to initialize service_groups: %w", err)
	}

	if err := InitPolicies(ctx); err != nil {
		return fmt.Errorf("failed to initialize notification_policies: %w", err)
	}

	return nil
}

// CreateRule creates a new alert rule
func CreateRule(ctx context.Context, rule Rule) (*Rule, error) {
	if rule.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if rule.Type == "" {
		return nil, fmt.Errorf("type is required")
	}
	validTypes := map[string]bool{"threshold": true, "absence": true, "rate_change": true}
	if !validTypes[rule.Type] {
		return nil, fmt.Errorf("invalid type: %s (must be threshold, absence, or rate_change)", rule.Type)
	}
	if rule.Condition == "" {
		return nil, fmt.Errorf("condition is required")
	}
	validConditions := map[string]bool{"gt": true, "lt": true, "gte": true, "lte": true, "eq": true}
	if !validConditions[rule.Condition] {
		return nil, fmt.Errorf("invalid condition: %s", rule.Condition)
	}

	rule.ID = uuid.New().String()
	now := time.Now().UTC()
	rule.CreatedAt = now
	rule.UpdatedAt = now

	if rule.Priority == "" || !ValidPriorities[rule.Priority] {
		rule.Priority = PriorityMedium
	}
	if rule.Metric == "" {
		rule.Metric = "count"
	}
	if rule.EvaluationIntervalSecs == 0 {
		rule.EvaluationIntervalSecs = 60
	}
	if rule.CooldownSeconds == 0 {
		rule.CooldownSeconds = 300
	}
	if rule.QueryFilters == "" {
		rule.QueryFilters = "[]"
	}
	if rule.NotificationChannelIDs == "" {
		rule.NotificationChannelIDs = "[]"
	}

	enabled := uint8(0)
	if rule.Enabled {
		enabled = 1
	}

	err := db.Conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.alert_rules (id, name, description, type, priority, query_filters, metric, field, condition, threshold, evaluation_interval_seconds, for_seconds, cooldown_seconds, notification_channel_ids, enabled, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		db.Database,
	), rule.ID, rule.Name, rule.Description, rule.Type, rule.Priority, rule.QueryFilters, rule.Metric, rule.Field, rule.Condition, rule.Threshold, rule.EvaluationIntervalSecs, rule.ForSeconds, rule.CooldownSeconds, rule.NotificationChannelIDs, enabled, rule.CreatedAt, rule.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to insert alert rule: %w", err)
	}

	return &rule, nil
}

// ListRules returns all alert rules with their current state
func ListRules(ctx context.Context) ([]RuleWithState, error) {
	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT id, name, description, type, priority, query_filters, metric, field, condition, threshold, evaluation_interval_seconds, for_seconds, cooldown_seconds, notification_channel_ids, enabled, created_at, updated_at FROM %s.alert_rules FINAL ORDER BY created_at DESC",
		db.Database,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to list alert rules: %w", err)
	}
	defer rows.Close()

	var ruleList []Rule
	for rows.Next() {
		var r Rule
		var enabled uint8
		if err := rows.Scan(&r.ID, &r.Name, &r.Description, &r.Type, &r.Priority, &r.QueryFilters, &r.Metric, &r.Field, &r.Condition, &r.Threshold, &r.EvaluationIntervalSecs, &r.ForSeconds, &r.CooldownSeconds, &r.NotificationChannelIDs, &enabled, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan alert rule: %w", err)
		}
		r.Enabled = enabled == 1
		ruleList = append(ruleList, r)
	}

	// Bulk fetch all states in one query instead of N+1
	stateMap, err := ListAllStates(ctx)
	if err != nil {
		// Non-fatal: attach nil states if bulk fetch fails
		stateMap = make(map[string]*State)
	}

	var rules []RuleWithState
	for _, r := range ruleList {
		rules = append(rules, RuleWithState{Rule: r, State: stateMap[r.ID]})
	}
	return rules, nil
}

// ListAllStates returns all alert states as a map keyed by rule_id
func ListAllStates(ctx context.Context) (map[string]*State, error) {
	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT rule_id, status, value, fired_at, resolved_at, last_notified_at, updated_at FROM %s.alert_states FINAL",
		db.Database,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to list alert states: %w", err)
	}
	defer rows.Close()

	stateMap := make(map[string]*State)
	for rows.Next() {
		var s State
		if err := rows.Scan(&s.RuleID, &s.Status, &s.Value, &s.FiredAt, &s.ResolvedAt, &s.LastNotifiedAt, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan alert state: %w", err)
		}
		stateMap[s.RuleID] = &s
	}
	return stateMap, nil
}

// GetRule returns an alert rule by ID with its current state
func GetRule(ctx context.Context, id string) (*RuleWithState, error) {
	rule, err := getRuleOnly(ctx, id)
	if err != nil {
		return nil, err
	}
	state, _ := GetState(ctx, id)
	return &RuleWithState{Rule: *rule, State: state}, nil
}

func getRuleOnly(ctx context.Context, id string) (*Rule, error) {
	row := db.Conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT id, name, description, type, priority, query_filters, metric, field, condition, threshold, evaluation_interval_seconds, for_seconds, cooldown_seconds, notification_channel_ids, enabled, created_at, updated_at FROM %s.alert_rules FINAL WHERE id = ?",
		db.Database,
	), id)

	var r Rule
	var enabled uint8
	if err := row.Scan(&r.ID, &r.Name, &r.Description, &r.Type, &r.Priority, &r.QueryFilters, &r.Metric, &r.Field, &r.Condition, &r.Threshold, &r.EvaluationIntervalSecs, &r.ForSeconds, &r.CooldownSeconds, &r.NotificationChannelIDs, &enabled, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return nil, fmt.Errorf("alert rule not found")
	}
	r.Enabled = enabled == 1
	return &r, nil
}

// UpdateRule updates an existing alert rule
func UpdateRule(ctx context.Context, id string, rule Rule) (*Rule, error) {
	existing, err := getRuleOnly(ctx, id)
	if err != nil {
		return nil, err
	}

	if rule.Name != "" {
		existing.Name = rule.Name
	}
	if rule.Description != "" {
		existing.Description = rule.Description
	}
	if rule.Type != "" {
		existing.Type = rule.Type
	}
	if rule.Priority != "" && ValidPriorities[rule.Priority] {
		existing.Priority = rule.Priority
	}
	if rule.QueryFilters != "" {
		existing.QueryFilters = rule.QueryFilters
	}
	if rule.Metric != "" {
		existing.Metric = rule.Metric
	}
	if rule.Field != "" {
		existing.Field = rule.Field
	}
	if rule.Condition != "" {
		existing.Condition = rule.Condition
	}
	if rule.Threshold != 0 {
		existing.Threshold = rule.Threshold
	}
	if rule.EvaluationIntervalSecs != 0 {
		existing.EvaluationIntervalSecs = rule.EvaluationIntervalSecs
	}
	if rule.ForSeconds != 0 {
		existing.ForSeconds = rule.ForSeconds
	}
	if rule.CooldownSeconds != 0 {
		existing.CooldownSeconds = rule.CooldownSeconds
	}
	if rule.NotificationChannelIDs != "" {
		existing.NotificationChannelIDs = rule.NotificationChannelIDs
	}
	// Enabled is explicitly set via the update body
	existing.Enabled = rule.Enabled

	now := time.Now().UTC()
	existing.UpdatedAt = now

	enabled := uint8(0)
	if existing.Enabled {
		enabled = 1
	}

	err = db.Conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.alert_rules (id, name, description, type, priority, query_filters, metric, field, condition, threshold, evaluation_interval_seconds, for_seconds, cooldown_seconds, notification_channel_ids, enabled, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		db.Database,
	), existing.ID, existing.Name, existing.Description, existing.Type, existing.Priority, existing.QueryFilters, existing.Metric, existing.Field, existing.Condition, existing.Threshold, existing.EvaluationIntervalSecs, existing.ForSeconds, existing.CooldownSeconds, existing.NotificationChannelIDs, enabled, existing.CreatedAt, existing.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to update alert rule: %w", err)
	}

	return existing, nil
}

// DeleteRule removes an alert rule and its state
func DeleteRule(ctx context.Context, id string) error {
	err := db.Conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.alert_rules DELETE WHERE id = ?", db.Database,
	), id)
	if err != nil {
		return fmt.Errorf("failed to delete alert rule: %w", err)
	}

	// Clean up state
	_ = db.Conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.alert_states DELETE WHERE rule_id = ?", db.Database,
	), id)

	return nil
}

// GetState returns the current state for a rule
func GetState(ctx context.Context, ruleID string) (*State, error) {
	row := db.Conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT rule_id, status, value, fired_at, resolved_at, last_notified_at, updated_at FROM %s.alert_states FINAL WHERE rule_id = ?",
		db.Database,
	), ruleID)

	var s State
	if err := row.Scan(&s.RuleID, &s.Status, &s.Value, &s.FiredAt, &s.ResolvedAt, &s.LastNotifiedAt, &s.UpdatedAt); err != nil {
		return nil, err
	}
	return &s, nil
}

// UpsertState inserts or updates alert state (ReplacingMergeTree handles dedup)
func UpsertState(ctx context.Context, s *State) error {
	return db.Conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.alert_states (rule_id, status, value, fired_at, resolved_at, last_notified_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
		db.Database,
	), s.RuleID, s.Status, s.Value, s.FiredAt, s.ResolvedAt, s.LastNotifiedAt, s.UpdatedAt)
}

// RecordHistory records an alert event in history
func RecordHistory(ctx context.Context, entry HistoryEntry) error {
	entry.ID = uuid.New().String()
	entry.CreatedAt = time.Now().UTC()

	return db.Conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.alert_history (id, rule_id, rule_name, status, value, message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
		db.Database,
	), entry.ID, entry.RuleID, entry.RuleName, entry.Status, entry.Value, entry.Message, entry.CreatedAt)
}

// ListHistory returns alert history entries
func ListHistory(ctx context.Context, ruleID string, limit, offset int) ([]HistoryEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	var query string
	var args []interface{}

	if ruleID != "" {
		query = fmt.Sprintf(
			"SELECT id, rule_id, rule_name, status, value, message, created_at FROM %s.alert_history WHERE rule_id = ? ORDER BY created_at DESC LIMIT %d OFFSET %d",
			db.Database, limit, offset,
		)
		args = append(args, ruleID)
	} else {
		query = fmt.Sprintf(
			"SELECT id, rule_id, rule_name, status, value, message, created_at FROM %s.alert_history ORDER BY created_at DESC LIMIT %d OFFSET %d",
			db.Database, limit, offset,
		)
	}

	rows, err := db.Conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list alert history: %w", err)
	}
	defer rows.Close()

	var entries []HistoryEntry
	for rows.Next() {
		var e HistoryEntry
		if err := rows.Scan(&e.ID, &e.RuleID, &e.RuleName, &e.Status, &e.Value, &e.Message, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan alert history: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// CreateChannel creates a new notification channel
func CreateChannel(ctx context.Context, ch Channel) (*Channel, error) {
	if ch.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	validTypes := map[string]bool{"webhook": true, "slack": true, "email": true, "pagerduty": true}
	if !validTypes[ch.Type] {
		return nil, fmt.Errorf("invalid type: %s (must be webhook, slack, email, or pagerduty)", ch.Type)
	}

	ch.ID = uuid.New().String()
	ch.CreatedAt = time.Now().UTC()

	err := db.Conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.notification_channels (id, name, type, config, created_at) VALUES (?, ?, ?, ?, ?)",
		db.Database,
	), ch.ID, ch.Name, ch.Type, ch.Config, ch.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to insert notification channel: %w", err)
	}

	return &ch, nil
}

// ListChannels returns all notification channels
func ListChannels(ctx context.Context) ([]Channel, error) {
	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT id, name, type, config, created_at FROM %s.notification_channels ORDER BY created_at DESC",
		db.Database,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to list notification channels: %w", err)
	}
	defer rows.Close()

	var channels []Channel
	for rows.Next() {
		var c Channel
		if err := rows.Scan(&c.ID, &c.Name, &c.Type, &c.Config, &c.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan notification channel: %w", err)
		}
		channels = append(channels, c)
	}
	return channels, nil
}

// GetChannel returns a notification channel by ID
func GetChannel(ctx context.Context, id string) (*Channel, error) {
	row := db.Conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT id, name, type, config, created_at FROM %s.notification_channels WHERE id = ?",
		db.Database,
	), id)

	var c Channel
	if err := row.Scan(&c.ID, &c.Name, &c.Type, &c.Config, &c.CreatedAt); err != nil {
		return nil, fmt.Errorf("notification channel not found")
	}
	return &c, nil
}

// DeleteChannel removes a notification channel by ID
func DeleteChannel(ctx context.Context, id string) error {
	err := db.Conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.notification_channels DELETE WHERE id = ?", db.Database,
	), id)
	if err != nil {
		return fmt.Errorf("failed to delete notification channel: %w", err)
	}
	return nil
}
