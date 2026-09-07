package query

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/google/uuid"
)

// Alert priorities. Untyped string constants rather than a typed enum, matching
// structs.AlertRule — see the note on that type for why the typed-enum pass is
// held back to its own change.
const (
	ALERT_PRIORITY_CRITICAL = "P0"
	ALERT_PRIORITY_HIGH     = "P1"
	ALERT_PRIORITY_MEDIUM   = "P2"
	ALERT_PRIORITY_LOW      = "P3"
)

// The four closed value sets migration 119 turned into MariaDB ENUMs.
//
// These are kept as a PRE-FLIGHT even though the column now enforces them,
// because the two failures read very differently to a caller: this produces
// `invalid condition: gtt` with a 400, while the database produces errno 1265
// "Data truncated for column 'condition'" with a 500. The database is the
// backstop, not the message.
var (
	validAlertPriorities = map[string]bool{
		ALERT_PRIORITY_CRITICAL: true,
		ALERT_PRIORITY_HIGH:     true,
		ALERT_PRIORITY_MEDIUM:   true,
		ALERT_PRIORITY_LOW:      true,
	}
	validAlertTypes      = map[string]bool{"threshold": true, "absence": true, "rate_change": true}
	validAlertMetrics    = map[string]bool{"count": true, "sum": true, "avg": true, "min": true, "max": true}
	validAlertConditions = map[string]bool{"gt": true, "lt": true, "gte": true, "lte": true, "eq": true}
)

// alertRulesTable is schema-qualified because the DSN's default database is
// monitor_auth. The MariaDB `monitor` schema is a different store from the
// ClickHouse database of the same name — this one is reached through db.SQL.
const alertRulesTable = "monitor.alert_rules"

// alertRuleColumns is the SELECT list, and its ORDER is what scanAlertRule
// unpacks positionally. Adding a column to one and not the other is a silent
// mis-scan, not a compile error.
//
// `condition` needs no backticks HERE and does need them in every unqualified
// use below. It is a MariaDB reserved word, but the documented exception is that
// a word following a period in a qualified name is always read as an identifier
// — so `monitor.alert_rules.condition` is legal bare while a lone `condition` in
// an INSERT column list or an UPDATE SET clause is a syntax error. Migration 119
// carries the full note; TestAlertRuleSQLQuotesReservedWords pins it.
var alertRuleColumns = []string{
	"monitor.alert_rules.id",
	"monitor.alert_rules.name",
	"monitor.alert_rules.description",
	"monitor.alert_rules.type",
	"monitor.alert_rules.priority",
	"monitor.alert_rules.query_filters",
	"monitor.alert_rules.metric",
	"monitor.alert_rules.field",
	"monitor.alert_rules.condition",
	"monitor.alert_rules.threshold",
	"monitor.alert_rules.evaluation_interval_seconds",
	"monitor.alert_rules.for_seconds",
	"monitor.alert_rules.cooldown_seconds",
	"monitor.alert_rules.notification_channel_ids",
	"monitor.alert_rules.enabled",
	"monitor.alert_rules.created_at",
	"monitor.alert_rules.updated_at",
}

// alertRuleInsertColumns names the same columns for a write, where `condition`
// IS unqualified and therefore MUST be backticked.
var alertRuleInsertColumns = []string{
	"id", "name", "description", "type", "priority", "query_filters", "metric",
	"field", "`condition`", "threshold", "evaluation_interval_seconds",
	"for_seconds", "cooldown_seconds", "notification_channel_ids", "enabled",
	"created_at", "updated_at",
}

type alertRuleScanner interface {
	Scan(dest ...interface{}) error
}

func scanAlertRule(row alertRuleScanner) (*structs.AlertRule, error) {
	var r structs.AlertRule
	if err := row.Scan(
		&r.ID, &r.Name, &r.Description, &r.Type, &r.Priority, &r.QueryFilters,
		&r.Metric, &r.Field, &r.Condition, &r.Threshold,
		&r.EvaluationIntervalSecs, &r.ForSeconds, &r.CooldownSeconds,
		&r.NotificationChannelIDs, &r.Enabled, &r.CreatedAt, &r.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return &r, nil
}

// requireJSONText rejects a value bound for one of the JSON columns migrations
// 119-122 introduced.
//
// It exists so the caller reads `query_filters must be valid JSON` instead of
// MariaDB's errno 4025 — `CONSTRAINT \`service_groups.services\` failed`, verified
// against 11.4 — which names a generated check constraint rather than saying what
// was wrong with the value. The check is the same one the column makes,
// deliberately duplicated: the column is the guarantee, this is the message.
func requireJSONText(field, value string) error {
	if !json.Valid([]byte(value)) {
		return fmt.Errorf("%s must be valid JSON", field)
	}
	return nil
}

// CreateAlertRuleRequest is the POST /v1/alert-rules body.
//
// It is a request type rather than a bare structs.AlertRule so that id,
// created_at and updated_at are not fields a caller can send. They were
// accepted and then silently overwritten before, which is the kind of thing
// somebody eventually depends on by accident.
type CreateAlertRuleRequest struct {
	Name                   string  `json:"name"`
	Description            string  `json:"description"`
	Type                   string  `json:"type"`
	Priority               string  `json:"priority"`
	QueryFilters           string  `json:"query_filters"`
	Metric                 string  `json:"metric"`
	Field                  string  `json:"field"`
	Condition              string  `json:"condition"`
	Threshold              float64 `json:"threshold"`
	EvaluationIntervalSecs uint32  `json:"evaluation_interval_seconds"`
	ForSeconds             uint32  `json:"for_seconds"`
	CooldownSeconds        uint32  `json:"cooldown_seconds"`
	NotificationChannelIDs string  `json:"notification_channel_ids"`
	Enabled                bool    `json:"enabled"`
}

// CreateAlertRule validates, applies defaults and inserts one rule.
//
// The defaulting is carried over verbatim from the ClickHouse implementation,
// including the quirk that an unrecognised priority is silently coerced to P2
// while an unrecognised type or condition is an error. That asymmetry is
// preserved rather than tidied: priority is advisory (it only selects a routing
// policy) whereas type and condition decide whether the rule can be evaluated at
// all.
func CreateAlertRule(engine db.Queryable, req CreateAlertRuleRequest) (*structs.AlertRule, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if req.Type == "" {
		return nil, fmt.Errorf("type is required")
	}
	if !validAlertTypes[req.Type] {
		return nil, fmt.Errorf("invalid type: %s (must be threshold, absence, or rate_change)", req.Type)
	}
	if req.Condition == "" {
		return nil, fmt.Errorf("condition is required")
	}
	if !validAlertConditions[req.Condition] {
		return nil, fmt.Errorf("invalid condition: %s", req.Condition)
	}

	now := time.Now().UTC()
	rule := structs.AlertRule{
		ID:                     uuid.New().String(),
		Name:                   req.Name,
		Description:            req.Description,
		Type:                   req.Type,
		Priority:               req.Priority,
		QueryFilters:           req.QueryFilters,
		Metric:                 req.Metric,
		Field:                  req.Field,
		Condition:              req.Condition,
		Threshold:              req.Threshold,
		EvaluationIntervalSecs: req.EvaluationIntervalSecs,
		ForSeconds:             req.ForSeconds,
		CooldownSeconds:        req.CooldownSeconds,
		NotificationChannelIDs: req.NotificationChannelIDs,
		Enabled:                req.Enabled,
		CreatedAt:              now,
		UpdatedAt:              now,
	}

	if rule.Priority == "" || !validAlertPriorities[rule.Priority] {
		rule.Priority = ALERT_PRIORITY_MEDIUM
	}
	if rule.Metric == "" {
		rule.Metric = "count"
	}
	if !validAlertMetrics[rule.Metric] {
		return nil, fmt.Errorf("invalid metric: %s (must be count, sum, avg, min, or max)", rule.Metric)
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
	if err := requireJSONText("query_filters", rule.QueryFilters); err != nil {
		return nil, err
	}
	if err := requireJSONText("notification_channel_ids", rule.NotificationChannelIDs); err != nil {
		return nil, err
	}

	qStr, args, err := sq.Insert(alertRulesTable).
		Columns(alertRuleInsertColumns...).
		Values(rule.ID, rule.Name, rule.Description, rule.Type, rule.Priority,
			rule.QueryFilters, rule.Metric, rule.Field, rule.Condition, rule.Threshold,
			rule.EvaluationIntervalSecs, rule.ForSeconds, rule.CooldownSeconds,
			rule.NotificationChannelIDs, rule.Enabled, rule.CreatedAt, rule.UpdatedAt).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to insert alert rule: %w", err)
	}
	return &rule, nil
}

// ListAlertRules returns every rule, newest first.
//
// Unpaginated, matching the endpoint it serves. db.MAX_LIMIT is deliberately not
// applied: this is the whole alerting configuration, six rows on the live
// instance, and a truncated list would silently hide rules from the page an
// operator uses to check that nothing is missing.
func ListAlertRules(engine db.Queryable) ([]structs.AlertRule, error) {
	q := sq.Select(alertRuleColumns...).From(alertRulesTable).
		OrderBy("monitor.alert_rules.created_at DESC", "monitor.alert_rules.id ASC")

	return queryAlertRules(engine, q)
}

// ListEnabledAlertRules returns the rules the evaluator should run. Called every
// 15 seconds by Evaluator.evaluateAll.
func ListEnabledAlertRules(engine db.Queryable) ([]structs.AlertRule, error) {
	q := sq.Select(alertRuleColumns...).From(alertRulesTable).
		Where(sq.Eq{"monitor.alert_rules.enabled": true}).
		OrderBy("monitor.alert_rules.created_at DESC", "monitor.alert_rules.id ASC")

	return queryAlertRules(engine, q)
}

func queryAlertRules(engine db.Queryable, q sq.SelectBuilder) ([]structs.AlertRule, error) {
	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	rows, err := engine.Query(qStr, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql query: %w", err)
	}
	defer rows.Close()

	rules := []structs.AlertRule{}
	for rows.Next() {
		rule, err := scanAlertRule(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan alert rule: %w", err)
		}
		rules = append(rules, *rule)
	}
	return rules, rows.Err()
}

// GetAlertRule returns one rule by id, or (nil, nil) when there is none.
//
// Absence is not an error here, following GetServiceRepo and GetIssue: the
// caller decides whether a missing row is a 404, a skip, or the normal case.
func GetAlertRule(engine db.Queryable, id string) (*structs.AlertRule, error) {
	q := sq.Select(alertRuleColumns...).From(alertRulesTable).
		Where(sq.Eq{"monitor.alert_rules.id": id}).Limit(1)

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	rule, err := scanAlertRule(engine.QueryRow(qStr, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan alert rule: %w", err)
	}
	return rule, nil
}

// UpdateAlertRuleRequest is the partial-update payload for
// PUT /v1/alert-rules/{id}. Every field is a pointer so an ABSENT field (nil) is
// distinguishable from a zero/false value. Only non-nil fields are applied;
// omitting a field preserves the current value — critically, omitting `enabled`
// does not disable the rule.
type UpdateAlertRuleRequest struct {
	Name                   *string  `json:"name"`
	Description            *string  `json:"description"`
	Type                   *string  `json:"type"`
	Priority               *string  `json:"priority"`
	QueryFilters           *string  `json:"query_filters"`
	Metric                 *string  `json:"metric"`
	Field                  *string  `json:"field"`
	Condition              *string  `json:"condition"`
	Threshold              *float64 `json:"threshold"`
	EvaluationIntervalSecs *uint32  `json:"evaluation_interval_seconds"`
	ForSeconds             *uint32  `json:"for_seconds"`
	CooldownSeconds        *uint32  `json:"cooldown_seconds"`
	NotificationChannelIDs *string  `json:"notification_channel_ids"`
	Enabled                *bool    `json:"enabled"`
}

// IsEmpty reports whether the request would change nothing.
func (r UpdateAlertRuleRequest) IsEmpty() bool {
	return r.Name == nil && r.Description == nil && r.Type == nil && r.Priority == nil &&
		r.QueryFilters == nil && r.Metric == nil && r.Field == nil && r.Condition == nil &&
		r.Threshold == nil && r.EvaluationIntervalSecs == nil && r.ForSeconds == nil &&
		r.CooldownSeconds == nil && r.NotificationChannelIDs == nil && r.Enabled == nil
}

// UpdateAlertRule applies a partial update and returns the stored row.
//
// This is now a real UPDATE of only the named columns, where the ClickHouse
// version had to read the whole row, merge in Go, and INSERT a complete new
// version — which is why the previous implementation could lose a concurrent
// edit to a field it was not itself changing. It cannot now: two callers editing
// different fields of one rule touch different columns.
//
// A missing rule is returned as an ERROR, not as (nil, nil), unlike
// GetAlertRule. That is deliberate and preserves the deployed contract: the
// handler renders any error from this call as a 400 with its message, so
// returning (nil, nil) would turn a well-understood 400 into whatever the
// handler does with a nil rule.
func UpdateAlertRule(engine db.Queryable, id string, req UpdateAlertRuleRequest) (*structs.AlertRule, error) {
	existing, err := GetAlertRule(engine, id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("alert rule not found")
	}
	if req.IsEmpty() {
		return existing, nil
	}

	u := sq.Update(alertRulesTable)

	if req.Name != nil {
		if *req.Name == "" {
			return nil, fmt.Errorf("name cannot be empty")
		}
		u = u.Set("name", *req.Name)
	}
	if req.Description != nil {
		u = u.Set("description", *req.Description)
	}
	if req.Type != nil {
		if !validAlertTypes[*req.Type] {
			return nil, fmt.Errorf("invalid type: %s (must be threshold, absence, or rate_change)", *req.Type)
		}
		u = u.Set("type", *req.Type)
	}
	if req.Priority != nil {
		if !validAlertPriorities[*req.Priority] {
			return nil, fmt.Errorf("invalid priority: %s", *req.Priority)
		}
		u = u.Set("priority", *req.Priority)
	}
	if req.QueryFilters != nil {
		if err := requireJSONText("query_filters", *req.QueryFilters); err != nil {
			return nil, err
		}
		u = u.Set("query_filters", *req.QueryFilters)
	}
	if req.Metric != nil {
		if !validAlertMetrics[*req.Metric] {
			return nil, fmt.Errorf("invalid metric: %s (must be count, sum, avg, min, or max)", *req.Metric)
		}
		u = u.Set("metric", *req.Metric)
	}
	if req.Field != nil {
		u = u.Set("field", *req.Field)
	}
	if req.Condition != nil {
		if !validAlertConditions[*req.Condition] {
			return nil, fmt.Errorf("invalid condition: %s", *req.Condition)
		}
		// Backticked: an UPDATE SET clause is an UNQUALIFIED use of a reserved
		// word. Without them this is a syntax error at execution time only —
		// the query builds fine and nothing catches it until a rule is edited.
		u = u.Set("`condition`", *req.Condition)
	}
	if req.Threshold != nil {
		u = u.Set("threshold", *req.Threshold)
	}
	if req.EvaluationIntervalSecs != nil {
		u = u.Set("evaluation_interval_seconds", *req.EvaluationIntervalSecs)
	}
	if req.ForSeconds != nil {
		u = u.Set("for_seconds", *req.ForSeconds)
	}
	if req.CooldownSeconds != nil {
		u = u.Set("cooldown_seconds", *req.CooldownSeconds)
	}
	if req.NotificationChannelIDs != nil {
		if err := requireJSONText("notification_channel_ids", *req.NotificationChannelIDs); err != nil {
			return nil, err
		}
		u = u.Set("notification_channel_ids", *req.NotificationChannelIDs)
	}
	if req.Enabled != nil {
		u = u.Set("enabled", *req.Enabled)
	}

	// updated_at is NOT set here. The column carries ON UPDATE
	// CURRENT_TIMESTAMP(3), so MariaDB stamps it — and only when a column
	// actually changed value, which is more truthful than the old code's
	// unconditional time.Now().
	qStr, args, err := u.Where(sq.Eq{"id": id}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to update alert rule: %w", err)
	}
	return GetAlertRule(engine, id)
}

// DeleteAlertRule removes one rule and reports whether it existed.
//
// The rule's ClickHouse alert_states row is NOT cleaned up here — that is a
// different store, and alerts.DeleteRule is the caller that does both. Splitting
// it that way keeps this file a pure MariaDB query layer.
func DeleteAlertRule(engine db.Queryable, id string) (bool, error) {
	qStr, args, err := sq.Delete(alertRulesTable).Where(sq.Eq{"id": id}).ToSql()
	if err != nil {
		return false, fmt.Errorf("failed to build sql query: %w", err)
	}

	res, err := engine.Exec(qStr, args...)
	if err != nil {
		return false, fmt.Errorf("failed to delete alert rule: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, nil
	}
	return affected > 0, nil
}
