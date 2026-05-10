package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

// safeIdentifierRegex validates field names to prevent SQL injection
var safeIdentifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

// validFilterColumns are columns that can be used in filter conditions
var validFilterColumns = map[string]bool{
	"service":    true,
	"env":        true,
	"job_id":     true,
	"request_id": true,
	"trace_id":   true,
	"user_id":    true,
	"name":       true,
	"level":      true,
}

// Evaluator periodically evaluates alert rules
type Evaluator struct {
	notifier *Notifier
	// pendingSince tracks when a rule first entered a "pending" state (for for_seconds)
	pendingSince map[string]time.Time
	// lastEvaluated tracks the last evaluation time per rule to respect per-rule intervals
	lastEvaluated map[string]time.Time
}

// NewEvaluator creates a new alert evaluator
func NewEvaluator() *Evaluator {
	return &Evaluator{
		notifier:      NewNotifier(),
		pendingSince:  make(map[string]time.Time),
		lastEvaluated: make(map[string]time.Time),
	}
}

// Run starts the evaluator loop, checking every 15 seconds for rules that need evaluation
func (e *Evaluator) Run(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.evaluateAll(ctx)
		}
	}
}

func (e *Evaluator) evaluateAll(ctx context.Context) {
	rules, err := listEnabledRules(ctx)
	if err != nil {
		log.Printf("alert evaluator: failed to list rules: %v", err)
		return
	}

	now := time.Now()
	for _, rule := range rules {
		interval := time.Duration(rule.EvaluationIntervalSecs) * time.Second
		if last, ok := e.lastEvaluated[rule.ID]; ok && interval > 0 && now.Sub(last) < interval {
			continue
		}
		e.evaluateRule(ctx, &rule)
		e.lastEvaluated[rule.ID] = now
	}
}

// EvaluateRuleNow evaluates a single rule and returns the current value (for testing)
func EvaluateRuleNow(ctx context.Context, rule *Rule) (float64, error) {
	return queryRuleValue(ctx, rule)
}

func (e *Evaluator) evaluateRule(ctx context.Context, rule *Rule) {
	value, err := queryRuleValue(ctx, rule)
	if err != nil {
		log.Printf("alert evaluator: failed to query rule %s (%s): %v", rule.ID, rule.Name, err)
		return
	}

	isFiring := CheckCondition(value, rule.Condition, rule.Threshold)
	state, _ := GetState(ctx, rule.ID)

	now := time.Now().UTC()

	if state == nil {
		state = &State{
			RuleID:    rule.ID,
			Status:    "ok",
			Value:     value,
			UpdatedAt: now,
		}
	}

	previousStatus := state.Status

	switch {
	case isFiring && previousStatus == "ok":
		if rule.ForSeconds > 0 {
			// Enter pending state
			e.pendingSince[rule.ID] = now
			state.Status = "ok" // Stay OK until for_seconds is met
			state.Value = value
			state.UpdatedAt = now
		} else {
			// Fire immediately
			state.Status = "firing"
			state.Value = value
			state.FiredAt = &now
			state.UpdatedAt = now
			e.onFiring(ctx, rule, state)
		}

	case isFiring && previousStatus == "firing":
		state.Value = value
		state.UpdatedAt = now
		// Check cooldown for re-notification
		if state.LastNotifiedAt != nil {
			cooldown := time.Duration(rule.CooldownSeconds) * time.Second
			if now.Sub(*state.LastNotifiedAt) >= cooldown {
				e.onFiring(ctx, rule, state)
			}
		}

	case !isFiring && previousStatus == "firing":
		// Resolved
		state.Status = "ok"
		state.Value = value
		state.ResolvedAt = &now
		state.UpdatedAt = now
		delete(e.pendingSince, rule.ID)
		e.onResolved(ctx, rule, state)

	case !isFiring:
		// Still OK
		delete(e.pendingSince, rule.ID)
		state.Value = value
		state.UpdatedAt = now
	}

	// Check pending -> firing transition
	if pendingSince, ok := e.pendingSince[rule.ID]; ok && isFiring {
		if now.Sub(pendingSince) >= time.Duration(rule.ForSeconds)*time.Second {
			state.Status = "firing"
			state.Value = value
			state.FiredAt = &now
			state.UpdatedAt = now
			delete(e.pendingSince, rule.ID)
			e.onFiring(ctx, rule, state)
		}
	}

	_ = UpsertState(ctx, state)
}

func (e *Evaluator) onFiring(ctx context.Context, rule *Rule, state *State) {
	now := time.Now().UTC()
	state.LastNotifiedAt = &now

	msg := fmt.Sprintf("Alert '%s' is firing: value %.2f %s threshold %.2f", rule.Name, state.Value, rule.Condition, rule.Threshold)

	_ = RecordHistory(ctx, HistoryEntry{
		RuleID:   rule.ID,
		RuleName: rule.Name,
		Status:   "firing",
		Value:    state.Value,
		Message:  msg,
	})

	e.sendNotifications(ctx, rule, msg, state.Value)
}

func (e *Evaluator) onResolved(ctx context.Context, rule *Rule, state *State) {
	now := time.Now().UTC()
	state.LastNotifiedAt = &now

	msg := fmt.Sprintf("Alert '%s' has resolved: value %.2f", rule.Name, state.Value)

	_ = RecordHistory(ctx, HistoryEntry{
		RuleID:   rule.ID,
		RuleName: rule.Name,
		Status:   "resolved",
		Value:    state.Value,
		Message:  msg,
	})

	e.sendNotifications(ctx, rule, msg, state.Value)
}

func (e *Evaluator) sendNotifications(ctx context.Context, rule *Rule, message string, value float64) {
	var channelIDs []string
	if err := json.Unmarshal([]byte(rule.NotificationChannelIDs), &channelIDs); err != nil {
		return
	}

	for _, chID := range channelIDs {
		ch, err := GetChannel(ctx, chID)
		if err != nil {
			log.Printf("alert evaluator: failed to get channel %s: %v", chID, err)
			continue
		}
		if err := e.notifier.Send(ch, rule.Name, message, value); err != nil {
			log.Printf("alert evaluator: failed to send notification via channel %s: %v", chID, err)
		}
	}
}

func queryRuleValue(ctx context.Context, rule *Rule) (float64, error) {
	// Parse filters
	var filters []structs.QueryFilter
	if rule.QueryFilters != "" && rule.QueryFilters != "[]" {
		if err := json.Unmarshal([]byte(rule.QueryFilters), &filters); err != nil {
			return 0, fmt.Errorf("failed to parse query filters: %w", err)
		}
	}

	// Determine time range based on evaluation interval
	to := time.Now().UTC()
	from := to.Add(-time.Duration(rule.EvaluationIntervalSecs) * time.Second)

	// Build aggregation
	agg := structs.AggregationType(rule.Metric)
	if agg == "" {
		agg = structs.AggCount
	}

	aggExpr, err := buildAggExpr(agg, rule.Field)
	if err != nil {
		return 0, err
	}

	// Build query
	sql := fmt.Sprintf("SELECT %s AS value FROM %s.events WHERE timestamp >= ? AND timestamp <= ?", aggExpr, db.Database)
	args := []interface{}{from, to}

	for _, f := range filters {
		cond, condArgs, err := buildFilterCondition(f)
		if err != nil {
			return 0, err
		}
		sql += " AND " + cond
		args = append(args, condArgs...)
	}

	var value float64
	if err := db.Conn.QueryRow(ctx, sql, args...).Scan(&value); err != nil {
		return 0, fmt.Errorf("query failed: %w", err)
	}

	return value, nil
}

func buildAggExpr(agg structs.AggregationType, field string) (string, error) {
	switch agg {
	case structs.AggCount:
		return "toFloat64(count())", nil
	case structs.AggSum:
		col, err := numericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("toFloat64(sum(%s))", col), nil
	case structs.AggAvg:
		col, err := numericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("toFloat64(avg(%s))", col), nil
	case structs.AggMin:
		col, err := numericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("toFloat64(min(%s))", col), nil
	case structs.AggMax:
		col, err := numericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("toFloat64(max(%s))", col), nil
	default:
		return "toFloat64(count())", nil
	}
}

func numericFieldExpr(field string) (string, error) {
	if field == "" {
		return "", fmt.Errorf("field is required for this aggregation")
	}
	if len(field) > 5 && field[:5] == "data." {
		key := field[5:]
		if !safeIdentifierRegex.MatchString(key) {
			return "", fmt.Errorf("invalid data field name: %s", key)
		}
		return fmt.Sprintf("toFloat64OrNull(JSONExtractRaw(data, '%s'))", key), nil
	}
	return "", fmt.Errorf("numeric aggregation only supported on data.* fields")
}

func buildFilterCondition(f structs.QueryFilter) (string, []interface{}, error) {
	var fieldExpr string

	if len(f.Field) > 5 && f.Field[:5] == "data." {
		key := f.Field[5:]
		if !safeIdentifierRegex.MatchString(key) {
			return "", nil, fmt.Errorf("invalid data field name: %s", key)
		}
		switch f.Operator {
		case "lt", "gt", "lte", "gte":
			fieldExpr = fmt.Sprintf("toFloat64OrNull(JSONExtractRaw(data, '%s'))", key)
		default:
			fieldExpr = fmt.Sprintf("JSONExtractString(data, '%s')", key)
		}
	} else if validFilterColumns[f.Field] {
		fieldExpr = f.Field
	} else {
		return "", nil, fmt.Errorf("invalid filter field: %s", f.Field)
	}

	switch f.Operator {
	case "eq", "":
		return fmt.Sprintf("%s = ?", fieldExpr), []interface{}{f.Value}, nil
	case "neq":
		return fmt.Sprintf("%s != ?", fieldExpr), []interface{}{f.Value}, nil
	case "lt":
		return fmt.Sprintf("%s < ?", fieldExpr), []interface{}{f.Value}, nil
	case "gt":
		return fmt.Sprintf("%s > ?", fieldExpr), []interface{}{f.Value}, nil
	case "lte":
		return fmt.Sprintf("%s <= ?", fieldExpr), []interface{}{f.Value}, nil
	case "gte":
		return fmt.Sprintf("%s >= ?", fieldExpr), []interface{}{f.Value}, nil
	case "contains":
		return fmt.Sprintf("%s LIKE ?", fieldExpr), []interface{}{fmt.Sprintf("%%%v%%", f.Value)}, nil
	default:
		return fmt.Sprintf("%s = ?", fieldExpr), []interface{}{f.Value}, nil
	}
}

// CheckCondition evaluates whether a value meets the alert condition against the threshold
func CheckCondition(value float64, condition string, threshold float64) bool {
	switch condition {
	case "gt":
		return value > threshold
	case "lt":
		return value < threshold
	case "gte":
		return value >= threshold
	case "lte":
		return value <= threshold
	case "eq":
		return value == threshold
	default:
		return false
	}
}

func listEnabledRules(ctx context.Context) ([]Rule, error) {
	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT id, name, description, type, query_filters, metric, field, condition, threshold, evaluation_interval_seconds, for_seconds, cooldown_seconds, notification_channel_ids, enabled, created_at, updated_at FROM %s.alert_rules FINAL WHERE enabled = 1",
		db.Database,
	))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rules []Rule
	for rows.Next() {
		var r Rule
		var enabled uint8
		if err := rows.Scan(&r.ID, &r.Name, &r.Description, &r.Type, &r.QueryFilters, &r.Metric, &r.Field, &r.Condition, &r.Threshold, &r.EvaluationIntervalSecs, &r.ForSeconds, &r.CooldownSeconds, &r.NotificationChannelIDs, &enabled, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		r.Enabled = enabled == 1
		rules = append(rules, r)
	}
	return rules, nil
}
