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
	router   *Router
	alertHub *AlertHub
	// pendingSince tracks when a rule first entered a "pending" state (for for_seconds)
	pendingSince map[string]time.Time
	// lastEvaluated tracks the last evaluation time per rule to respect per-rule intervals
	lastEvaluated map[string]time.Time
}

// NewEvaluator creates a new alert evaluator
func NewEvaluator(alertHub *AlertHub) *Evaluator {
	return &Evaluator{
		router:        NewRouter(),
		alertHub:      alertHub,
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

// EvaluateRuleNow evaluates a single rule and returns the current value and
// whether it is firing (for the test endpoint). It branches on rule.Type just
// like the live evaluator.
func EvaluateRuleNow(ctx context.Context, rule *Rule) (value float64, isFiring bool, err error) {
	return evaluateRuleState(ctx, rule)
}

func (e *Evaluator) evaluateRule(ctx context.Context, rule *Rule) {
	value, isFiring, err := evaluateRuleState(ctx, rule)
	if err != nil {
		log.Printf("alert evaluator: failed to query rule %s (%s): %v", rule.ID, rule.Name, err)
		return
	}

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

	if e.alertHub != nil {
		e.alertHub.PublishStateChange(rule.ID, rule.Name, "firing", msg, state.Value)
	}

	alertCtx := BuildAlertContext(ctx, rule, "firing", state.Value, msg)
	if err := e.router.Route(ctx, alertCtx, rule); err != nil {
		log.Printf("alert evaluator: routing failed for rule %s: %v", rule.ID, err)
	}
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

	if e.alertHub != nil {
		e.alertHub.PublishStateChange(rule.ID, rule.Name, "resolved", msg, state.Value)
	}

	alertCtx := BuildAlertContext(ctx, rule, "resolved", state.Value, msg)
	if err := e.router.Route(ctx, alertCtx, rule); err != nil {
		log.Printf("alert evaluator: routing failed for rule %s: %v", rule.ID, err)
	}
}

// evaluateRuleState computes a rule's current value and whether it is firing,
// branching on rule.Type. The "value" recorded is type-dependent (see below).
func evaluateRuleState(ctx context.Context, rule *Rule) (value float64, isFiring bool, err error) {
	now := time.Now().UTC()
	interval := time.Duration(rule.EvaluationIntervalSecs) * time.Second

	switch rule.Type {
	case "absence":
		// COUNT over the interval ignoring metric/field; firing when nothing arrived.
		count, err := queryCountForRange(ctx, rule, now.Add(-interval), now)
		if err != nil {
			return 0, false, err
		}
		return count, count == 0, nil

	case "rate_change":
		// Percent change of the aggregate between the previous and current window.
		cur, err := queryValueForRange(ctx, rule, now.Add(-interval), now)
		if err != nil {
			return 0, false, err
		}
		prev, err := queryValueForRange(ctx, rule, now.Add(-2*interval), now.Add(-interval))
		if err != nil {
			return 0, false, err
		}
		var pct float64
		if prev == 0 {
			if cur > 0 {
				pct = 100
			} else {
				pct = 0
			}
		} else {
			pct = (cur - prev) / prev * 100
		}
		return pct, CheckCondition(pct, rule.Condition, rule.Threshold), nil

	default:
		// threshold (and empty/unknown): aggregate over the interval vs threshold.
		v, err := queryValueForRange(ctx, rule, now.Add(-interval), now)
		if err != nil {
			return 0, false, err
		}
		return v, CheckCondition(v, rule.Condition, rule.Threshold), nil
	}
}

// parseRuleFilters decodes a rule's query_filters JSON array.
func parseRuleFilters(rule *Rule) ([]structs.QueryFilter, error) {
	var filters []structs.QueryFilter
	if rule.QueryFilters != "" && rule.QueryFilters != "[]" {
		if err := json.Unmarshal([]byte(rule.QueryFilters), &filters); err != nil {
			return nil, fmt.Errorf("failed to parse query filters: %w", err)
		}
	}
	return filters, nil
}

// queryAggForRange runs an arbitrary aggregation expression over [from,to],
// applying the rule's query_filters. aggExpr is a trusted, code-built expression.
func queryAggForRange(ctx context.Context, rule *Rule, aggExpr string, from, to time.Time) (float64, error) {
	filters, err := parseRuleFilters(rule)
	if err != nil {
		return 0, err
	}

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

// queryValueForRange runs the rule's configured metric aggregation over [from,to].
func queryValueForRange(ctx context.Context, rule *Rule, from, to time.Time) (float64, error) {
	agg := structs.AggregationType(rule.Metric)
	if agg == "" {
		agg = structs.AggCount
	}
	aggExpr, err := buildAggExpr(agg, rule.Field)
	if err != nil {
		return 0, err
	}
	return queryAggForRange(ctx, rule, aggExpr, from, to)
}

// queryCountForRange runs a COUNT over [from,to] regardless of the rule's metric
// (used by absence alerts, which only care whether any matching event arrived).
func queryCountForRange(ctx context.Context, rule *Rule, from, to time.Time) (float64, error) {
	return queryAggForRange(ctx, rule, "toFloat64(count())", from, to)
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
		"SELECT id, name, description, type, priority, query_filters, metric, field, condition, threshold, evaluation_interval_seconds, for_seconds, cooldown_seconds, notification_channel_ids, enabled, created_at, updated_at FROM %s.alert_rules FINAL WHERE enabled = 1",
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
		if err := rows.Scan(&r.ID, &r.Name, &r.Description, &r.Type, &r.Priority, &r.QueryFilters, &r.Metric, &r.Field, &r.Condition, &r.Threshold, &r.EvaluationIntervalSecs, &r.ForSeconds, &r.CooldownSeconds, &r.NotificationChannelIDs, &enabled, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		r.Enabled = enabled == 1
		rules = append(rules, r)
	}
	return rules, nil
}
