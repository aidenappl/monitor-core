package alerts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/structs"
)

// KNOWN GAP — TIMER-DRIVEN alert evaluation is ZONE-WIDE. Recorded 2026-09-06,
// alongside the change that made every event READ project-scoped; narrowed the
// same day, when the HTTP half of it turned out to be a live leak rather than a
// gap.
//
// TWO CALLERS, TWO ANSWERS. evaluateRuleState is reached from exactly two
// places, and they differ in the only thing that matters here — whether a
// request is on the other end of it:
//
//   - Evaluator.Run, on a 15-second timer, with a background context. There is
//     no request, no credential and therefore no project to scope against, and
//     nothing is returned to a caller: the value becomes a firing decision and a
//     notification. This is the gap, and it stays.
//   - EvaluateRuleNow, from routes.HandleTestAlertRule (POST
//     /v1/alert-rules/{id}/test), which passes r.Context() — an ordinary /v1
//     request context that HAS been through QueryAuthMiddleware and DOES carry a
//     project. Its aggregate is returned to the caller as JSON.
//
// The second was NOT a gap, it was an oracle. A rule carries a caller-chosen
// aggregation, field and filter set, so an admin key bound to project A could
// POST a rule and read back max(data.amount), count() or a contains-filtered
// count over EVERY project's events — one number at a time, but a number of
// someone else's data on demand, with no row ever crossing the boundary to
// notice. The reasoning that used to sit here ("no rule can be made to read a
// project its author cannot already read, because rule authorship is itself an
// admin-scoped action") was true when admin meant global and became false the
// moment middleware/query_auth.go decided that admin is a scope over VERBS and
// never over tenants. It is restated here because that inversion is the exact
// shape of the next mistake: a pre-tenancy safety argument that nobody
// re-derived after tenancy landed.
//
// So queryAggForRange now applies scope.ProjectPredicate WHENEVER the context
// carries a project, and only falls through to zone-wide on the sentinel that
// means "no request was involved". The timer keeps the documented behaviour; the
// endpoint stops answering questions about other tenants.
//
// CONSEQUENCE, ON PURPOSE: a rule's test result and the value that fires it can
// now disagree once a second project exists — the test reports the caller's
// project, the timer counts the zone. They agree today, because every credential
// resolves to env.DefaultProjectSlug (migration 117 bound all existing keys to
// it). A test that under-reports is a confusing false negative; a test that
// reports another tenant's traffic is a disclosure, and only one of those is
// worth keeping.
//
// "project" also remains in structs.FilterColumns so a TIMER rule can opt in
// with a query_filters entry of {"field":"project","value":"..."}. Close the
// remaining half by giving alert_rules a project column, resolving it in
// listEnabledRules, and passing it into queryAggForRange as the same predicate —
// at which point the ctx-conditional below collapses into an unconditional one.

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
	rules, err := listEnabledRules()
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
func EvaluateRuleNow(ctx context.Context, rule *structs.AlertRule) (value float64, isFiring bool, err error) {
	return evaluateRuleState(ctx, rule)
}

func (e *Evaluator) evaluateRule(ctx context.Context, rule *structs.AlertRule) {
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

func (e *Evaluator) onFiring(ctx context.Context, rule *structs.AlertRule, state *State) {
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

func (e *Evaluator) onResolved(ctx context.Context, rule *structs.AlertRule, state *State) {
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
func evaluateRuleState(ctx context.Context, rule *structs.AlertRule) (value float64, isFiring bool, err error) {
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
func parseRuleFilters(rule *structs.AlertRule) ([]structs.QueryFilter, error) {
	var filters []structs.QueryFilter
	if rule.QueryFilters != "" && rule.QueryFilters != "[]" {
		if err := json.Unmarshal([]byte(rule.QueryFilters), &filters); err != nil {
			return nil, fmt.Errorf("failed to parse query filters: %w", err)
		}
	}
	return filters, nil
}

// buildAggQuery assembles the statement queryAggForRange runs, and is split out
// of it for the same reason routes.subscriptionFilters is split out of its
// handler: the thing worth asserting on is the text, and it is unreachable
// through a function whose next act is to hit ClickHouse. A test can call this
// twice — once with a project on the context, once without — and read the
// difference, which no assertion about a returned float64 could show.
//
// aggExpr is a trusted, code-built expression; the rule's filters are bound.
func buildAggQuery(ctx context.Context, rule *structs.AlertRule, aggExpr string, from, to time.Time) (string, []interface{}, error) {
	filters, err := parseRuleFilters(rule)
	if err != nil {
		return "", nil, err
	}

	sql := fmt.Sprintf("SELECT %s AS value FROM %s.events WHERE timestamp >= ? AND timestamp <= ?", aggExpr, db.Database)
	args := []interface{}{from, to}

	// Attached BEFORE the rule's own filters, not after: these are positional
	// `?` placeholders, so the args have to be appended in the order their
	// placeholders appear in the text. Splicing this in below the loop would
	// slide every filter's binding one position along — valid SQL, no error,
	// wrong rows.
	predicate, scopeArgs, err := scope.ProjectPredicate(ctx)
	switch {
	case err == nil:
		sql += " AND " + predicate
		args = append(args, scopeArgs...)
	case errors.Is(err, scope.ErrNoProject):
		// The timer path. Deliberately zone-wide — the documented half of the
		// gap above — and reached only when no request context exists.
	default:
		// ErrNoProject is the only error ProjectPredicate returns today. Any
		// other one means the scoping rule changed under this call, and the safe
		// reading of "I could not work out whose data this is" is to refuse
		// rather than to fall back to every tenant's.
		return "", nil, err
	}

	for _, f := range filters {
		cond, condArgs, err := buildFilterCondition(f)
		if err != nil {
			return "", nil, err
		}
		sql += " AND " + cond
		args = append(args, condArgs...)
	}

	return sql, args, nil
}

// queryAggForRange runs an arbitrary aggregation expression over [from,to],
// applying the rule's query_filters.
//
// The tenancy predicate is attached when — and only when — the context carries a
// project. See the KNOWN GAP header for why that is conditional rather than
// mandatory: the timer has no request to derive one from, the HTTP test endpoint
// does, and the aggregate this returns is handed straight back to that endpoint's
// caller.
func queryAggForRange(ctx context.Context, rule *structs.AlertRule, aggExpr string, from, to time.Time) (float64, error) {
	sql, args, err := buildAggQuery(ctx, rule, aggExpr, from, to)
	if err != nil {
		return 0, err
	}

	var value float64
	if err := db.Conn.QueryRow(ctx, sql, args...).Scan(&value); err != nil {
		return 0, fmt.Errorf("query failed: %w", err)
	}
	return value, nil
}

// queryValueForRange runs the rule's configured metric aggregation over [from,to].
func queryValueForRange(ctx context.Context, rule *structs.AlertRule, from, to time.Time) (float64, error) {
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
func queryCountForRange(ctx context.Context, rule *structs.AlertRule, from, to time.Time) (float64, error) {
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
		if !structs.SafeIdentifierRegex.MatchString(key) {
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
		if !structs.SafeIdentifierRegex.MatchString(key) {
			return "", nil, fmt.Errorf("invalid data field name: %s", key)
		}
		switch f.Operator {
		case "lt", "gt", "lte", "gte":
			fieldExpr = fmt.Sprintf("toFloat64OrNull(JSONExtractRaw(data, '%s'))", key)
		default:
			fieldExpr = fmt.Sprintf("JSONExtractString(data, '%s')", key)
		}
	} else if structs.FilterColumns[f.Field] {
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

// listEnabledRules reads the rules the evaluator should run.
//
// It is a MariaDB read now (migration 119), so it no longer takes a context: the
// query layer's signature is db.Queryable-first and carries none, matching every
// other relational read in the repo. The wrapper is kept rather than inlined at
// the one call site because it is the seam a future project-scoped evaluator
// changes — see the KNOWN GAP header above.
//
// The `FINAL` that used to be on this statement is gone with the engine that
// needed it. It was there because a ReplacingMergeTree can hold several versions
// of one rule until a background merge collapses them, so a read without it
// could return a rule as enabled after it had been disabled — a disabled rule
// that kept firing until ClickHouse got around to merging. There is one row per
// rule now.
func listEnabledRules() ([]structs.AlertRule, error) {
	return query.ListEnabledAlertRules(db.SQL)
}
