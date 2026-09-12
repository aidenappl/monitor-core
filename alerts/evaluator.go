package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/structs"
)

// CLOSED — TIMER-DRIVEN alert evaluation used to be ZONE-WIDE. Recorded
// 2026-09-06 as a known gap, narrowed the same day when its HTTP half turned out
// to be a live leak, and closed here by migration 127.
//
// WHAT IT WAS. evaluateRuleState is reached from two places, and they differed
// in the only thing that mattered — whether a request was on the other end:
//
//   - Evaluator.Run, on a 15-second timer, with a BACKGROUND context. There was
//     no request, no credential and therefore no project, so the aggregate ran
//     over the whole zone. A rule reading "more than 50 errors in a minute"
//     counted every tenant's errors, and fired for a project whose own traffic
//     was quiet.
//   - EvaluateRuleNow, from routes.HandleTestAlertRule, which passes the
//     request's context and IS scoped. That half was never a gap, it was an
//     ORACLE: a rule carries a caller-chosen aggregation, field and filter set,
//     so an admin key bound to project A could POST a rule and read back
//     max(data.amount) or a contains-filtered count over EVERY project's events,
//     one number at a time, with no row crossing the boundary to notice. It was
//     closed first, on its own.
//
// The consequence the old note recorded — that a rule's TEST value and its
// FIRING value legitimately disagreed, one scoped and one not — was a genuinely
// bad property to ship: the endpoint an operator uses to check a rule reported
// something other than what the rule does.
//
// WHAT CLOSED IT. alert_rules has a `project` column (migration 127), so the
// timer now has a project without needing a request: evaluateAll stamps each
// rule's OWN project onto the context before evaluating it, and every aggregate
// underneath is scoped by scope.ProjectPredicate exactly as the HTTP path
// already was. The two paths now build the same statement for the same rule,
// which alerts/evaluator_test.go pins directly.
//
// THE FALL-THROUGH IS GONE, and that is the part worth defending. buildAggQuery
// used to swallow scope.ErrNoProject and continue unscoped, which meant an
// unscoped aggregate was reachable by simply not having a project. It now
// propagates, so an unscoped aggregate is UNCONSTRUCTABLE — a future caller that
// forgets the project gets an error on the first evaluation instead of every
// tenant's numbers, which is the same property scope.ProjectPredicate's own
// header claims for the event reads.
//
// listEnabledRules stays UNSCOPED on purpose and is not a hole in this: the
// evaluator must run every project's rules, and the tenancy is applied per rule
// one layer down. query.ListEnabledAlertRules carries that argument in full.
//
// "project" also remains in structs.FilterColumns, so a rule can still narrow
// itself further with a query_filters entry — it can no longer WIDEN itself,
// because the predicate is ANDed on top.

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
	for i := range rules {
		rule := rules[i]
		interval := time.Duration(rule.EvaluationIntervalSecs) * time.Second
		if last, ok := e.lastEvaluated[rule.ID]; ok && interval > 0 && now.Sub(last) < interval {
			continue
		}

		// THE ONE LINE THAT CLOSES THE ZONE-WIDE GAP. Run's context is a
		// background one with no project, so without this every aggregate below
		// counted the whole zone. The rule's own column is the authority here —
		// there is no credential on this path to derive anything else from — and
		// it is NOT NULL (migration 127), so a rule that reached this loop always
		// carries one. Should it ever be empty, ProjectPredicate refuses and the
		// rule fails loudly rather than quietly counting every tenant.
		//
		// Stamped PER RULE, not once for the loop: two rules in one pass
		// routinely belong to different projects.
		ruleCtx := scope.WithProject(ctx, rule.Project)

		e.evaluateRule(ruleCtx, &rule)
		e.lastEvaluated[rule.ID] = now
	}
}

// EvaluateRuleNow evaluates a single rule and returns the current value and
// whether it is firing (for the test endpoint). It branches on rule.Type just
// like the live evaluator.
//
// The context's project is used AS IT ARRIVES, and is deliberately not
// overwritten with rule.Project the way the timer does it. The two look
// interchangeable and are not: this is an HTTP path, and on an HTTP path the
// CREDENTIAL decides what may be read, never a field of the row being read. They
// agree today because routes.HandleTestAlertRule loads the rule through a
// project-scoped GetRule, so rule.Project is the caller's project by
// construction — and if a future caller ever loads a rule some other way, the
// credential still wins here rather than the row handing itself a wider scope.
func EvaluateRuleNow(ctx context.Context, rule *structs.AlertRule) (value float64, isFiring bool, err error) {
	return evaluateRuleState(ctx, rule)
}

func (e *Evaluator) evaluateRule(ctx context.Context, rule *structs.AlertRule) {
	value, isFiring, err := evaluateRuleState(ctx, rule)
	if err != nil {
		log.Printf("alert evaluator: failed to query rule %s (%s): %v", rule.ID, rule.Name, err)
		return
	}

	state, _ := GetState(ctx, rule.Project, rule.ID)

	now := time.Now().UTC()

	if state == nil {
		state = &State{
			RuleID:    rule.ID,
			Project:   rule.Project,
			Status:    "ok",
			Value:     value,
			UpdatedAt: now,
		}
	}

	// Re-stamped on the read path too, not only when the state is new. A row
	// written before migration 007 comes back with an empty project, and writing
	// it straight back would keep it invisible to every non-default project
	// forever — one evaluation is all it takes to repair, and this is where it
	// happens.
	state.Project = rule.Project

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
		Project:  rule.Project,
		RuleID:   rule.ID,
		RuleName: rule.Name,
		Status:   "firing",
		Value:    state.Value,
		Message:  msg,
	})

	if e.alertHub != nil {
		e.alertHub.PublishStateChange(rule.Project, rule.ID, rule.Name, "firing", msg, state.Value)
	}

	alertCtx := BuildAlertContext(ctx, rule, "firing", state.Value, msg)
	// The destination count is Route's own business to log — it emits the WARN
	// when it is zero, where it knows which channels resolved and which did not.
	if _, err := e.router.Route(ctx, alertCtx, rule); err != nil {
		log.Printf("alert evaluator: routing failed for rule %s: %v", rule.ID, err)
	}
}

func (e *Evaluator) onResolved(ctx context.Context, rule *structs.AlertRule, state *State) {
	now := time.Now().UTC()
	state.LastNotifiedAt = &now

	msg := fmt.Sprintf("Alert '%s' has resolved: value %.2f", rule.Name, state.Value)

	_ = RecordHistory(ctx, HistoryEntry{
		Project:  rule.Project,
		RuleID:   rule.ID,
		RuleName: rule.Name,
		Status:   "resolved",
		Value:    state.Value,
		Message:  msg,
	})

	if e.alertHub != nil {
		e.alertHub.PublishStateChange(rule.Project, rule.ID, rule.Name, "resolved", msg, state.Value)
	}

	alertCtx := BuildAlertContext(ctx, rule, "resolved", state.Value, msg)
	if _, err := e.router.Route(ctx, alertCtx, rule); err != nil {
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
// through a function whose next act is to hit ClickHouse. A test can call it
// once as the timer reaches it and once as the test endpoint does, and compare
// the two statements — which is the regression guard on the closed gap above,
// and which no assertion about a returned float64 could express.
//
// aggExpr is a trusted, code-built expression; the rule's filters are bound.
func buildAggQuery(ctx context.Context, rule *structs.AlertRule, aggExpr string, from, to time.Time) (string, []interface{}, error) {
	filters, err := parseRuleFilters(rule)
	if err != nil {
		return "", nil, err
	}

	sql := fmt.Sprintf("SELECT %s AS value FROM %s.events WHERE timestamp >= ? AND timestamp <= ?", aggExpr, db.Database)
	args := []interface{}{from, to}

	// UNCONDITIONAL. There used to be an `errors.Is(err, scope.ErrNoProject)`
	// arm here that swallowed the error and continued zone-wide, because the
	// timer had no project to offer. It has one now (evaluateAll stamps
	// rule.Project), so the arm's only remaining effect would be to make an
	// unscoped aggregate reachable by forgetting to supply a project — which is
	// precisely the failure the sentinel exists to prevent. Propagating it makes
	// the unscoped read UNCONSTRUCTABLE rather than merely unusual.
	//
	// Attached BEFORE the rule's own filters, not after: these are positional
	// `?` placeholders, so the args have to be appended in the order their
	// placeholders appear in the text. Splicing this in below the loop would
	// slide every filter's binding one position along — valid SQL, no error,
	// wrong rows.
	predicate, scopeArgs, err := scope.ProjectPredicate(ctx)
	if err != nil {
		return "", nil, err
	}
	sql += " AND " + predicate
	args = append(args, scopeArgs...)

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
// The tenancy predicate is ALWAYS attached — see the closed note at the top of
// this file. A context with no project produces an error here rather than a
// zone-wide read, on both the timer and the HTTP paths.
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
// the one call site because it is the seam that decides whether the evaluator
// sees every project's rules. It reads ALL of them, across every project, and
// evaluateAll scopes each one individually before it is evaluated — see the
// closed note above and query.ListEnabledAlertRules for why the LISTING itself
// must stay unscoped while the EVALUATION must not.
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
