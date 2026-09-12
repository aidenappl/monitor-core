package alerts

import (
	"context"
	"fmt"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/google/uuid"
)

// WHAT IS LEFT IN THIS FILE, AND WHY IT IS ONLY THIS.
//
// The alerting CONFIGURATION — rules, notification channels, routing policies and
// service groups — moved to MariaDB in migrations 119-122 and its SQL now lives
// in query/*.query.go. What remains here is the half that did not move:
//
//   * alert_states and alert_history, which stay in ClickHouse. They are
//     per-evaluation FACTS: a state row is rewritten by a timer every 15 seconds
//     and a history row is appended on every transition, so they are time-series
//     shaped in the way issue occurrences are, and alert_history carries a 90-day
//     TTL that has no MariaDB equivalent short of a scheduled DELETE. Migration
//     119's header states the split in full.
//   * ListRules, GetRule and DeleteRule, which are the three operations that
//     touch BOTH stores. They are not a service layer over query/ — every other
//     rule operation goes straight from handler to query — they are the specific
//     cases where a MariaDB row and a ClickHouse row have to be read or written
//     together, which is the one situation the house rules keep a layer for.
//
// CLOSED — the ad-hoc CREATE TABLE gap. Both tables were created by a bare
// CREATE TABLE inside Init at boot, with no migration file, which is the pattern
// migration 119 condemns for the six that moved. They now have one:
// migrations/007_alert_tables.sql, carrying the same DDL plus the `project`
// column, and Init no longer issues any DDL of its own. The separate change that
// note was waiting for is this one — the project column could not be added to a
// statement that did not exist in a migration.

// State represents the current state of an alert rule. ClickHouse
// (alert_states), one row per rule, rewritten on every evaluation.
type State struct {
	RuleID string `json:"rule_id"`
	// Project is the tenant whose rule this state belongs to, copied from the
	// rule row. alert_states is keyed by rule_id alone, so this column is not
	// needed to find the row — it is what lets a read REFUSE one, and what stops
	// an id lifted from a URL being turned into another tenant's alert status.
	Project        string     `json:"project"`
	Status         string     `json:"status"`
	Value          float64    `json:"value"`
	FiredAt        *time.Time `json:"fired_at,omitempty"`
	ResolvedAt     *time.Time `json:"resolved_at,omitempty"`
	LastNotifiedAt *time.Time `json:"last_notified_at,omitempty"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// HistoryEntry is one firing/resolved transition. ClickHouse (alert_history),
// append-only, expired by a 90-day TTL.
type HistoryEntry struct {
	ID string `json:"id"`
	// Project is the tenant this transition belongs to, copied from the rule.
	// GET /v1/alert-history was a zone-wide read of every rule's NAME and the
	// message built from it before this column existed.
	Project   string    `json:"project"`
	RuleID    string    `json:"rule_id"`
	RuleName  string    `json:"rule_name"`
	Status    string    `json:"status"`
	Value     float64   `json:"value"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

// RuleWithState is a rule joined to its current state — one row from MariaDB and
// one from ClickHouse. The embedded struct is what keeps the JSON flat, so the
// wire shape is identical to when both halves lived in one ClickHouse table.
type RuleWithState struct {
	structs.AlertRule
	State *State `json:"state,omitempty"`
	// HasDestinations reports whether a firing alert for this rule would reach
	// any notification channel at all — the policies that match it resolved
	// against the channels that actually exist, or its own
	// notification_channel_ids when no policy matches.
	//
	// It exists because the deployed instance was in exactly the state it makes
	// visible: six enabled rules, every one with `notification_channel_ids: []`,
	// evaluating on schedule and notifying nobody. Nothing in the API said so —
	// a rule with no destinations looked identical to a rule with them, and the
	// only evidence was the absence of notifications nobody was waiting for. The
	// router logs the same condition at WARN when it actually fires; this is the
	// half an operator can see BEFORE the incident.
	HasDestinations bool `json:"has_destinations"`
}

// projectPredicate builds the ClickHouse tenancy filter for an EXPLICIT project.
//
// It stamps the project onto the context and hands it to scope.ProjectPredicate
// rather than reading whatever the incoming context carries, because the project
// these reads must use is the one the CALLER resolved — requireProject in the
// handler, or the rule's own column on the evaluator's timer, which has no
// request and therefore no context project at all.
//
// Routing it through scope.ProjectPredicate keeps ONE implementation of the
// empty-string transition rule (see scope/scope.go). A second copy here would
// read pre-007 rows differently from every other ClickHouse read, and would
// diverge for good the day that arm is removed.
func projectPredicate(ctx context.Context, project string) (string, []interface{}, error) {
	return scope.ProjectPredicate(scope.WithProject(ctx, project))
}

// Init creates the two ClickHouse tables that did NOT move, and — when
// seedPolicies is true — seeds the default notification policies on a fresh
// install.
//
// The seeding is a MariaDB write and sits here rather than in bootstrap/ for one
// reason: bootstrap runs on the CONTROL plane and this runs on the DATA plane,
// and the alerting surface has always been data-plane. Moving the seed would
// change which processes create those four rows, which is precisely the kind of
// silent behaviour change this move must not make. It is called from main.go's
// data-plane block, after both migration runners.
//
// seedPolicies EXISTS BECAUSE OF A REAL INCIDENT, not as a convenience. An empty
// notification_policies table means two different things and they need opposite
// treatment: a genuinely fresh install (seed the defaults) versus an install
// whose policies are still sitting in ClickHouse awaiting the cutover (seed
// NOTHING). Seeding the second case puts four defaults at positions 1-4 and the
// operator's real policies land at 5-8 behind them — and because matching is
// first-past-the-post by position, the defaults then govern every route while
// the real policies are never reached. Nothing errors; alerting is simply wrong.
// main.go passes false whenever the cutover guard is unsatisfied.
func Init(ctx context.Context, seedPolicies bool) error {
	// NO DDL HERE ANY MORE. alert_states and alert_history are created by
	// migrations/007_alert_tables.sql, which runs before this in main.go's
	// data-plane block. Keeping a second copy of the CREATE here would be two
	// definitions of one table that drift — and the `project` column this change
	// adds is exactly the kind of thing that would land in only one of them.
	//
	// ctx is still taken because Init is a boot step whose next addition will
	// almost certainly need one, and because every caller already holds one.
	_ = ctx

	if seedPolicies {
		// SEEDED FOR THE DEFAULT PROJECT ONLY, because that is the only project
		// this boot step can name. env.DefaultProjectSlug is what migration 129
		// backfilled every existing policy to and what an unconfigured install
		// stamps on ingest, so it is the project a fresh install's operator is
		// actually looking at.
		//
		// A project created LATER gets no policies from here and needs none: the
		// seeded set is written disabled (see
		// query.SeedDefaultNotificationPolicies), so its only effect is to give
		// an operator four rows to fill in. A project without them falls back to
		// each rule's own notification_channel_ids, which is the same behaviour.
		if err := query.SeedDefaultNotificationPolicies(db.SQL, env.DefaultProjectSlug); err != nil {
			return fmt.Errorf("failed to seed default notification policies: %w", err)
		}
	}

	return nil
}

// ListRules returns one project's rules with their current state and whether
// each one has anywhere to send a notification.
//
// Cross-store: the rules come from MariaDB and the states from ClickHouse, in
// two queries rather than N+1. A failed state fetch is non-fatal — the rules are
// still returned, with nil states — because the configuration is the answer the
// caller asked for and the state is decoration on it.
//
// FIVE QUERIES REGARDLESS OF RULE COUNT, and that shape is deliberate. Working
// out has_destinations means matching each rule against the project's policies
// and then resolving the channel ids those name, which done per rule would be
// two round trips per row on a page that renders the whole alerting
// configuration. The policies, the channels and the service groups are each read
// ONCE and matched in memory by the pure helpers in router.go — the same helpers
// Route itself uses, so the badge cannot disagree with what actually happens
// when the rule fires.
func ListRules(ctx context.Context, project string) ([]RuleWithState, error) {
	ruleList, err := query.ListAlertRules(db.SQL, project)
	if err != nil {
		return nil, err
	}

	stateMap, err := ListAllStates(ctx, project)
	if err != nil {
		stateMap = make(map[string]*State)
	}

	// Best-effort, like the states: a rule's configuration is the answer, and a
	// failure to work out its routing must not cost the caller the listing. A
	// failed read leaves the maps empty, which reports has_destinations = false —
	// the conservative direction, since it points an operator at a rule to check
	// rather than reassuring them about one.
	policies, channels, groups := routingInputs(project)

	rules := make([]RuleWithState, 0, len(ruleList))
	for i := range ruleList {
		r := ruleList[i]
		alertCtx := buildAlertContextFrom(groups, &r, "firing", 0, "")
		rules = append(rules, RuleWithState{
			AlertRule:       r,
			State:           stateMap[r.ID],
			HasDestinations: countDestinations(alertCtx, &r, policies, channels) > 0,
		})
	}
	return rules, nil
}

// routingInputs loads the three project-scoped tables a destination count needs.
//
// Errors are swallowed on purpose and the caller is told so: this feeds an
// advisory badge, and the alternative — failing GET /v1/alert-rules because the
// service-groups table could not be read — trades a missing hint for a broken
// page. A read that fails yields an empty set, which reports "no destinations".
func routingInputs(project string) ([]structs.NotificationPolicy, map[string]bool, []structs.ServiceGroup) {
	policies, err := query.ListNotificationPolicies(db.SQL, project)
	if err != nil {
		policies = nil
	}

	known := map[string]bool{}
	if channels, err := query.ListNotificationChannels(db.SQL, project); err == nil {
		for _, ch := range channels {
			known[ch.ID] = true
		}
	}

	groups, err := query.ListServiceGroups(db.SQL, project)
	if err != nil {
		groups = nil
	}
	return policies, known, groups
}

// GetRule returns one rule with its current state.
//
// A missing rule is an ERROR rather than (nil, nil), matching what the handler
// has always turned into a 404. query.GetAlertRule is the layer that reports
// absence as absence; this one is the API's opinion about it.
func GetRule(ctx context.Context, project, id string) (*RuleWithState, error) {
	rule, err := query.GetAlertRule(db.SQL, project, id)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, fmt.Errorf("alert rule not found")
	}
	state, _ := GetState(ctx, project, id)

	// has_destinations is computed here too rather than left false. The field is
	// on the embedded shape, so it serialises on this endpoint whatever happens,
	// and a hardcoded false on the single-rule view would be a worse lie than an
	// absent field — it is the view an operator opens to check exactly this.
	policies, channels, groups := routingInputs(project)
	alertCtx := buildAlertContextFrom(groups, rule, "firing", 0, "")

	return &RuleWithState{
		AlertRule:       *rule,
		State:           state,
		HasDestinations: countDestinations(alertCtx, rule, policies, channels) > 0,
	}, nil
}

// DeleteRule removes a rule from MariaDB and its state row from ClickHouse.
//
// The two stores are why this is not a bare call to query.DeleteAlertRule. The
// state cleanup keeps its original best-effort handling: a leftover alert_states
// row is orphaned data that nothing reads (ListAllStates is only ever keyed by a
// rule that exists), so failing the delete over it would refuse a request that
// actually succeeded at the part that matters.
func DeleteRule(ctx context.Context, project, id string) error {
	if _, err := query.DeleteAlertRule(db.SQL, project, id); err != nil {
		return err
	}

	// The mutation carries the project too, for the reason the MariaDB deletes
	// do: it must not depend on the statement above having been the one that
	// decided which tenant this id belongs to. A predicate that cannot be built
	// aborts the cleanup rather than widening it — the leftover state row is
	// orphaned data nothing reads, which is the failure this call already treats
	// as acceptable.
	predicate, args, err := projectPredicate(ctx, project)
	if err != nil {
		return nil
	}
	_ = db.Conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.alert_states DELETE WHERE rule_id = ? AND %s", db.Database, predicate,
	), append([]interface{}{id}, args...)...)

	return nil
}

// ListAllStates returns one project's alert states as a map keyed by rule_id.
//
// FINAL is required and stays: alert_states is a ReplacingMergeTree rewritten
// every 15 seconds per rule, so without it a read returns however many versions
// a background merge has not yet collapsed.
func ListAllStates(ctx context.Context, project string) (map[string]*State, error) {
	predicate, args, err := projectPredicate(ctx, project)
	if err != nil {
		return nil, err
	}

	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT rule_id, project, status, value, fired_at, resolved_at, last_notified_at, updated_at FROM %s.alert_states FINAL WHERE %s",
		db.Database, predicate,
	), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list alert states: %w", err)
	}
	defer rows.Close()

	stateMap := make(map[string]*State)
	for rows.Next() {
		var s State
		if err := rows.Scan(&s.RuleID, &s.Project, &s.Status, &s.Value, &s.FiredAt, &s.ResolvedAt, &s.LastNotifiedAt, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan alert state: %w", err)
		}
		stateMap[s.RuleID] = &s
	}
	return stateMap, nil
}

// GetState returns the current state for a rule within a project.
func GetState(ctx context.Context, project, ruleID string) (*State, error) {
	predicate, args, err := projectPredicate(ctx, project)
	if err != nil {
		return nil, err
	}

	row := db.Conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT rule_id, project, status, value, fired_at, resolved_at, last_notified_at, updated_at FROM %s.alert_states FINAL WHERE rule_id = ? AND %s",
		db.Database, predicate,
	), append([]interface{}{ruleID}, args...)...)

	var s State
	if err := row.Scan(&s.RuleID, &s.Project, &s.Status, &s.Value, &s.FiredAt, &s.ResolvedAt, &s.LastNotifiedAt, &s.UpdatedAt); err != nil {
		return nil, err
	}
	return &s, nil
}

// UpsertState inserts or updates alert state (ReplacingMergeTree handles dedup).
//
// The project comes off the State, which the evaluator copies from the rule. It
// is written on EVERY evaluation, which is why alert_states needs no manual
// backfill the way alert_history does: the timer rewrites every row of every
// enabled rule within one evaluation interval, and the ReplacingMergeTree
// collapses the unstamped version away.
func UpsertState(ctx context.Context, s *State) error {
	return db.Conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.alert_states (rule_id, project, status, value, fired_at, resolved_at, last_notified_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		db.Database,
	), s.RuleID, s.Project, s.Status, s.Value, s.FiredAt, s.ResolvedAt, s.LastNotifiedAt, s.UpdatedAt)
}

// RecordHistory records an alert event in history.
//
// entry.Project is set by the caller from the RULE's own column, not read off
// the context: this is called from the evaluator's timer goroutine, and the rule
// row is the only thing there that knows whose alert it is. The column is NOT
// NULL in MariaDB, so a rule that reached this point always has one — an empty
// value here would mean a row visible only to the default project through
// scope.ProjectPredicate's transition arm.
func RecordHistory(ctx context.Context, entry HistoryEntry) error {
	entry.ID = uuid.New().String()
	entry.CreatedAt = time.Now().UTC()

	return db.Conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.alert_history (id, project, rule_id, rule_name, status, value, message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		db.Database,
	), entry.ID, entry.Project, entry.RuleID, entry.RuleName, entry.Status, entry.Value, entry.Message, entry.CreatedAt)
}

// ListHistory returns one project's alert history entries.
//
// GET /v1/alert-history was a ZONE-WIDE read before this. The rows are not
// metadata: rule_name is operator-written and routinely names a service, a
// customer or an environment, and `message` is the sentence the evaluator built
// from it — "Alert 'payments 5xx spike' is firing: value 412.00 gt threshold
// 50.00". That is another tenant's incident log, served through the page an
// operator opens to review their own.
func ListHistory(ctx context.Context, project, ruleID string, limit, offset int) ([]HistoryEntry, error) {
	// Clamped exactly as before, now spelled with the shared constants: an
	// absent or negative limit defaults, an oversized one is TRIMMED to the
	// maximum rather than reset to the default. The distinction is visible to a
	// caller asking for 600 and is preserved on purpose.
	if limit <= 0 {
		limit = db.DEFAULT_LIMIT
	}
	if limit > db.MAX_LIMIT {
		limit = db.MAX_LIMIT
	}

	predicate, args, err := projectPredicate(ctx, project)
	if err != nil {
		return nil, err
	}

	// The project predicate comes FIRST and rule_id second, because both bind
	// positionally into one statement — appending the project after the optional
	// rule_id would make the argument order depend on whether the caller passed
	// a rule, which is the kind of thing that is right in testing and wrong in
	// production. LIMIT and OFFSET stay interpolated rather than bound, as
	// before: they are ints this function has already clamped.
	//
	// Named `q`, not `query`: this package imports the query package, and a
	// local of that name shadows it. Harmless here and a compile error the next
	// time somebody adds a query.* call inside this function.
	q := fmt.Sprintf(
		"SELECT id, project, rule_id, rule_name, status, value, message, created_at FROM %s.alert_history WHERE %s",
		db.Database, predicate,
	)
	if ruleID != "" {
		q += " AND rule_id = ?"
		args = append(args, ruleID)
	}
	q += fmt.Sprintf(" ORDER BY created_at DESC LIMIT %d OFFSET %d", limit, offset)

	rows, err := db.Conn.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list alert history: %w", err)
	}
	defer rows.Close()

	var entries []HistoryEntry
	for rows.Next() {
		var e HistoryEntry
		if err := rows.Scan(&e.ID, &e.Project, &e.RuleID, &e.RuleName, &e.Status, &e.Value, &e.Message, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan alert history: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, nil
}
