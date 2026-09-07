package alerts

import (
	"context"
	"fmt"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
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
// KNOWN INCONSISTENCY, recorded rather than left to be found: the two tables
// below are still created by an ad-hoc CREATE TABLE at boot, with no migration
// file, which is exactly the pattern migration 119 condemns for the six that
// moved. They are the last two in the repo created that way. Giving them files
// means a ClickHouse migration (migrations/007…), which is a different runner
// with its own re-runnability story, and it is a separate change.

// State represents the current state of an alert rule. ClickHouse
// (alert_states), one row per rule, rewritten on every evaluation.
type State struct {
	RuleID         string     `json:"rule_id"`
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
	ID        string    `json:"id"`
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
	err := db.Conn.Exec(ctx, `
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

	// The TTL is deliberate and must survive any edit to this statement: alert
	// history is unbounded otherwise, one row per transition per rule forever.
	// It is also the single strongest reason this table stayed in ClickHouse —
	// MariaDB has no expression for it.
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

	if seedPolicies {
		if err := query.SeedDefaultNotificationPolicies(db.SQL); err != nil {
			return fmt.Errorf("failed to seed default notification policies: %w", err)
		}
	}

	return nil
}

// ListRules returns every rule with its current state.
//
// Cross-store: the rules come from MariaDB and the states from ClickHouse, in
// two queries rather than N+1. A failed state fetch is non-fatal — the rules are
// still returned, with nil states — because the configuration is the answer the
// caller asked for and the state is decoration on it.
func ListRules(ctx context.Context) ([]RuleWithState, error) {
	ruleList, err := query.ListAlertRules(db.SQL)
	if err != nil {
		return nil, err
	}

	stateMap, err := ListAllStates(ctx)
	if err != nil {
		stateMap = make(map[string]*State)
	}

	rules := make([]RuleWithState, 0, len(ruleList))
	for _, r := range ruleList {
		rules = append(rules, RuleWithState{AlertRule: r, State: stateMap[r.ID]})
	}
	return rules, nil
}

// GetRule returns one rule with its current state.
//
// A missing rule is an ERROR rather than (nil, nil), matching what the handler
// has always turned into a 404. query.GetAlertRule is the layer that reports
// absence as absence; this one is the API's opinion about it.
func GetRule(ctx context.Context, id string) (*RuleWithState, error) {
	rule, err := query.GetAlertRule(db.SQL, id)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, fmt.Errorf("alert rule not found")
	}
	state, _ := GetState(ctx, id)
	return &RuleWithState{AlertRule: *rule, State: state}, nil
}

// DeleteRule removes a rule from MariaDB and its state row from ClickHouse.
//
// The two stores are why this is not a bare call to query.DeleteAlertRule. The
// state cleanup keeps its original best-effort handling: a leftover alert_states
// row is orphaned data that nothing reads (ListAllStates is only ever keyed by a
// rule that exists), so failing the delete over it would refuse a request that
// actually succeeded at the part that matters.
func DeleteRule(ctx context.Context, id string) error {
	if _, err := query.DeleteAlertRule(db.SQL, id); err != nil {
		return err
	}

	_ = db.Conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.alert_states DELETE WHERE rule_id = ?", db.Database,
	), id)

	return nil
}

// ListAllStates returns all alert states as a map keyed by rule_id.
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

// GetState returns the current state for a rule.
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

// UpsertState inserts or updates alert state (ReplacingMergeTree handles dedup).
func UpsertState(ctx context.Context, s *State) error {
	return db.Conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.alert_states (rule_id, status, value, fired_at, resolved_at, last_notified_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
		db.Database,
	), s.RuleID, s.Status, s.Value, s.FiredAt, s.ResolvedAt, s.LastNotifiedAt, s.UpdatedAt)
}

// RecordHistory records an alert event in history.
func RecordHistory(ctx context.Context, entry HistoryEntry) error {
	entry.ID = uuid.New().String()
	entry.CreatedAt = time.Now().UTC()

	return db.Conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.alert_history (id, rule_id, rule_name, status, value, message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
		db.Database,
	), entry.ID, entry.RuleID, entry.RuleName, entry.Status, entry.Value, entry.Message, entry.CreatedAt)
}

// ListHistory returns alert history entries.
func ListHistory(ctx context.Context, ruleID string, limit, offset int) ([]HistoryEntry, error) {
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

	// Named `q`, not `query`: this package now imports the query package, and a
	// local of that name shadows it. Harmless here and a compile error the next
	// time somebody adds a query.* call inside this function.
	var q string
	var args []interface{}

	if ruleID != "" {
		q = fmt.Sprintf(
			"SELECT id, rule_id, rule_name, status, value, message, created_at FROM %s.alert_history WHERE rule_id = ? ORDER BY created_at DESC LIMIT %d OFFSET %d",
			db.Database, limit, offset,
		)
		args = append(args, ruleID)
	} else {
		q = fmt.Sprintf(
			"SELECT id, rule_id, rule_name, status, value, message, created_at FROM %s.alert_history ORDER BY created_at DESC LIMIT %d OFFSET %d",
			db.Database, limit, offset,
		)
	}

	rows, err := db.Conn.Query(ctx, q, args...)
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
