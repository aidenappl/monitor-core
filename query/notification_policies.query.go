package query

import (
	"database/sql"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/google/uuid"
)

const notificationPoliciesTable = "monitor.notification_policies"

// notificationPoliciesProjectColumn is the one string every scoped read of this
// table binds against. Named once because it appears in two builders and, in
// spelled-out form, in three hand-written statements below.
const notificationPoliciesProjectColumn = "monitor.notification_policies.project"

var notificationPolicyColumns = []string{
	"monitor.notification_policies.id",
	"monitor.notification_policies.project",
	"monitor.notification_policies.name",
	"monitor.notification_policies.description",
	"monitor.notification_policies.position",
	"monitor.notification_policies.matchers",
	"monitor.notification_policies.channel_ids",
	"monitor.notification_policies.continue_matching",
	"monitor.notification_policies.repeat_interval_seconds",
	"monitor.notification_policies.enabled",
	"monitor.notification_policies.is_default",
	"monitor.notification_policies.created_at",
	"monitor.notification_policies.updated_at",
}

var notificationPolicyInsertColumns = []string{
	"id", "project", "name", "description", "position", "matchers", "channel_ids",
	"continue_matching", "repeat_interval_seconds", "enabled", "is_default",
	"created_at", "updated_at",
}

// txBeginner is satisfied by *sql.DB and not by *sql.Tx, which is exactly the
// distinction the two multi-statement writes in this file need. It mirrors the
// anonymous interface bootstrap/admin.go asserts for the same purpose.
type txBeginner interface {
	Begin() (*sql.Tx, error)
}

type notificationPolicyScanner interface {
	Scan(dest ...interface{}) error
}

func scanNotificationPolicy(row notificationPolicyScanner) (*structs.NotificationPolicy, error) {
	var p structs.NotificationPolicy
	if err := row.Scan(
		&p.ID, &p.Project, &p.Name, &p.Description, &p.Position, &p.Matchers, &p.ChannelIDs,
		&p.ContinueMatching, &p.RepeatIntervalSeconds, &p.Enabled, &p.IsDefault,
		&p.CreatedAt, &p.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return &p, nil
}

// ListNotificationPolicies returns one project's policies in evaluation order.
//
// The ORDER BY is the routing order alerts/router.go depends on, not a display
// preference: it walks this slice top-down and stops at the first match unless
// the policy sets continue_matching. `id ASC` is the tiebreaker, and migration
// 129 is the change that made it REACHABLE again rather than dead: the unique key
// is (project, position) now, so two policies in different projects legitimately
// share a position and only the id keeps this listing deterministic across them.
// Within one project the key still forbids a tie.
//
// Scoping this is the load-bearing half of 129. Unscoped, a project's alert
// routing was decided by every project's policies interleaved by position — the
// first match wins and continue_matching is false by default, so another
// tenant's policy at position 1 silently captured this tenant's alerts.
func ListNotificationPolicies(engine db.Queryable, project string) ([]structs.NotificationPolicy, error) {
	q := sq.Select(notificationPolicyColumns...).From(notificationPoliciesTable).
		OrderBy("monitor.notification_policies.position ASC", "monitor.notification_policies.id ASC")

	q, err := scopeAlerting(q, notificationPoliciesProjectColumn, project)
	if err != nil {
		return nil, err
	}

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	rows, err := engine.Query(qStr, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql query: %w", err)
	}
	defer rows.Close()

	policies := []structs.NotificationPolicy{}
	for rows.Next() {
		p, err := scanNotificationPolicy(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan notification policy: %w", err)
		}
		policies = append(policies, *p)
	}
	return policies, rows.Err()
}

// GetNotificationPolicy returns one policy by id within a project, or (nil, nil)
// when there is none. The project is part of the lookup, so another tenant's
// policy reads as ABSENT rather than as forbidden.
func GetNotificationPolicy(engine db.Queryable, project, id string) (*structs.NotificationPolicy, error) {
	q := sq.Select(notificationPolicyColumns...).From(notificationPoliciesTable).
		Where(sq.Eq{"monitor.notification_policies.id": id}).Limit(1)

	q, err := scopeAlerting(q, notificationPoliciesProjectColumn, project)
	if err != nil {
		return nil, err
	}

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	p, err := scanNotificationPolicy(engine.QueryRow(qStr, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan notification policy: %w", err)
	}
	return p, nil
}

// CreateNotificationPolicyRequest is the POST /v1/notification-policies body.
//
// IsDefault is settable by a caller, which is carried over from the ClickHouse
// implementation and is a foot-gun worth naming: DeleteNotificationPolicy
// refuses to delete a default, so a policy created with `"is_default": true` can
// never be removed through the API. It is preserved rather than fixed because
// this change is a store move; closing it is a one-line drop of the field.
type CreateNotificationPolicyRequest struct {
	Name                  string `json:"name"`
	Description           string `json:"description"`
	Matchers              string `json:"matchers"`
	ChannelIDs            string `json:"channel_ids"`
	ContinueMatching      bool   `json:"continue_matching"`
	RepeatIntervalSeconds uint32 `json:"repeat_interval_seconds"`
	Enabled               bool   `json:"enabled"`
	IsDefault             bool   `json:"is_default"`
}

// CreateNotificationPolicy appends one policy to the end of the routing order.
//
// THE POSITION IS ALLOCATED UNDER A LOCK, inside the same transaction as the
// INSERT. The ClickHouse version read `max(position)` and added one in Go, so
// two concurrent creates read the same maximum and both wrote it — and nothing
// in a ReplacingMergeTree could object. Migration 121's UNIQUE key would now
// turn that race into a duplicate-key error rather than a silent duplicate,
// which is better but is still a failed request; taking the locking read and the
// insert together makes the second caller wait and get the next position.
//
// The one case the lock cannot cover is an EMPTY table: there is no row to lock,
// so two creates racing on a fresh install can both compute position 1 and one
// will fail on the unique key. That is a loud, retryable failure of a request
// nobody is making concurrently on a fresh install, and closing it properly
// needs a sentinel row this table has no other use for.
// The position is allocated WITHIN THE PROJECT (see nextPolicyPosition), which is
// what migration 129 bought: before it, creating a policy here took the next slot
// in a zone-global sequence and every other project's routing order moved.
func CreateNotificationPolicy(engine db.Queryable, project string, req CreateNotificationPolicyRequest) (*structs.NotificationPolicy, error) {
	// Checked first because the column is NOT NULL with no default (migration
	// 129): an empty project reaches MariaDB as errno 1364 naming the column,
	// where this names the request that had no tenant.
	if project == "" {
		return nil, ErrNoAlertingProject
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	now := time.Now().UTC()
	p := structs.NotificationPolicy{
		ID:                    uuid.New().String(),
		Project:               project,
		Name:                  req.Name,
		Description:           req.Description,
		Matchers:              req.Matchers,
		ChannelIDs:            req.ChannelIDs,
		ContinueMatching:      req.ContinueMatching,
		RepeatIntervalSeconds: req.RepeatIntervalSeconds,
		Enabled:               req.Enabled,
		IsDefault:             req.IsDefault,
		CreatedAt:             now,
		UpdatedAt:             now,
	}
	if p.Matchers == "" {
		p.Matchers = "{}"
	}
	if p.ChannelIDs == "" {
		p.ChannelIDs = "[]"
	}
	if err := requireJSONText("matchers", p.Matchers); err != nil {
		return nil, err
	}
	if err := requireJSONText("channel_ids", p.ChannelIDs); err != nil {
		return nil, err
	}

	beginner, ok := engine.(txBeginner)
	if !ok {
		// A *sql.Tx handed in by a caller that is already managing its own
		// transaction. Allocate and insert on it directly — the caller's
		// transaction is the isolation, so this is not the unprotected path.
		return insertNotificationPolicy(engine, p)
	}

	tx, err := beginner.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin notification policy create: %w", err)
	}
	created, err := insertNotificationPolicy(tx, p)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit notification policy create: %w", err)
	}
	return created, nil
}

func insertNotificationPolicy(engine db.Queryable, p structs.NotificationPolicy) (*structs.NotificationPolicy, error) {
	pos, err := nextPolicyPosition(engine, p.Project)
	if err != nil {
		return nil, err
	}
	p.Position = pos

	qStr, args, err := sq.Insert(notificationPoliciesTable).
		Columns(notificationPolicyInsertColumns...).
		Values(p.ID, p.Project, p.Name, p.Description, p.Position, p.Matchers, p.ChannelIDs,
			p.ContinueMatching, p.RepeatIntervalSeconds, p.Enabled, p.IsDefault,
			p.CreatedAt, p.UpdatedAt).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to insert notification policy: %w", err)
	}
	return &p, nil
}

// nextPolicyPosition reads the highest position under a row lock and returns the
// one after it.
//
// It is a plain `ORDER BY … LIMIT 1 FOR UPDATE` rather than `SELECT MAX(position)
// … FOR UPDATE` on purpose: a locking read of one ordinary row is unambiguously
// permitted, whereas locking clauses combined with aggregation are a corner of
// the manual that is not worth betting a write path on. Same answer, no doubt.
// THE LOCK IS NOW PER PROJECT, which is both narrower and stronger. Narrower
// because a create in one project no longer blocks a create in another. Stronger
// because the number it returns is the next free slot in THIS project's routing
// order — under the old zone-global read, a project whose highest position was 2
// got position 9 because some other tenant had nine policies, leaving gaps that
// made the ordering unreadable.
func nextPolicyPosition(engine db.Queryable, project string) (int, error) {
	const q = "SELECT position FROM monitor.notification_policies WHERE project = ? ORDER BY position DESC LIMIT 1 FOR UPDATE"

	var max int
	err := engine.QueryRow(q, project).Scan(&max)
	if err == sql.ErrNoRows {
		return 1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to determine notification policy position: %w", err)
	}
	return max + 1, nil
}

// UpdateNotificationPolicyRequest is the PUT /v1/notification-policies/{id} body.
//
// KNOWN WART, PRESERVED DELIBERATELY: the string and numeric fields are
// "non-empty wins" (an omitted name keeps the old name, and repeat_interval
// cannot be set back to 0), while ContinueMatching and Enabled are applied
// UNCONDITIONALLY — so omitting `enabled` DISABLES the policy. That is exactly
// the bug UpdateAlertRuleRequest fixed for rules by using pointers, and
// alerts/AGENTS.md has carried it as an open finding since. It is reproduced
// here rather than fixed because this change is a store move and monitor-web
// sends the whole object; fixing it means the pointer treatment, which is a
// behaviour change that deserves its own commit.
//
// Position is absent on purpose. Ordering is changed only through
// ReorderNotificationPolicies, which is the one path that can satisfy migration
// 121's UNIQUE key.
type UpdateNotificationPolicyRequest struct {
	Name                  string `json:"name"`
	Description           string `json:"description"`
	Matchers              string `json:"matchers"`
	ChannelIDs            string `json:"channel_ids"`
	ContinueMatching      bool   `json:"continue_matching"`
	RepeatIntervalSeconds uint32 `json:"repeat_interval_seconds"`
	Enabled               bool   `json:"enabled"`
}

// UpdateNotificationPolicy applies the request to one policy within a project
// and returns it.
func UpdateNotificationPolicy(engine db.Queryable, project, id string, req UpdateNotificationPolicyRequest) (*structs.NotificationPolicy, error) {
	existing, err := GetNotificationPolicy(engine, project, id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("notification policy not found")
	}

	u := sq.Update(notificationPoliciesTable).
		Set("continue_matching", req.ContinueMatching).
		Set("enabled", req.Enabled)

	if req.Name != "" {
		u = u.Set("name", req.Name)
	}
	if req.Description != "" {
		u = u.Set("description", req.Description)
	}
	if req.Matchers != "" {
		if err := requireJSONText("matchers", req.Matchers); err != nil {
			return nil, err
		}
		u = u.Set("matchers", req.Matchers)
	}
	if req.ChannelIDs != "" {
		if err := requireJSONText("channel_ids", req.ChannelIDs); err != nil {
			return nil, err
		}
		u = u.Set("channel_ids", req.ChannelIDs)
	}
	if req.RepeatIntervalSeconds != 0 {
		u = u.Set("repeat_interval_seconds", req.RepeatIntervalSeconds)
	}

	// `project` is in the UPDATE's own WHERE rather than trusted from the read
	// above. A policy is a ROUTING instruction: retargeting another tenant's
	// policy at channels you control redirects their alerts to you, which is
	// worse than reading them. Read-then-write is not atomic and a future caller
	// may skip the read — the reason DeleteAPIKey records.
	qStr, args, err := u.Where(sq.Eq{"id": id, "project": project}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to update notification policy: %w", err)
	}
	return GetNotificationPolicy(engine, project, id)
}

// DeleteNotificationPolicy removes one policy. A default policy is refused.
//
// The vacated position is NOT closed up. Positions are an ORDER, not a rank, so
// a gap changes nothing about routing — and compacting would mean rewriting
// every following row under the unique key, i.e. a second reorder, on a delete.
func DeleteNotificationPolicy(engine db.Queryable, project, id string) error {
	if project == "" {
		return ErrNoAlertingProject
	}

	// The read is for the is_default REFUSAL, not for the tenancy check — the
	// project is in the DELETE's own WHERE below, so a caller that one day skips
	// this read still cannot delete across tenants.
	existing, err := GetNotificationPolicy(engine, project, id)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("notification policy not found")
	}
	if existing.IsDefault {
		return fmt.Errorf("cannot delete default policy")
	}

	qStr, args, err := sq.Delete(notificationPoliciesTable).
		Where(sq.Eq{"id": id, "project": project}).ToSql()
	if err != nil {
		return fmt.Errorf("failed to build sql query: %w", err)
	}
	if _, err := engine.Exec(qStr, args...); err != nil {
		return fmt.Errorf("failed to delete notification policy: %w", err)
	}
	return nil
}

// vacatePositionsSQL moves every policy OF ONE PROJECT into the negative half of
// the range in ONE statement, so that project's positive 1..N range is free for
// the assignment that follows.
//
// THIS IS THE WHOLE TRICK, and it is why structs.NotificationPolicy.Position is
// a signed int and migration 121 puts no CHECK on the column. InnoDB validates a
// unique index per ROW — MariaDB has no deferrable constraints — so assigning
// the new order directly would collide the moment any policy moves onto a
// position another policy has not yet vacated. Negation is injective and its
// image is disjoint from its domain (every stored position is >= 1), so this
// statement can never violate the key, and afterwards no positive position
// exists for the second phase to collide with.
//
// THE `WHERE project = ?` IS MANDATORY, and it is the one place in this file
// where omitting the predicate does not merely widen a read — it CORRUPTS other
// tenants. Without it, reordering project A negates every project's positions and
// phase two only reassigns A's, leaving every other project's policies sitting at
// negative positions forever: still ordered relative to each other, but ahead of
// A's in any `ORDER BY position`, and unfixable except by a manual rewrite.
//
// Negation stays injective under the narrower key (project, position) that
// migration 129 installs, because it never changes the project half of the pair.
// A negated row can therefore collide with nothing — not with its own project's
// remaining rows, which are all negative too by the time this statement returns,
// and not with another project's, which are a different key.
const vacatePositionsSQL = "UPDATE monitor.notification_policies SET position = -position WHERE project = ?"

// ReorderNotificationPolicies rewrites the routing order.
//
// The named ids are placed first, in the order given; every policy not named
// keeps its relative order and follows them. A partial list is therefore
// well-defined rather than rejected, and a full list — which is what
// monitor-web's drag-reorder sends — behaves exactly as it did before.
//
// ATOMIC, which the ClickHouse implementation could not be: it rewrote rows one
// at a time with no transaction, so a failure halfway through left the list half
// in the old order and half in the new, with duplicate positions by
// construction. Here every step runs in one transaction against the constraint
// that makes duplicates impossible, so the outcome is the new order or the old
// one and never a mixture.
//
// It REFUSES to run without a transaction rather than falling back to a
// sequential loop the way bootstrap.EnsureAdminUser does. The fallback there
// degrades to two writes that might leave an orphan; the fallback here would be
// the exact defect this function exists to remove.
func ReorderNotificationPolicies(engine db.Queryable, project string, orderedIDs []string) error {
	if project == "" {
		return ErrNoAlertingProject
	}
	if len(orderedIDs) == 0 {
		return fmt.Errorf("ids are required")
	}

	beginner, ok := engine.(txBeginner)
	if !ok {
		return fmt.Errorf("reordering notification policies requires a transaction-capable handle")
	}

	tx, err := beginner.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin notification policy reorder: %w", err)
	}
	if err := reorderNotificationPoliciesTx(tx, project, orderedIDs); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit notification policy reorder: %w", err)
	}
	return nil
}

func reorderNotificationPoliciesTx(tx *sql.Tx, project string, orderedIDs []string) error {
	// Phase 0 — take THIS PROJECT's list under lock, in its current order. This
	// is what keeps a concurrent create (which allocates max+1 within the same
	// project) or a second reorder from interleaving with the two rewrites below.
	//
	// The project predicate also decides what `final` contains, and that is the
	// tenancy check for the whole function: an id belonging to another tenant is
	// simply not in `known`, so the validation below rejects it as "not found"
	// rather than as forbidden — and it can never reach a statement.
	const lockAll = "SELECT id FROM monitor.notification_policies WHERE project = ? ORDER BY position ASC, id ASC FOR UPDATE"
	rows, err := tx.Query(lockAll, project)
	if err != nil {
		return fmt.Errorf("failed to read notification policy order: %w", err)
	}
	current := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan notification policy id: %w", err)
		}
		current = append(current, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("failed to read notification policy order: %w", err)
	}
	rows.Close()

	known := make(map[string]bool, len(current))
	for _, id := range current {
		known[id] = true
	}

	// A named id that does not exist, or is named twice, is rejected rather than
	// skipped. Silently dropping either would produce an order the caller did not
	// ask for and has no way to detect — the response is a success either way.
	named := make(map[string]bool, len(orderedIDs))
	final := make([]string, 0, len(current))
	for _, id := range orderedIDs {
		if !known[id] {
			return fmt.Errorf("policy %s not found", id)
		}
		if named[id] {
			return fmt.Errorf("policy %s listed more than once", id)
		}
		named[id] = true
		final = append(final, id)
	}
	for _, id := range current {
		if !named[id] {
			final = append(final, id)
		}
	}

	// Phase 1 — vacate this project's positive range (see vacatePositionsSQL).
	if _, err := tx.Exec(vacatePositionsSQL, project); err != nil {
		return fmt.Errorf("failed to stage notification policy positions: %w", err)
	}

	// Phase 2 — assign the new order. Every target is free.
	//
	// `AND project = ?` is redundant given `final` was built from the scoped
	// lock above, and it is here anyway: this is a mutation, and the rule the
	// rest of this file follows is that a mutation carries its own predicate
	// rather than inheriting one from a read that a later edit may move.
	for i, id := range final {
		if _, err := tx.Exec(
			"UPDATE monitor.notification_policies SET position = ? WHERE id = ? AND project = ?", i+1, id, project,
		); err != nil {
			return fmt.Errorf("failed to reorder policy %s: %w", id, err)
		}
	}
	return nil
}

// SeedDefaultNotificationPolicies inserts the four priority-routing defaults for
// ONE PROJECT on a fresh install.
//
// THE GATE IS "THIS PROJECT HAS NO POLICIES", not "the table is empty" and not
// "no default exists". Both earlier gates were proxies for "this is a fresh
// install", and under migration 129 the table holds every project's routing
// order at once — so a table-wide count would mean the FIRST project to be set
// up permanently blocks every later one from getting its defaults, and the later
// project would boot with an empty routing table and no sign that anything was
// skipped. cutover.BackfillConfig still copies the existing ClickHouse defaults
// across with their own ids, and a project that already holds those must not be
// seeded on top of — emptiness within the project says that directly.
//
// ⚠️ THE SEEDED POLICIES ARE WRITTEN `enabled = 0`, AND THAT IS THE POINT OF
// THIS FUNCTION'S EXISTENCE RATHER THAN A DETAIL OF IT.
//
// They ship with EMPTY channel_ids — they always have — and alerts/router.go
// treats "a policy matched" as reason enough to skip the rule's own
// notification_channel_ids fallback. Enabled, these four match EVERY alert by
// priority (P0/P1/P2/P3 covers the whole enum), carry continue_matching = false,
// and route to nowhere. So a project that was seeded and never edited does not
// merely lack routing — it actively SUPPRESSES the per-rule channels an operator
// did configure, and nothing errors or logs. That is exactly the shape of the
// live defect this change was written against: six enabled alert rules on the
// deployed instance, every one with `notification_channel_ids: []`, alerting
// evaluating correctly and notifying nobody.
//
// Two ways out were available and only one keeps the templates:
//
//   - DO NOT SEED non-default projects. Cheap, but it leaves the DEFAULT
//     project — the one every existing install actually runs — with the
//     suppressing set intact, and it makes the per-project gate above dead code
//     for every project it excludes. It fixes the tenancy question and none of
//     the routing one.
//   - SEED THEM DISABLED. A disabled policy is skipped by the router's
//     `if !policy.Enabled` continue, so policyMatched stays false and the rule's
//     own channel list is honoured — which is the behaviour an operator who has
//     configured channels and no policies expects. The four rows survive as a
//     visible, fillable template: set the channel ids, flip enabled, and the
//     routing table is what it always meant to be.
//
// The second is taken. It is the only one that also repairs the default project,
// and the endpoints it changes are ones where the previous behaviour was silent
// non-delivery rather than anything a caller could be depending on.
//
// EXISTING ROWS ARE NOT TOUCHED. The gate means this only ever runs for a
// project with no policies at all, so an install that already seeded four
// ENABLED defaults keeps them, along with whatever an operator has since put in
// their channel_ids.
//
// The four rows go in as ONE statement so the set is all-or-nothing, and so two
// processes booting together cannot interleave into a half-seeded list — the
// loser fails on the (project, position) unique key with nothing written.
func SeedDefaultNotificationPolicies(engine db.Queryable, project string) error {
	if project == "" {
		return ErrNoAlertingProject
	}

	var count int
	if err := engine.QueryRow(
		"SELECT COUNT(*) FROM monitor.notification_policies WHERE project = ?", project,
	).Scan(&count); err != nil {
		return fmt.Errorf("failed to count notification policies: %w", err)
	}
	if count > 0 {
		return nil
	}

	now := time.Now().UTC()
	defaults := []struct {
		name     string
		position int
		matchers string
	}{
		{"Critical (P0) — All Channels", 1, `{"priority":"P0"}`},
		{"High (P1) — PagerDuty + Email", 2, `{"priority":"P1"}`},
		{"Medium (P2) — Email Only", 3, `{"priority":"P2"}`},
		{"Low (P3) — Web Only", 4, `{"priority":"P3"}`},
	}

	insert := sq.Insert(notificationPoliciesTable).Columns(notificationPolicyInsertColumns...)
	for _, d := range defaults {
		// enabled = false, is_default = true. The `false` in the enabled slot is
		// the whole decision documented above — a template, not a live route.
		insert = insert.Values(uuid.New().String(), project, d.name, "", d.position, d.matchers,
			"[]", false, uint32(0), false, true, now, now)
	}

	qStr, args, err := insert.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build sql query: %w", err)
	}
	if _, err := engine.Exec(qStr, args...); err != nil {
		return fmt.Errorf("failed to seed default notification policies: %w", err)
	}
	return nil
}
