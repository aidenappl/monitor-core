// Package cutover holds the one-time ClickHouse → MariaDB copy for the six
// configuration tables that moved in migrations 119-124.
//
// IT IS ITS OWN PACKAGE SO THAT IT CAN BE DELETED IN ONE GESTURE. The issue
// backfill lives in issues/backfill.go, beside the subsystem it belongs to,
// because there is exactly one of it. This one spans three subsystems — alerts,
// dashboards, views — and two of those packages are now empty, so there is no
// "beside" to put it. Once a release has passed and the legacy ClickHouse tables
// are dropped, this directory goes with them and nothing else has to be untangled.
//
// It is reached as `monitor-core backfill-config`, following the backfill-issues
// precedent in main.go: an argv subcommand rather than a second package main,
// which the repo's AGENTS.md forbids, and rather than a boot step, because it is
// a cutover and not part of normal startup.
package cutover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/tools"
	"github.com/go-sql-driver/mysql"
)

// Report counts what one run did, per table.
//
// Copied and Skipped are reported separately because on a RE-RUN the interesting
// number is Skipped: it is the proof that the second run recognised the first
// one's work rather than duplicating it.
type Report struct {
	Table   string
	Copied  int
	Skipped int
}

// BackfillConfig copies the six configuration tables from ClickHouse to MariaDB.
//
// SAFE TO RE-RUN, and the rule that makes it safe is simple: a row whose id is
// ALREADY in MariaDB is SKIPPED ENTIRELY, never updated. Identity carries across
// unchanged — both stores key these tables on the same UUID string — so there is
// no id remapping and no ambiguity about which row is which.
//
// Skipping rather than upserting is the deliberate half. issues.BackfillFromClickHouse
// can re-run additively because its conflict rule is monotonic (the greater
// occurrence count wins, the seen-window only widens), so a second run cannot
// walk live data backwards. Nothing here is monotonic: these are rows a human
// edits. If a rule's threshold is corrected in Monitor after the cutover and this
// is then re-run, an upsert would silently restore the old threshold from a
// ClickHouse table nobody is writing to any more. A re-run is a REPAIR for rows
// that did not make it, not a re-import — the same instinct that made the issue
// backfill leave triage state alone.
//
// The consequence, stated so it is not discovered: this cannot be used to pull
// across an edit made in ClickHouse after the first run. There is no path that
// makes an edit there any more, which is the point.
//
// SAFE ALONGSIDE A RUNNING INSTANCE. It is safe beside the OLD binary, which
// reads ClickHouse and never touches these MariaDB tables, and safe beside the
// NEW one, because existing ids are skipped and positions are appended after the
// current maximum.
//
// SAFE IS NOT THE SAME AS CORRECT, AND THE ORDER STILL MATTERS. Run this BEFORE
// the new binary serves. Two things go wrong if it runs after:
//
//   - The evaluator has nothing to evaluate. The new binary reads alert_rules
//     from MariaDB, so until this has run every rule is simply absent — no error,
//     no alert, and no signal anywhere that alerting has stopped.
//   - The default notification policies double. alerts.Init seeds four defaults
//     into an empty policies table, and this then appends the four ClickHouse
//     originals after them (nothing collides — positions are renumbered from
//     MAX+1), leaving eight policies where four match every alert by priority and
//     route it nowhere. Recovery is to delete the four seeded rows by hand.
//
// AGENTS.md §8 carries the sequence.
//
// The ClickHouse tables are READ, never written and never dropped. Retire them
// manually once a release has passed — and note that doing so retires the ability
// to re-run this.
func BackfillConfig(ctx context.Context) ([]Report, error) {
	steps := []struct {
		name string
		run  func(context.Context) (Report, error)
	}{
		{"alert_rules", backfillAlertRules},
		{"notification_channels", backfillNotificationChannels},
		{"service_groups", backfillServiceGroups},
		{"notification_policies", backfillNotificationPolicies},
		{"dashboards", backfillDashboards},
		{"saved_views", backfillSavedViews},
	}

	// Order matters only for readability — there are no foreign keys between
	// these six tables, because every cross-reference they hold (a rule's
	// notification_channel_ids, a policy's channel_ids, a matcher's
	// service_group) lives inside a JSON blob. Nothing here can fail for want of
	// a row inserted by a later step.
	reports := make([]Report, 0, len(steps))
	for _, step := range steps {
		report, err := step.run(ctx)
		reports = append(reports, report)
		if err != nil {
			return reports, fmt.Errorf("failed to backfill %s: %w", step.name, err)
		}
		log.Printf("cutover: %s — copied %d, skipped %d (already present)", report.Table, report.Copied, report.Skipped)
	}

	// Record that the cutover happened, so the boot guard can stop asking.
	//
	// Written ONLY after all six steps have succeeded — a partial run leaves no
	// marker, and RequireConfigBackfill keeps refusing until the rest is copied,
	// which is the correct reading of "half the configuration is still in
	// ClickHouse". Stamped last for the same reason db.RunMigrations records a
	// file only after a clean Exec: the marker must never be able to claim more
	// than actually happened.
	//
	// A DRY RUN MUST NOT STAMP IT. The marker is what silences the boot guard, so
	// stamping it here would tell every future boot that a cutover which never
	// wrote a row had completed — turning the rehearsal into the exact silent
	// failure the guard exists to catch.
	if DryRun {
		log.Printf("cutover: DRY RUN — nothing was written and the completion marker was NOT stamped")
		return reports, nil
	}

	if err := markConfigBackfillComplete(); err != nil {
		return reports, fmt.Errorf("copied every table but failed to record the cutover marker (re-run this to stamp it): %w", err)
	}
	return reports, nil
}

// The four value sets migration 119 and 120 turned into MariaDB ENUMs.
//
// Deliberately LOCAL COPIES of the maps in query/alert_rules.query.go rather than
// exported from there. This package is scheduled for deletion; giving it a
// dependency on the query layer's internals would mean either exporting them
// permanently for a throwaway caller, or an import that has to be unpicked later.
// If they ever disagree the migration's ENUM is the arbiter, and the disagreement
// surfaces here as a loud refusal rather than a bad row.
var (
	backfillAlertTypes      = map[string]bool{"threshold": true, "absence": true, "rate_change": true}
	backfillAlertConditions = map[string]bool{"gt": true, "lt": true, "gte": true, "lte": true, "eq": true}
	backfillAlertMetrics    = map[string]bool{"count": true, "sum": true, "avg": true, "min": true, "max": true}
	backfillAlertPriorities = map[string]bool{"P0": true, "P1": true, "P2": true, "P3": true}
	backfillChannelTypes    = map[string]bool{"webhook": true, "slack": true, "email": true, "pagerduty": true}
)

// requireEnum maps a legacy ClickHouse String onto a MariaDB ENUM value.
//
// An EMPTY value takes the column's default, because ClickHouse's own DEFAULT
// clause means a legacy row can genuinely carry one. A NON-EMPTY value that is
// not in the set is REFUSED, naming the row. It would otherwise be truncated to
// the empty string by the ENUM insert under a non-strict sql_mode, or abort with
// errno 1265 under a strict one, and neither says which rule is at fault. Nothing
// should hit this: the create path validated type, condition and metric on the
// way in.
func requireEnum(table, id, column, value, fallback string, valid map[string]bool) (string, error) {
	if value == "" {
		return fallback, nil
	}
	if !valid[value] {
		return "", fmt.Errorf("%s row %s has %s=%q, which migration 119 does not accept — correct it in ClickHouse and re-run", table, id, column, value)
	}
	return value, nil
}

// requireJSON maps a legacy ClickHouse String onto a MariaDB JSON column.
//
// The empty string is the case that actually occurs: the ClickHouse columns
// carried `DEFAULT '[]'` / `DEFAULT '{}'`, but a row written before those
// defaults, or by an INSERT that named the column explicitly, can hold "" — which
// json_valid rejects. Anything else invalid is refused rather than repaired,
// because a malformed matchers object is a routing rule whose meaning nobody can
// guess.
func requireJSON(table, id, column, value, fallback string) (string, error) {
	if value == "" {
		return fallback, nil
	}
	if !json.Valid([]byte(value)) {
		return "", fmt.Errorf("%s row %s has %s=%q, which is not valid JSON — correct it in ClickHouse and re-run", table, id, column, value)
	}
	return value, nil
}

// existingIDs reads the ids already in a MariaDB table, so the copy can skip them.
//
// One query per table rather than a lookup per row: these tables hold tens of
// rows, and a set built up front also means the skip decision cannot drift
// between the check and the insert within a run.
// DryRun makes BackfillConfig read, validate and report WITHOUT writing anything.
//
// It exists because this cutover is a one-way write into six live configuration
// tables, run by hand, under time pressure, on the one deploy where getting it
// wrong stops alerting. "Safe to re-run" is true but it is not the same as
// "safe to try", and an operator should be able to see exactly what would be
// copied — and which rows would be REFUSED, which is the failure that actually
// bites — before committing to it.
//
// Enforced at ONE chokepoint (insertRow) rather than at the six call sites, so a
// seventh table added later is covered by default instead of by remembering.
// Every read, every enum check and every duplicate-key decision still runs, so a
// clean dry run exercises the whole path except the write itself.
var DryRun bool

// insertRow performs the cutover's only write, or reports what it would have
// written. The MARKER is guarded separately in BackfillConfig — a dry run must
// not stamp completion, or the boot guard would go quiet on a cutover that never
// happened.
func insertRow(table, id, statement string, args ...interface{}) error {
	if DryRun {
		log.Printf("cutover: DRY RUN — would insert %s row %s", table, id)
		return nil
	}
	_, err := db.SQL.Exec(statement, args...)
	return err
}

func existingIDs(table string) (map[string]bool, error) {
	rows, err := db.SQL.Query("SELECT id FROM " + table)
	if err != nil {
		return nil, fmt.Errorf("failed to read existing ids from %s: %w", table, err)
	}
	defer rows.Close()

	ids := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan existing id from %s: %w", table, err)
		}
		ids[id] = true
	}
	return ids, rows.Err()
}

// describeInsertError turns a duplicate-key failure into something an operator
// can act on.
//
// Errno 1062 is the one failure this cutover is genuinely expected to hit,
// because migrations 119, 120 and 122 add UNIQUE keys on `name` that the
// ClickHouse tables never had, and 121 adds one on `position`. Raw, it reads
// "Error 1062 (23000): Duplicate entry 'X' for key 'uq_alert_rules_name'" with no
// hint that the fix is to rename a row and re-run — which it is, and which is
// safe, because everything already copied is skipped on the next pass.
func describeInsertError(table, id string, err error) error {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return fmt.Errorf("%s row %s collides with a row already in MariaDB (%s). Migrations 119-122 add uniqueness the ClickHouse tables never had; resolve the duplicate — rename one, or delete the row that should not be there — and re-run. Rows already copied are skipped: %w", table, id, mysqlErr.Message, err)
	}
	return fmt.Errorf("failed to insert %s row %s: %w", table, id, err)
}

// chTime normalizes a ClickHouse DateTime64(3,'UTC') for a MariaDB DATETIME(3),
// exactly as issues/backfill.go does. MariaDB stores no zone, so the value is
// pinned to UTC before it loses the ability to say so.
func chTime(t time.Time) time.Time { return t.UTC().Truncate(time.Millisecond) }

func backfillAlertRules(ctx context.Context) (Report, error) {
	report := Report{Table: "monitor.alert_rules"}

	existing, err := existingIDs("monitor.alert_rules")
	if err != nil {
		return report, err
	}

	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT id, name, description, type, priority, query_filters, metric, field, condition, threshold, evaluation_interval_seconds, for_seconds, cooldown_seconds, notification_channel_ids, enabled, created_at, updated_at FROM %s.alert_rules FINAL",
		db.Database,
	))
	if err != nil {
		return report, fmt.Errorf("failed to read legacy alert rules: %w", err)
	}
	defer rows.Close()

	// `condition` is backticked: this is an unqualified use of a MariaDB reserved
	// word. Without the backticks the statement is a syntax error that only
	// appears when the cutover is actually run.
	// `project` is stamped here for the reason migrations 127-133 backfill it with
	// a literal: the column is NOT NULL with no default, so an INSERT that omits it
	// fails errno 1364 and aborts the whole backfill on its first row. Unlike the
	// migrations, Go CAN read the environment, so this uses the configured default
	// rather than a hardcoded 'default'.
	const insert = "INSERT INTO monitor.alert_rules " +
		"(id, project, name, description, type, priority, query_filters, metric, field, `condition`, threshold, " +
		"evaluation_interval_seconds, for_seconds, cooldown_seconds, notification_channel_ids, enabled, created_at, updated_at) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	for rows.Next() {
		var (
			id, name, description, ruleType, priority string
			queryFilters, metric, field, condition    string
			channelIDs                                string
			threshold                                 float64
			evalInterval, forSeconds, cooldown        uint32
			enabled                                   uint8
			createdAt, updatedAt                      time.Time
		)
		if err := rows.Scan(&id, &name, &description, &ruleType, &priority, &queryFilters,
			&metric, &field, &condition, &threshold, &evalInterval, &forSeconds, &cooldown,
			&channelIDs, &enabled, &createdAt, &updatedAt); err != nil {
			return report, fmt.Errorf("failed to scan legacy alert rule: %w", err)
		}

		if existing[id] {
			report.Skipped++
			continue
		}

		// `type` and `condition` have no fallback: they are required by
		// alerts.CreateRule and the MariaDB column is NOT NULL with no default,
		// so an empty one is a row that cannot be represented and must be
		// looked at rather than guessed.
		if ruleType, err = requireEnum("alert_rules", id, "type", ruleType, "", backfillAlertTypes); err != nil || ruleType == "" {
			if err == nil {
				err = fmt.Errorf("alert_rules row %s has an empty type — correct it in ClickHouse and re-run", id)
			}
			return report, err
		}
		if condition, err = requireEnum("alert_rules", id, "condition", condition, "", backfillAlertConditions); err != nil || condition == "" {
			if err == nil {
				err = fmt.Errorf("alert_rules row %s has an empty condition — correct it in ClickHouse and re-run", id)
			}
			return report, err
		}
		if priority, err = requireEnum("alert_rules", id, "priority", priority, "P2", backfillAlertPriorities); err != nil {
			return report, err
		}
		if metric, err = requireEnum("alert_rules", id, "metric", metric, "count", backfillAlertMetrics); err != nil {
			return report, err
		}
		if queryFilters, err = requireJSON("alert_rules", id, "query_filters", queryFilters, "[]"); err != nil {
			return report, err
		}
		if channelIDs, err = requireJSON("alert_rules", id, "notification_channel_ids", channelIDs, "[]"); err != nil {
			return report, err
		}

		if err := insertRow("monitor.alert_rules", id, insert, id, env.DefaultProjectSlug, name, description, ruleType, priority, queryFilters,
			metric, field, condition, threshold, evalInterval, forSeconds, cooldown, channelIDs,
			enabled == 1, chTime(createdAt), chTime(updatedAt)); err != nil {
			return report, describeInsertError("alert_rules", id, err)
		}
		report.Copied++
	}
	return report, rows.Err()
}

func backfillNotificationChannels(ctx context.Context) (Report, error) {
	report := Report{Table: "monitor.notification_channels"}

	existing, err := existingIDs("monitor.notification_channels")
	if err != nil {
		return report, err
	}

	// No FINAL: the legacy table was a plain MergeTree, matching the read
	// alerts.ListChannels made.
	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT id, name, type, config, created_at FROM %s.notification_channels",
		db.Database,
	))
	if err != nil {
		return report, fmt.Errorf("failed to read legacy notification channels: %w", err)
	}
	defer rows.Close()

	// Writes the CIPHERTEXT column, exactly as query.CreateNotificationChannel
	// does. A cutover that kept writing plaintext would quietly reintroduce the
	// disclosure migration 126 exists to close — on the one code path whose whole
	// job is to carry old rows forward, which is where it would be least looked
	// for. `config` is left NULL for the same reason it is there.
	// `project` is stamped here for the reason migrations 127-133 backfill it with
	// a literal: the column is NOT NULL with no default, so an INSERT that omits it
	// fails errno 1364 and aborts the whole backfill on its first row. Unlike the
	// migrations, Go CAN read the environment, so this uses the configured default
	// rather than a hardcoded 'default'.
	const insert = "INSERT INTO monitor.notification_channels (id, project, name, type, config, config_enc, created_at) VALUES (?, ?, ?, ?, NULL, ?, ?)"

	for rows.Next() {
		var id, name, chType, config string
		var createdAt time.Time
		if err := rows.Scan(&id, &name, &chType, &config, &createdAt); err != nil {
			return report, fmt.Errorf("failed to scan legacy notification channel: %w", err)
		}

		if existing[id] {
			report.Skipped++
			continue
		}

		if chType, err = requireEnum("notification_channels", id, "type", chType, "", backfillChannelTypes); err != nil || chType == "" {
			if err == nil {
				err = fmt.Errorf("notification_channels row %s has an empty type — correct it in ClickHouse and re-run", id)
			}
			return report, err
		}
		if config, err = requireJSON("notification_channels", id, "config", config, "{}"); err != nil {
			return report, err
		}

		encrypted, err := tools.Encrypt(config)
		if err != nil {
			return report, fmt.Errorf("failed to encrypt config for notification_channels row %s: %w", id, err)
		}

		if err := insertRow("monitor.notification_channels", id, insert, id, env.DefaultProjectSlug, name, chType, encrypted, chTime(createdAt)); err != nil {
			return report, describeInsertError("notification_channels", id, err)
		}
		report.Copied++
	}
	return report, rows.Err()
}

func backfillServiceGroups(ctx context.Context) (Report, error) {
	report := Report{Table: "monitor.service_groups"}

	existing, err := existingIDs("monitor.service_groups")
	if err != nil {
		return report, err
	}

	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT id, name, description, services, created_at, updated_at FROM %s.service_groups FINAL",
		db.Database,
	))
	if err != nil {
		return report, fmt.Errorf("failed to read legacy service groups: %w", err)
	}
	defer rows.Close()

	// `project` is stamped here for the reason migrations 127-133 backfill it with
	// a literal: the column is NOT NULL with no default, so an INSERT that omits it
	// fails errno 1364 and aborts the whole backfill on its first row. Unlike the
	// migrations, Go CAN read the environment, so this uses the configured default
	// rather than a hardcoded 'default'.
	const insert = "INSERT INTO monitor.service_groups (id, project, name, description, services, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)"

	for rows.Next() {
		var id, name, description, services string
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&id, &name, &description, &services, &createdAt, &updatedAt); err != nil {
			return report, fmt.Errorf("failed to scan legacy service group: %w", err)
		}

		if existing[id] {
			report.Skipped++
			continue
		}

		if services, err = requireJSON("service_groups", id, "services", services, "[]"); err != nil {
			return report, err
		}

		if err := insertRow("monitor.service_groups", id, insert, id, env.DefaultProjectSlug, name, description, services, chTime(createdAt), chTime(updatedAt)); err != nil {
			return report, describeInsertError("service_groups", id, err)
		}
		report.Copied++
	}
	return report, rows.Err()
}

// backfillNotificationPolicies is the only one of the six that does not copy a
// column verbatim.
//
// POSITIONS ARE RENUMBERED, densely, from the next free value. The whole premise
// of migration 121 is that the ClickHouse positions may not be unique — a racy
// max+1 allocator and a non-transactional reorder are exactly what produced
// duplicates — so copying them across would fail the new UNIQUE key on the first
// collision. Reading them in (position, created_at, id) order and assigning
// sequentially preserves the ORDER, which is the only thing position has ever
// meant to alerts/router.go, and discards the values, which never meant anything.
//
// The next free value is read once, before the loop, and incremented locally.
// That is correct because this runs before the new binary serves, so nothing else
// is writing the table; it is also why the caveat on BackfillConfig about not
// racing alerts.Init matters here specifically.
func backfillNotificationPolicies(ctx context.Context) (Report, error) {
	report := Report{Table: "monitor.notification_policies"}

	existing, err := existingIDs("monitor.notification_policies")
	if err != nil {
		return report, err
	}

	var nextPosition int
	if err := db.SQL.QueryRow(
		"SELECT COALESCE(MAX(position), 0) + 1 FROM monitor.notification_policies",
	).Scan(&nextPosition); err != nil {
		return report, fmt.Errorf("failed to read the next notification policy position: %w", err)
	}

	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT id, name, description, position, matchers, channel_ids, continue_matching, repeat_interval_seconds, enabled, is_default, created_at, updated_at FROM %s.notification_policies FINAL ORDER BY position ASC, created_at ASC, id ASC",
		db.Database,
	))
	if err != nil {
		return report, fmt.Errorf("failed to read legacy notification policies: %w", err)
	}
	defer rows.Close()

	// `project` is stamped here for the reason migrations 127-133 backfill it with
	// a literal: the column is NOT NULL with no default, so an INSERT that omits it
	// fails errno 1364 and aborts the whole backfill on its first row. Unlike the
	// migrations, Go CAN read the environment, so this uses the configured default
	// rather than a hardcoded 'default'.
	const insert = "INSERT INTO monitor.notification_policies " +
		"(id, project, name, description, position, matchers, channel_ids, continue_matching, repeat_interval_seconds, enabled, is_default, created_at, updated_at) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	for rows.Next() {
		var (
			id, name, description, matchers, channelIDs string
			legacyPosition, repeatInterval              uint32
			continueMatching, enabled, isDefault        uint8
			createdAt, updatedAt                        time.Time
		)
		if err := rows.Scan(&id, &name, &description, &legacyPosition, &matchers, &channelIDs,
			&continueMatching, &repeatInterval, &enabled, &isDefault, &createdAt, &updatedAt); err != nil {
			return report, fmt.Errorf("failed to scan legacy notification policy: %w", err)
		}

		if existing[id] {
			report.Skipped++
			continue
		}

		if matchers, err = requireJSON("notification_policies", id, "matchers", matchers, "{}"); err != nil {
			return report, err
		}
		if channelIDs, err = requireJSON("notification_policies", id, "channel_ids", channelIDs, "[]"); err != nil {
			return report, err
		}

		if err := insertRow("monitor.notification_policies", id, insert, id, env.DefaultProjectSlug, name, description, nextPosition, matchers, channelIDs,
			continueMatching == 1, repeatInterval, enabled == 1, isDefault == 1,
			chTime(createdAt), chTime(updatedAt)); err != nil {
			return report, describeInsertError("notification_policies", id, err)
		}
		if int(legacyPosition) != nextPosition {
			log.Printf("cutover: notification policy %s renumbered from position %d to %d (order preserved)", id, legacyPosition, nextPosition)
		}
		nextPosition++
		report.Copied++
	}
	return report, rows.Err()
}

func backfillDashboards(ctx context.Context) (Report, error) {
	report := Report{Table: "monitor.dashboards"}

	existing, err := existingIDs("monitor.dashboards")
	if err != nil {
		return report, err
	}

	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT id, name, description, config, created_at, updated_at FROM %s.dashboards FINAL",
		db.Database,
	))
	if err != nil {
		return report, fmt.Errorf("failed to read legacy dashboards: %w", err)
	}
	defer rows.Close()

	// `project` is stamped rather than copied: the legacy ClickHouse table
	// predates the tenancy dimension entirely, so there is nothing in the source
	// row to carry across. env.DefaultProjectSlug is the same reading migration
	// 131 gives its own backfill — a row written before any project existed
	// belongs to the default one — with the advantage that this path CAN read the
	// environment, so an install that overrode MON_DEFAULT_PROJECT lands its rows
	// where its keys already write instead of in a project nothing reads.
	//
	// It is not optional: the column is NOT NULL with no default, so an insert
	// without it fails as errno 1364 on the first row and aborts the cutover.
	const insert = "INSERT INTO monitor.dashboards (id, project, name, description, config, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)"

	for rows.Next() {
		var id, name, description, config string
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&id, &name, &description, &config, &createdAt, &updatedAt); err != nil {
			return report, fmt.Errorf("failed to scan legacy dashboard: %w", err)
		}

		if existing[id] {
			report.Skipped++
			continue
		}

		// config is copied verbatim, including an empty one. It is an opaque
		// client-owned blob in a LONGTEXT — migration 123 argues why it is not a
		// JSON column — so there is nothing here to validate or repair.
		if err := insertRow("monitor.dashboards", id, insert, id, env.DefaultProjectSlug, name, description, config, chTime(createdAt), chTime(updatedAt)); err != nil {
			return report, describeInsertError("dashboards", id, err)
		}
		report.Copied++
	}
	return report, rows.Err()
}

func backfillSavedViews(ctx context.Context) (Report, error) {
	report := Report{Table: "monitor.saved_views"}

	existing, err := existingIDs("monitor.saved_views")
	if err != nil {
		return report, err
	}

	// No FINAL: the legacy table was a plain MergeTree with no dedup at all, so
	// it can genuinely hold two rows for one id. The skip set makes the second
	// one a no-op rather than a duplicate-key failure — which is the one place
	// this cutover repairs legacy data rather than merely moving it.
	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT id, name, query_params, page, created_at FROM %s.saved_views",
		db.Database,
	))
	if err != nil {
		return report, fmt.Errorf("failed to read legacy saved views: %w", err)
	}
	defer rows.Close()

	// Stamped with the default project, for the reason backfillDashboards gives:
	// the legacy rows predate tenancy, and the column is NOT NULL with no default.
	const insert = "INSERT INTO monitor.saved_views (id, project, name, query_params, page, created_at) VALUES (?, ?, ?, ?, ?, ?)"

	seen := map[string]bool{}
	for rows.Next() {
		var id, name, queryParams, page string
		var createdAt time.Time
		if err := rows.Scan(&id, &name, &queryParams, &page, &createdAt); err != nil {
			return report, fmt.Errorf("failed to scan legacy saved view: %w", err)
		}

		if existing[id] || seen[id] {
			report.Skipped++
			continue
		}
		seen[id] = true

		if page == "" {
			page = "events"
		}

		if err := insertRow("monitor.saved_views", id, insert, id, env.DefaultProjectSlug, name, queryParams, page, chTime(createdAt)); err != nil {
			return report, describeInsertError("saved_views", id, err)
		}
		report.Copied++
	}
	return report, rows.Err()
}
