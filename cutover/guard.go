package cutover

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
)

// THE CUTOVER HAS AN ORDER, AND NOTHING ELSE ENFORCES IT.
//
// `monitor-core backfill-config` must run BEFORE the new binary serves. Migrations
// 119-124 create six EMPTY MariaDB tables, and the new code reads its alerting
// configuration from them — so a process that starts before the copy has run
// finds no rules, no policies and no channels, and behaves exactly as it would on
// an install that has never had any.
//
// EVERY ONE OF THOSE FAILURES IS SILENT. That is the whole reason this file
// exists, and it is worth being specific rather than gesturing at "the order
// matters":
//
//   - listEnabledRules returns an empty slice and a nil error. Evaluator.evaluateAll
//     ranges over nothing, logs nothing, and returns. There is no error path, no
//     counter and no metric that distinguishes "no rules configured" from "six rules
//     that have vanished" — the loop simply has no work. Alerting stops, and the
//     first person to notice is whoever eventually wonders why an incident never
//     paged.
//   - query.SeedDefaultNotificationPolicies sees an empty table, seeds the four
//     priority defaults, and the backfill then APPENDS the four ClickHouse originals
//     behind them (positions are renumbered from MAX+1, so nothing collides). Eight
//     policies, of which four match every alert by priority and carry no channels,
//     so the routing list short-circuits into a black hole. No error, no warning.
//
// CI MAKES THAT ORDERING THE DEFAULT, NOT THE EXCEPTION. build-and-deploy.yml
// builds the image and triggers the Lattice redeploy on a push to main, and
// `backfill-config` only exists INSIDE that new image — so with no intervention
// the new binary is serving before anyone can possibly have run the cutover.
// Documenting a runbook step that the pipeline races is not a control.
//
// So the boot refuses. A crash-loop is a loud, self-describing failure that
// Lattice surfaces as an anomaly within a minute and that names its own remedy in
// the container log; silently-not-alerting is a failure with no observer at all.
// That is the same trade main() already makes for the ClickHouse events probe,
// for the same reason: a process that comes up green and quietly does nothing is
// the worst of the available outcomes.
//
// WHAT THIS DELIBERATELY IS NOT is a row-count heuristic that lives forever.
// "MariaDB is empty and ClickHouse is not" is true of an un-migrated instance —
// and it is also true of an instance that was migrated correctly and whose
// operator later deleted their last alert rule through the UI. Bricking a healthy
// Monitor because somebody tidied up their alerts would be a far worse defect than
// the one being guarded. The marker below is what separates the two, and it is
// why the guard is a one-shot rather than a permanent tripwire.

// CONFIG_BACKFILL_MARKER is the settings key BackfillConfig stamps once it has
// copied all six tables. Its presence — not a row count — is what says the
// cutover happened.
//
// It lives in the `settings` KV table (migration 105) rather than in a table of
// its own because it is one fact with a lifetime measured in one release: when
// the legacy ClickHouse tables are dropped and this package is deleted, the row
// goes with a single DELETE and leaves no schema behind. The value is the RFC3339
// timestamp of the run, which is the thing an operator actually wants when they
// are reconstructing what happened on a deploy day.
const CONFIG_BACKFILL_MARKER = "cutover:config_backfill_completed_at"

// configTables pairs each legacy ClickHouse table with the MariaDB table that
// replaced it, and states what goes wrong if the copy has not happened.
//
// The consequence strings are not decoration: this error is read by someone
// looking at a container that will not start, and the difference between "run the
// backfill" and "run the backfill or alerting is off" is the difference between
// treating it as a chore and treating it as an incident.
var configTables = []struct {
	clickhouse  string
	mariadb     string
	consequence string
}{
	{"alert_rules", "monitor.alert_rules",
		"the evaluator would find no rules and stop alerting, with nothing logged"},
	{"notification_policies", "monitor.notification_policies",
		"alerts.Init would seed four defaults that the backfill then appends behind, routing every alert nowhere"},
	{"notification_channels", "monitor.notification_channels",
		"a firing alert would resolve no channel and be delivered nowhere"},
	{"service_groups", "monitor.service_groups",
		"any policy keyed on a service group would match nothing"},
	{"dashboards", "monitor.dashboards",
		"every saved dashboard would be missing from the UI"},
	{"saved_views", "monitor.saved_views",
		"every saved view would be missing from the UI"},
}

// RequireConfigBackfill refuses the boot when the ClickHouse → MariaDB copy has
// not been run and there is something to copy. Called from main()'s data-plane
// block, before alerts.Init — which is load-bearing placement: getting there
// first is what stops the default-policy seed from creating the eight-policy
// state described above, so the guard PREVENTS that failure rather than merely
// reporting it afterwards.
//
// It answers nil in all three states that are actually fine:
//
//   - the marker is present (the cutover ran, whatever the tables hold now);
//   - the legacy table does not exist (a fresh install, or a post-retirement one
//     where the ClickHouse originals have been dropped);
//   - the legacy table is empty, or the MariaDB one is not (there is nothing
//     stranded, including after a manual copy that never touched the marker).
//
// It FAILS OPEN on a ClickHouse probe error, loudly, and that direction is
// chosen rather than inherited. In the state being guarded against ClickHouse is
// healthy by construction — db.Connect, the migration runner and the events probe
// have all just succeeded against it — so a probe that errors here is telling us
// about something other than the cutover, and refusing the boot over it would
// invent a new way for a deploy to fail in exchange for no protection at all. The
// MariaDB side fails CLOSED for the mirror-image reason: it was migrated three
// statements ago, so a count that will not run there means the guard cannot
// answer the question it exists to answer.
func RequireConfigBackfill(ctx context.Context) error {
	if done, err := configBackfillMarked(); err != nil {
		// Not fatal: an unreadable marker only costs the short circuit. The row
		// counts below answer the same question, less cheaply.
		log.Printf("WARNING: could not read the %s marker, falling back to row counts: %v", CONFIG_BACKFILL_MARKER, err)
	} else if done {
		return nil
	}

	var stranded []string
	for _, t := range configTables {
		legacy, ok := legacyRowCount(ctx, t.clickhouse)
		if !ok || legacy == 0 {
			continue
		}

		migrated, err := mariadbRowCount(t.mariadb)
		if err != nil {
			return fmt.Errorf("cannot verify the configuration cutover: %w", err)
		}
		if migrated > 0 {
			continue
		}

		stranded = append(stranded, fmt.Sprintf(
			"  %-21s %d row(s) in ClickHouse, 0 in %s — %s",
			t.clickhouse, legacy, t.mariadb, t.consequence,
		))
	}

	if len(stranded) == 0 {
		return nil
	}

	return fmt.Errorf(
		"the configuration cutover has not been run — refusing to serve with the alerting configuration stranded in ClickHouse:\n%s\n"+
			"Run `monitor-core backfill-config` with this same environment (it applies migrations 119-124 itself, copies the rows, and exits without serving), then start the service again. "+
			"It is safe to re-run: rows already in MariaDB are skipped, never overwritten. "+
			"If these legacy rows are genuinely being abandoned, drop the ClickHouse tables listed above instead — that clears this check permanently",
		strings.Join(stranded, "\n"),
	)
}

// configBackfillMarked reports whether a completed backfill has been recorded.
func configBackfillMarked() (bool, error) {
	_, err := query.GetSetting(db.SQL, CONFIG_BACKFILL_MARKER)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// markConfigBackfillComplete stamps the marker. Called by BackfillConfig once all
// six tables have been copied without error.
func markConfigBackfillComplete() error {
	return query.SetSetting(db.SQL, CONFIG_BACKFILL_MARKER, time.Now().UTC().Format(time.RFC3339))
}

// legacyRowCount counts a legacy ClickHouse table, reporting ok=false when the
// answer cannot be determined.
//
// Existence is checked through system.tables rather than by running the count and
// interpreting the failure. An UNKNOWN_TABLE is the ORDINARY state on a fresh
// install and again after the originals are retired, and a guard that reaches
// that state by catching an error cannot tell it apart from a grant problem or a
// typo in db.Database. One ordinary SELECT that returns 0 says it directly.
//
// No FINAL on the count: this only ever asks "is there anything here", and an
// un-merged ReplacingMergeTree over-counts rather than under-counts, so the
// answer to that question is the same either way.
func legacyRowCount(ctx context.Context, table string) (uint64, bool) {
	var exists uint64
	if err := db.Conn.QueryRow(ctx,
		"SELECT count() FROM system.tables WHERE database = ? AND name = ?",
		db.Database, table,
	).Scan(&exists); err != nil {
		log.Printf("WARNING: could not check whether %s.%s still exists, skipping its cutover check: %v", db.Database, table, err)
		return 0, false
	}
	if exists == 0 {
		return 0, true
	}

	var count uint64
	if err := db.Conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s.%s", db.Database, table)).Scan(&count); err != nil {
		log.Printf("WARNING: could not count %s.%s, skipping its cutover check: %v", db.Database, table, err)
		return 0, false
	}
	return count, true
}

// mariadbRowCount counts the destination table. The table name is a constant
// from configTables, never anything a caller supplies.
func mariadbRowCount(table string) (int, error) {
	var count int
	if err := db.SQL.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count %s: %w", table, err)
	}
	return count, nil
}
