-- dedupe_issues_by_fingerprint.sql — MANUAL, ONE-OFF. NOT A MIGRATION.
--
-- ⚠️ SUPERSEDED — DO NOT RUN. It operates on the WRONG STORE.
--
-- This script targets `monitor.issues` as a ClickHouse ReplacingMergeTree and
-- uses FINAL. That table stopped being the source of truth in
-- db/migrations/111_create_issues.sql, which moved the issue row to MariaDB
-- precisely BECAUSE ReplacingMergeTree could not enforce uniqueness — the
-- duplicate fingerprints this script was written to clean up are the defect that
-- motivated the move. The ClickHouse table still physically exists but is created
-- by nothing and read only by issues/backfill.go.
--
-- So running this today would rewrite a dead table while the live issue rows in
-- MariaDB are untouched, and its INSERT would re-inflate occurrence counts in a
-- store nothing serves from. The problem it solves cannot recur: MariaDB's
-- UNIQUE KEY on the fingerprint makes duplicate rows unrepresentable, and the
-- upsert is a single atomic statement.
--
-- Kept only as the record of why the issue row moved. It is retained rather than
-- deleted because migration 111's header cites this cleanup as evidence.
--
-- If you are here during the Phase 1 project rollout, the two scripts you
-- actually want are backfill_events_project.sql and, later,
-- delete_orphaned_issue_rollups.sql — both in this directory.
--
-- This lives in migrations/manual/ and NOT in migrations/ on purpose. The
-- ClickHouse runner (migrations/embed.go) has no applied-tracking table: it
-- re-executes every top-level migrations/*.sql on EVERY boot and requires each
-- statement to be idempotent. This script is neither idempotent nor safe to
-- repeat — its INSERT would re-fold and re-inflate occurrence counts on each
-- run, and it issues an asynchronous ALTER ... DELETE. It also carries example
-- SQL inside comments, and the runner splits files naively on ';', which would
-- hand ClickHouse a comment-only fragment and take the service down on boot
-- (the caller treats a migration error as fatal).
--
-- Run it by hand, once, with the checks below. Do not move it up a directory.
--
-- Reconciles issue rows that were split across multiple ids for a single
-- fingerprint.
--
-- Background: the issues table is ReplacingMergeTree(updated_at) ORDER BY (id),
-- so ClickHouse only collapses rows that agree on `id`. Issue creation used to
-- mint uuid.New() per insert, so two worker goroutines that both observed "no
-- issue yet" for one fingerprint created two permanently distinct issues. FINAL
-- cannot merge them because they differ in the ORDER BY key. Production carried
-- four rows for a single fingerprint on at least two fingerprints.
--
-- The application fix (issues.issueIDFor) derives the id from the fingerprint, so
-- concurrent creators now converge on one row without coordinating. That applies
-- to NEW writes only — rows created before the fix keep their random ids, which
-- is what this migration cleans up.
--
-- Strategy: for each fingerprint, keep the row with the highest occurrence_count
-- (ties broken by most recent last_seen) and delete the rest, after folding the
-- duplicates' counts and timestamps into the survivor so no history is lost.
--
-- !! DESTRUCTIVE — READ BEFORE RUNNING !!
-- ALTER TABLE ... DELETE is an asynchronous mutation and is not reversible.
-- Take a backup of the issues table first:
--
--   CREATE TABLE monitor.issues_backup_004 AS monitor.issues;
--   INSERT INTO monitor.issues_backup_004 SELECT * FROM monitor.issues FINAL;
--
-- Run the SELECT at the bottom first to see exactly what will be affected.
-- Adjust the `monitor.` database prefix if your deployment differs.

-- Step 1: fold each duplicate group into a single surviving row.
-- The survivor keeps its own id; totals are summed and the window widened to
-- cover every duplicate, so the merged issue reflects the full history.
INSERT INTO monitor.issues
    (id, fingerprint, service, name, message, path, status,
     occurrence_count, first_seen, last_seen, resolved_at, updated_at)
SELECT
    argMax(id, (occurrence_count, last_seen))          AS id,
    fingerprint,
    argMax(service, (occurrence_count, last_seen))     AS service,
    argMax(name, (occurrence_count, last_seen))        AS name,
    argMax(message, last_seen)                         AS message,
    argMax(path, (occurrence_count, last_seen))        AS path,
    -- If any duplicate is still unresolved the merged issue is unresolved:
    -- silently resolving an active error is the worse failure mode.
    if(countIf(status = 'unresolved') > 0, 'unresolved',
       argMax(status, last_seen))                      AS status,
    sum(occurrence_count)                              AS occurrence_count,
    min(first_seen)                                    AS first_seen,
    max(last_seen)                                     AS last_seen,
    argMax(resolved_at, last_seen)                     AS resolved_at,
    now64(3)                                           AS updated_at
FROM monitor.issues FINAL
GROUP BY fingerprint
HAVING count() > 1;

-- Step 2: delete the non-surviving rows.
-- Scoped to fingerprints that actually had duplicates, and never deletes the id
-- chosen as survivor in step 1.
ALTER TABLE monitor.issues
DELETE WHERE fingerprint IN (
    SELECT fingerprint FROM monitor.issues FINAL
    GROUP BY fingerprint HAVING count() > 1
)
AND id NOT IN (
    SELECT argMax(id, (occurrence_count, last_seen))
    FROM monitor.issues FINAL
    GROUP BY fingerprint HAVING count() > 1
);

-- Verification — run before and after. Should return zero rows afterwards.
--
--   SELECT fingerprint, count() AS rows, groupArray(id) AS ids,
--          groupArray(message) AS messages
--   FROM monitor.issues FINAL
--   GROUP BY fingerprint
--   HAVING rows > 1
--   ORDER BY rows DESC;
--
-- Note: mutations are asynchronous. Track progress with
--   SELECT * FROM system.mutations WHERE table = 'issues' AND is_done = 0;
