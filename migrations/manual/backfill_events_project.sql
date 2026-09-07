-- backfill_events_project.sql — MANUAL, ONE-OFF. NOT A MIGRATION.
--
-- What it does: stamps the default project slug onto every events row written
-- before 006_events_project.sql added the column. Adding a column in ClickHouse
-- is metadata-only — existing parts are not rewritten — so those rows read back
-- as the empty string rather than as anything meaningful. This is the statement
-- that turns them into rows of a real project.
--
-- Why it is NOT automatic, and must never be moved up a directory into
-- migrations/: the boot runner (migrations/embed.go) has no applied-tracking
-- table. It re-executes every top-level migrations/*.sql file on EVERY boot and
-- requires each statement to be idempotent DDL. ALTER TABLE ... UPDATE is a
-- mutation, not DDL. Sitting in migrations/ it would queue a fresh table-wide
-- rewrite on every single restart of the service, forever — one more queued
-- mutation per crash-loop iteration, each one re-reading and re-writing every
-- part it touches. This subdirectory is excluded from the `*.sql` embed pattern
-- precisely so that cannot happen by accident.
--
-- ClickHouse mutations are ASYNCHRONOUS. The statement returns as soon as the
-- mutation is accepted, not when it has been applied; the rows change under you
-- over the following seconds to minutes depending on how many parts are in the
-- retention window. Do not read the result of the verification query as final
-- until system.mutations reports the mutation done.
--
-- Adjust the `monitor.` database prefix if CLICKHOUSE_DATABASE differs in your
-- deployment. Unlike the files in migrations/, this one is NOT rewritten for
-- you — the runner never sees it.
--
-- Substitute the deployment's MON_DEFAULT_PROJECT value for 'default' below if
-- it has been overridden. The slug must match what the ingest path stamps, or
-- the backfilled history will sit in a project no live traffic writes to.

-- Step 0 — before. How many rows are still unstamped, and over what span.
--
--   SELECT count() AS unstamped, min(timestamp) AS oldest, max(timestamp) AS newest
--   FROM monitor.events
--   WHERE project = ''

-- Step 1 — the backfill.
-- Scoped to project = '' so it touches only pre-006 rows: rows already carrying
-- a project were stamped server-side at ingest from the authenticating api_keys
-- row, and reassigning those to the default would destroy real tenancy data.
-- That scoping is also what makes the statement safe to re-run if it is
-- interrupted — a second run matches only whatever the first did not reach.
ALTER TABLE monitor.events
UPDATE project = 'default'
WHERE project = '';

-- Step 2 — watch it. A mutation that is still running has is_done = 0, and one
-- that failed carries the reason in latest_fail_reason rather than surfacing an
-- error at the client that submitted it.
--
--   SELECT mutation_id, command, parts_to_do, is_done, latest_fail_reason
--   FROM system.mutations
--   WHERE database = 'monitor' AND table = 'events' AND is_done = 0
--   ORDER BY create_time DESC

-- Step 3 — after. Should report zero unstamped rows once the mutation is done.
--
--   SELECT project, count() AS rows
--   FROM monitor.events
--   GROUP BY project
--   ORDER BY rows DESC
--
-- If it is taking too long or was issued against the wrong slug, a mutation can
-- be cancelled while it is still running:
--
--   KILL MUTATION WHERE database = 'monitor' AND table = 'events' AND mutation_id = '...'
