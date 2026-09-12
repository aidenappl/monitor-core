-- The alert_states and alert_history tables, and the project column both need.
--
-- THESE TWO WERE THE LAST TABLES IN THE REPO WITH NO MIGRATION FILE. They were
-- created by an ad-hoc CREATE TABLE inside alerts.Init at boot -- exactly the
-- pattern db/migrations/119 condemns for the six configuration tables it moved,
-- and which its header records as an open gap. The DDL below is that DDL,
-- byte-equivalent in engine, ORDER BY and TTL, plus the tenancy column. Init now
-- issues no DDL at all, so there is one definition of each table rather than two
-- that drift.
--
-- ⚠️ WHY BOTH A CREATE AND AN ALTER FOR THE SAME COLUMN. The tables ALREADY
-- EXIST on every deployed zone, created by the old boot-time statements. A
-- CREATE TABLE IF NOT EXISTS is therefore a no-op there and its new column would
-- never appear -- the CREATE serves FRESH installs only. The ALTER is what
-- reaches the existing ones. Neither alone is sufficient and both are idempotent,
-- so running them together on any install is correct.
--
-- EVERY STATEMENT MUST STAY IDEMPOTENT. This runner (migrations/embed.go) has no
-- applied-tracking table and REPLAYS EVERY FILE ON EVERY BOOT. A statement that
-- is not safe to re-execute forever -- an INSERT ... SELECT, or a ClickHouse
-- mutation such as ALTER TABLE ... UPDATE -- would be re-issued on each restart
-- and compound, one queued table-wide rewrite per crash-loop iteration.
--
-- NOTE for anyone editing this file: the runner splits on the semicolon
-- character with no parser, so never put one inside a comment, and never leave a
-- comment after the final statement. Either produces a comment-only fragment
-- that ClickHouse rejects at startup, taking the service down with it. 005
-- carries the same warning for the same reason.
--
-- THE SORTING KEYS ARE UNCHANGED and the project column is deliberately NOT in
-- them, following 006. Changing a sorting key means rebuilding the table, which
-- a runner with no applied-tracking can never safely emit -- and there is
-- nothing to win. alert_states holds one row per rule, a few dozen at most, and
-- alert_history is bounded by its 90-day TTL. A key change would buy a rewrite
-- and no measurable read.
--
-- LowCardinality(String) matches the events column 006 added, so both read the
-- same way for the same predicate. It carries no explicit DEFAULT, which is what
-- makes the empty string the value of every pre-existing row -- see below.
--
-- ADDING A COLUMN IN CLICKHOUSE IS METADATA-ONLY. Existing parts are not
-- rewritten, so every row written before this file reads back as the EMPTY
-- STRING, not NULL. scope.ProjectPredicate reads empty as belonging to the
-- default project during the transition window, which is why the history of an
-- existing single-project install stays visible the moment this lands.
--
--   * alert_history needs a one-off fill, because its rows are permanent facts
--     nobody rewrites: migrations/manual/backfill_alert_history_project.sql,
--     run BY HAND for the mutation reason above.
--   * alert_states needs NO backfill. The evaluator rewrites every enabled
--     rule's state row on its own interval and the ReplacingMergeTree collapses
--     the unstamped version away, so those repair themselves within one
--     evaluation pass.

CREATE TABLE IF NOT EXISTS monitor.alert_states (
    rule_id String,
    project LowCardinality(String),
    status String DEFAULT 'ok',
    value Float64 DEFAULT 0,
    fired_at Nullable(DateTime64(3, 'UTC')),
    resolved_at Nullable(DateTime64(3, 'UTC')),
    last_notified_at Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (rule_id);

ALTER TABLE monitor.alert_states
    ADD COLUMN IF NOT EXISTS project LowCardinality(String) AFTER rule_id;

CREATE TABLE IF NOT EXISTS monitor.alert_history (
    id String,
    project LowCardinality(String),
    rule_id String,
    rule_name String,
    status String,
    value Float64,
    message String DEFAULT '',
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = MergeTree
ORDER BY (created_at, rule_id)
TTL toDate(created_at) + INTERVAL 90 DAY;

ALTER TABLE monitor.alert_history
    ADD COLUMN IF NOT EXISTS project LowCardinality(String) AFTER id;
