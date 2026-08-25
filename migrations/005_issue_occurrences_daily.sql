-- Per-day occurrence counts per issue, with NO TTL, so history outlives the
-- 30-day expiry on monitor.events. Without this an issue older than the
-- retention window keeps its totals but loses every trace of WHEN it happened,
-- which is exactly what a regression history needs.
--
-- AggregatingMergeTree rather than SummingMergeTree because this is not only
-- sums, it also folds min/max for the seen-window. The materialized view fires
-- on insert, so no cron job and no backfill job exist to drift.
--
-- Keyed on issue_id, which only exists on rows since 004 — the WHERE clause
-- skips unstamped rows rather than bucketing them all under an empty key.
--
-- NOTE for anyone editing this file: the ClickHouse runner splits on the
-- semicolon character, so never put one inside a comment, and never leave a
-- comment after the final statement. Either produces a comment-only fragment
-- that ClickHouse rejects at startup, taking the service down with it.
CREATE TABLE IF NOT EXISTS monitor.issue_occurrences_daily (
    issue_id String,
    day Date,
    occurrences AggregateFunction(count, UInt64),
    first_seen AggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen AggregateFunction(max, DateTime64(3, 'UTC'))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (issue_id, day);

CREATE MATERIALIZED VIEW IF NOT EXISTS monitor.issue_occurrences_daily_mv
TO monitor.issue_occurrences_daily
AS SELECT
    issue_id,
    toDate(timestamp) AS day,
    countState() AS occurrences,
    minState(timestamp) AS first_seen,
    maxState(timestamp) AS last_seen
FROM monitor.events
WHERE issue_id != '' AND level IN ('error', 'fatal')
GROUP BY issue_id, day;
