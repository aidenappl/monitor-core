-- Saved dashboard layouts. Moves from a ClickHouse ReplacingMergeTree ORDER BY
-- (id) created inside dashboards.Init; 119 carries the argument for the whole
-- move.
--
-- `config` IS DELIBERATELY LONGTEXT AND NOT JSON, which is the opposite of the
-- call 119-122 made for query_filters, matchers, channel_ids and services. The
-- line is who parses the value:
--
--   * Monitor parses those. A malformed one is a rule that cannot be evaluated
--     or a group that silently matches nothing, so json_valid is the database
--     enforcing an invariant the code already depends on.
--   * Monitor never parses this. `config` is written by monitor-web, read back
--     by monitor-web, and passed through this service as an opaque string.
--     Constraining it would be Monitor holding an opinion about a format it does
--     not own — and it would break today's callers immediately, because the
--     handler does not require `config` at all and a dashboard created without
--     one stores the empty string, which json_valid rejects.
--
-- The same reasoning applies to saved_views.query_params in 124.
--
-- LONGTEXT rather than TEXT because a dashboard config is a whole panel layout:
-- TEXT caps at 64KB, which is a limit nobody would think to check and which
-- would truncate silently under a non-strict sql_mode.
--
-- NO UNIQUE ON name. A dashboard name is a human label with no machine meaning —
-- nothing looks a dashboard up by it — so two called "Overview" is a preference,
-- not a defect. The live row count is unknown, and an aborting backfill over a
-- cosmetic constraint is the worse trade.

CREATE TABLE IF NOT EXISTS monitor.dashboards (
    id          CHAR(36)        NOT NULL PRIMARY KEY,
    name        VARCHAR(255)    NOT NULL,
    description VARCHAR(1000)   NOT NULL DEFAULT '',
    config      LONGTEXT        NOT NULL,
    created_at  DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at  DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
