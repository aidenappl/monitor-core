-- Saved query/filter views, one per dashboard page. Moves from a ClickHouse
-- MergeTree ORDER BY (id) created inside views.Init; 119 carries the argument for
-- the whole move.
--
-- This one had the sharpest version of the ClickHouse problem, because its engine
-- was a plain MergeTree rather than a ReplacingMergeTree. There was no dedup at
-- all — not even eventual — so re-inserting an id simply produced a second row,
-- and DELETE was an asynchronous `ALTER TABLE … DELETE` mutation that a
-- subsequent list could still return. A saved view that reappeared after being
-- deleted was the expected behaviour of the storage, not a bug in the handler.
--
-- `page` stays a VARCHAR rather than becoming an ENUM, unlike the enum-shaped
-- columns in 119 and 120. Those had a closed set the SERVER branches on; this one
-- is whatever route monitor-web saved the view from, so its set is owned by the
-- frontend's router and changes when a page is added. An ENUM here would mean a
-- migration every time the web app grows a page, and the failure mode of getting
-- it wrong — a view that cannot be saved on a new page — would look like a
-- backend bug from the frontend.
--
-- `query_params` is LONGTEXT for the reason 123 spells out: it is a client-owned
-- blob this service never parses.
--
-- NO updated_at. The ClickHouse table had none and neither the type nor any
-- handler exposes an update — a view is created and deleted, never edited. The
-- column is left off rather than added silently, so the absence of an update
-- path stays visible in the schema.
--
-- NO UNIQUE ON (page, name), which is the composite that would look natural. It
-- is left off for 123's reason: the name is a human label, nothing looks a view
-- up by it, the live row count is unknown, and an aborting backfill over a
-- cosmetic constraint is the worse trade. If it is ever added it should be
-- (page, name), not (name) — the same view name on two different pages is
-- ordinary.

CREATE TABLE IF NOT EXISTS monitor.saved_views (
    id           CHAR(36)       NOT NULL PRIMARY KEY,
    name         VARCHAR(255)   NOT NULL,
    query_params LONGTEXT       NOT NULL,
    page         VARCHAR(64)    NOT NULL DEFAULT 'events',
    created_at   DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    KEY idx_saved_views_page (page)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
