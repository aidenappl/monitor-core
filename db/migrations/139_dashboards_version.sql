-- Optimistic concurrency for dashboards (DASHBOARD-11).
--
-- A dashboard PUT carries the whole config, so two tabs — or two people —
-- editing one dashboard silently overwrote each other: last write won and the
-- first edit vanished with nothing said. `version` starts at 1 and every
-- successful update increments it in the same statement. A PUT that sends the
-- version it last read is applied only when that is still the stored version
-- (`WHERE ... AND version = ?`), and is otherwise refused with a 409 carrying
-- the current dashboard. A PUT without a version keeps the old last-write-wins
-- behaviour, so a client that predates this keeps working unchanged.
--
-- NOT NULL DEFAULT 1 backfills every existing row in the same metadata change.
-- IF NOT EXISTS keeps the file re-runnable.

ALTER TABLE monitor.dashboards
    ADD COLUMN IF NOT EXISTS version INT UNSIGNED NOT NULL DEFAULT 1;
