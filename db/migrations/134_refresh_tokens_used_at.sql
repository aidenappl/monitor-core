-- Record WHEN a refresh token was spent, so a second presentation can be told
-- apart from a replay.
--
-- 102 recorded only THAT a token was spent (replaced_by), not when. That made
-- every re-presentation of a rotated token indistinguishable from theft, so two
-- tabs refreshing with the same cookie — or a client retrying after a lost
-- response — revoked the whole family and logged the user out. used_at lets
-- HandleRefresh apply a short grace window (routes/refresh_grace.go) instead.
--
-- NULLABLE, WITH NO BACKFILL, deliberately: rows rotated before this migration
-- have no trustworthy spend time, and classifyRefresh treats a spent row with a
-- NULL used_at as reuse — exactly the pre-134 behaviour — rather than guessing a
-- timestamp that could open a grace window after the fact.
--
-- IF NOT EXISTS keeps the file idempotent: db/sql.go records a migration only
-- after the whole file succeeds, and MariaDB commits DDL implicitly.

ALTER TABLE refresh_tokens
    ADD COLUMN IF NOT EXISTS used_at DATETIME NULL;
