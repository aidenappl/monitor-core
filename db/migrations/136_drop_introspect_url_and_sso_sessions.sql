-- Removes two leftovers of the introspection checkpoint that go-oidc-session
-- replaced (135):
--   sso_providers.introspect_url — nothing reads it; the admin API no longer
--     accepts or returns it.
--   sso_sessions — one row per user, never refreshed, read only by that
--     checkpoint. oidc_sessions is the session store.
--
-- IF EXISTS keeps the file idempotent: db/sql.go records a migration only after
-- the whole file succeeds, and MariaDB commits DDL implicitly.

ALTER TABLE sso_providers DROP COLUMN IF EXISTS introspect_url;
DROP TABLE IF EXISTS sso_sessions;
