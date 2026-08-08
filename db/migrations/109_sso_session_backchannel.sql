-- Back-channel logout (OIDC Back-Channel Logout 1.0) needs to find sessions by
-- what the logout token names, and a logout token names a `sid` or a `sub`.
-- sso_sessions could be looked up by neither: its only key is user_id.
--
-- ⚠️ THESE COLUMNS CANNOT BE BACKFILLED. `subject` could technically be derived
-- from identities, but `sid` exists only inside the id_token of the login that
-- created the session, and that token is gone. Every session established before
-- this migration is therefore unreachable by a session-scoped logout for the
-- rest of its life. That is the argument for adding the columns now rather than
-- when the receiver is finished — the cost of waiting is paid by sessions, not
-- by code.
--
-- Both are NULLABLE on purpose. `subject` is null on rows written before this
-- migration; `sid` is null both for those and for any provider that issues no
-- `sid`, which is conformant. Neither absence is an error, and the receiver
-- treats "no matching session" as a normal outcome.

ALTER TABLE sso_sessions
    ADD COLUMN IF NOT EXISTS subject VARCHAR(255) NULL AFTER provider;

ALTER TABLE sso_sessions
    ADD COLUMN IF NOT EXISTS sid VARCHAR(64) NULL AFTER subject;

-- Lookups are always scoped by provider as well as the identifier, never by the
-- identifier alone. Two providers may legitimately mint the same opaque `sid`
-- or the same `sub` — they are only unique WITHIN an issuer — so an unscoped
-- match would let one identity provider end sessions belonging to another.
ALTER TABLE sso_sessions
    ADD INDEX IF NOT EXISTS idx_sso_sessions_provider_sid (provider, sid);

ALTER TABLE sso_sessions
    ADD INDEX IF NOT EXISTS idx_sso_sessions_provider_subject (provider, subject);
