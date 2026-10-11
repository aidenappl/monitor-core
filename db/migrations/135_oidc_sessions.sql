-- 135_oidc_sessions.sql
--
-- One row per signed-in browser for SSO (go-oidc-session), keyed by the SHA-256
-- of the mon_sso_session cookie, holding the provider's tokens encrypted with
-- MON_CRYPTO_KEY. /auth/refresh refreshes them against the provider and ends
-- the row when the provider refuses — that is what bounds a Monitor session to
-- the provider session (routes/refresh_sso.go).
--
-- Replaces sso_sessions (one row per user, never refreshed, read only by the
-- introspection checkpoint, now removed). sso_sessions is left in place,
-- unused, rather than dropped here.
--
-- Shape is go-oidc-session's sqlstore sessions table.

CREATE TABLE IF NOT EXISTS oidc_sessions (
    id            CHAR(64)     NOT NULL,
    user_id       BIGINT       NOT NULL,
    provider      VARCHAR(64)  NOT NULL,
    subject       VARCHAR(255) NOT NULL,
    sid           VARCHAR(255) NULL,
    access_token  TEXT         NOT NULL,
    refresh_token TEXT         NULL,
    id_token      TEXT         NULL,
    token_expiry  DATETIME(6)  NULL,
    created_at    DATETIME(6)  NOT NULL,
    last_seen_at  DATETIME(6)  NOT NULL,
    refreshed_at  DATETIME(6)  NOT NULL,
    expires_at    DATETIME(6)  NOT NULL,
    PRIMARY KEY (id),
    KEY idx_user (user_id),
    KEY idx_subject (provider, subject),
    KEY idx_sid (provider, sid),
    KEY idx_expires (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
