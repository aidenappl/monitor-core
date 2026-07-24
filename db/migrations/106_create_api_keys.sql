-- Ported from the ClickHouse api_keys table (id/name/key_hash/key_prefix/scope).
-- Keeps the string UUID id and admin|ingest scope semantics used by the
-- apikeys package and X-Api-Key auth path.
CREATE TABLE IF NOT EXISTS api_keys (
    id           VARCHAR(36)  NOT NULL PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    key_hash     CHAR(64)     NOT NULL,
    key_prefix   VARCHAR(32)  NOT NULL,
    scope        VARCHAR(20)  NOT NULL DEFAULT 'admin',
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at DATETIME     NULL,
    UNIQUE KEY uq_api_keys_key_hash (key_hash),
    INDEX idx_api_keys_scope (scope)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
