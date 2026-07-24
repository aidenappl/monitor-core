CREATE TABLE IF NOT EXISTS refresh_tokens (
    id          BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id     BIGINT        NOT NULL,
    token_hash  BINARY(32)    NOT NULL,
    family_id   BINARY(16)    NOT NULL,
    replaced_by BIGINT        NULL,
    user_agent  VARCHAR(512)  NULL,
    ip          VARBINARY(16) NULL,
    expires_at  DATETIME      NOT NULL,
    revoked_at  DATETIME      NULL,
    inserted_at DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_refresh_tokens_hash (token_hash),
    INDEX idx_refresh_tokens_family (family_id),
    INDEX idx_refresh_tokens_user (user_id),
    CONSTRAINT fk_refresh_tokens_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
