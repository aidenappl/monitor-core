CREATE TABLE IF NOT EXISTS sso_sessions (
    user_id         BIGINT       NOT NULL PRIMARY KEY,
    provider        VARCHAR(64)  NOT NULL,
    access_token    TEXT         NOT NULL,
    refresh_token   TEXT         NULL,
    last_checked_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    inserted_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_sso_sessions_last_checked (last_checked_at),
    CONSTRAINT fk_sso_sessions_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
