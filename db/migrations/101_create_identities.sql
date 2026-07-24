CREATE TABLE IF NOT EXISTS identities (
    id               BIGINT         NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id          BIGINT         NOT NULL,
    provider         VARCHAR(64)    NOT NULL,
    provider_user_id VARCHAR(255)   NOT NULL,
    provider_email   VARCHAR(320)   NULL,
    email_verified   TINYINT(1)     NOT NULL DEFAULT 0,
    password_hash    VARBINARY(255) NULL,
    identity_data    JSON           NULL,
    last_login_at    DATETIME       NULL,
    inserted_at      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_identities_provider_sub (provider, provider_user_id),
    INDEX idx_identities_user (user_id),
    CONSTRAINT fk_identities_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
