CREATE TABLE IF NOT EXISTS users (
    id             BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email          VARCHAR(254) NOT NULL,
    email_verified TINYINT(1)   NOT NULL DEFAULT 0,
    name           VARCHAR(255) NULL,
    display_name   VARCHAR(255) NULL,
    role           VARCHAR(20)  NOT NULL DEFAULT 'viewer',
    active         TINYINT(1)   NOT NULL DEFAULT 1,
    updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    inserted_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_users_email (email),
    INDEX idx_users_active (active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
