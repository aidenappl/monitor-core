-- Which source repository each reporting service is built from.
--
-- Monitor watches many services across more than one GitHub org, so an issue on
-- `scraper-service` and an issue on `website` do not belong to the same repo —
-- and neither belongs to monitor-core. Without this mapping a linked PR is just
-- a URL someone pasted, and nothing can answer "where does this error live".
--
-- The mapping is MANY-TO-ONE and cannot be derived from the service name. The
-- estate runs `auth-service-v1` alongside `auth-service-v2`, and `team-service`
-- at v1, v2 and v3 — all versions of one service, each reporting under its own
-- name, all built from a single repo. Stripping a `-vN` suffix would be a guess
-- that silently mislabels anything not following the convention, so the mapping
-- is explicit. `service` is the primary key; several rows may share one
-- owner/repo, and that is the normal case.
--
-- A service with no row here is simply unmapped: links still work, they just
-- cannot be resolved from a bare `#123`. Nothing fails.
--
-- default_branch is recorded because GitHub only honours PR closing keywords on
-- the default branch, so any future "did this fix ship" logic needs to know what
-- that branch is per repo rather than assuming `main`.

CREATE TABLE IF NOT EXISTS monitor.service_repos (
    service        VARCHAR(255)   NOT NULL PRIMARY KEY,
    provider       ENUM('github') NOT NULL DEFAULT 'github',
    owner          VARCHAR(255)   NOT NULL,
    repo           VARCHAR(255)   NOT NULL,
    default_branch VARCHAR(255)   NULL,
    inserted_at    DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at     DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    KEY idx_service_repos_lookup (provider, owner, repo)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
