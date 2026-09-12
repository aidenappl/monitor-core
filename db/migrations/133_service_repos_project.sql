-- Give service->repo mappings a project.
--
-- Part of the tenancy completion (127-133). service_repos was one of SEVEN
-- monitor.* tables carrying no tenancy column at all, so every list returned
-- every project's rows and `grep "scope\."` found zero hits across all of their
-- query files. Only monitor.issues (118) was ever partitioned.
--
-- THE BACKFILL VALUE IS THE LITERAL 'default', for 118's reason: SQL cannot read
-- the environment, and 'default' is MON_DEFAULT_PROJECT's compiled-in default
-- (env/env.go). An install that overrode it gets a loud warning from
-- bootstrap.VerifyConfigTenancy rather than a silent mis-attribution.
--
-- EVERY STATEMENT IS INDIVIDUALLY IDEMPOTENT, and that is required rather than
-- tidy: db/sql.go records a migration only after the WHOLE file succeeds, and
-- MariaDB commits DDL implicitly — so a file that fails part-way is retried FROM
-- THE TOP on the next boot, with its earlier statements already applied.
--
-- The `MODIFY ... NOT NULL` is the loud step. Under MariaDB's strict sql_mode a
-- row the UPDATE missed aborts the boot here, rather than being coerced to '' and
-- becoming a row no project can ever see.
--
-- THE AWKWARD ONE: this table's PRIMARY KEY is `service` alone (115), so it needs
-- a real key change rather than an added index. That PK is itself the bug — a
-- service name is unique only within one project's event stream, so as written
-- two tenants both running `api` cannot map it to different repositories.
--
-- The column is added FIRST so the composite PK reads (project, service) in a
-- natural order.
--
-- DROP + ADD in ONE statement: MariaDB applies it atomically, so the table is
-- never left without a primary key, and re-running it on an already-migrated
-- table drops and re-adds the identical key — which is what makes a retry safe.
--
-- idx_service_repos_lookup (provider, owner, repo) is deliberately LEFT ALONE.
-- The GitHub webhook resolves a repo to its services without a project in hand —
-- GitHub presents no tenant — so that lookup stays cross-project within the zone
-- and the handler scopes each issue it touches instead.

ALTER TABLE monitor.service_repos
    ADD COLUMN IF NOT EXISTS project VARCHAR(30) NULL FIRST;

UPDATE monitor.service_repos SET project = 'default' WHERE project IS NULL;

ALTER TABLE monitor.service_repos
    MODIFY COLUMN project VARCHAR(30) NOT NULL;

ALTER TABLE monitor.service_repos
    DROP PRIMARY KEY, ADD PRIMARY KEY (project, service);
