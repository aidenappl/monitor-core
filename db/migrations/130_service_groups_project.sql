-- Give service groups a project.
--
-- Part of the tenancy completion (127-133). service_groups was one of SEVEN
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
-- A service group names services, and service names are unique only within one
-- project's event stream — two tenants each running `api` is the ordinary case.

ALTER TABLE monitor.service_groups
    ADD COLUMN IF NOT EXISTS project VARCHAR(30) NULL AFTER id;

UPDATE monitor.service_groups SET project = 'default' WHERE project IS NULL;

ALTER TABLE monitor.service_groups
    MODIFY COLUMN project VARCHAR(30) NOT NULL;

ALTER TABLE monitor.service_groups
    DROP INDEX IF EXISTS uq_service_groups_name;

ALTER TABLE monitor.service_groups
    ADD UNIQUE KEY IF NOT EXISTS uq_service_groups_project_name (project, name);
