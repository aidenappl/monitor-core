-- Give dashboards a project.
--
-- Part of the tenancy completion (127-133). dashboards was one of SEVEN
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
-- NO UNIQUE KEY AND NO INDEX. 123 declined a unique key on name deliberately and
-- that argument is unchanged. No index either: 118's rule is that an index added
-- before a query needs it is one nothing uses — revisit when a zone holds enough
-- dashboards to measure.
--
-- Worth knowing what this fixes: a dashboard's PANELS were already project-scoped
-- (services/analytics.go), so an unscoped list showed another project's dashboard
-- rendering entirely empty — a dashboard that looks broken rather than one that
-- leaks. Annoying rather than dangerous, and now neither.

ALTER TABLE monitor.dashboards
    ADD COLUMN IF NOT EXISTS project VARCHAR(30) NULL AFTER id;

UPDATE monitor.dashboards SET project = 'default' WHERE project IS NULL;

ALTER TABLE monitor.dashboards
    MODIFY COLUMN project VARCHAR(30) NOT NULL;
