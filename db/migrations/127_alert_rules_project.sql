-- Give alert rules a project.
--
-- Part of the tenancy completion (127-133). alert_rules was one of SEVEN
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
-- THE UNIQUE KEY IS NOT WEAKENED BY THIS, it is corrected. 119 required a unique
-- name because "a rule is chosen by name in the policy editor, and two rows
-- sharing one makes the choice meaningless" — that is a PER-TENANT argument, so
-- per-project uniqueness is its right strength. Zone-global meant two projects
-- could not each own a rule called "high error rate", which is the obvious name
-- in both.

ALTER TABLE monitor.alert_rules
    ADD COLUMN IF NOT EXISTS project VARCHAR(30) NULL AFTER id;

UPDATE monitor.alert_rules SET project = 'default' WHERE project IS NULL;

ALTER TABLE monitor.alert_rules
    MODIFY COLUMN project VARCHAR(30) NOT NULL;

ALTER TABLE monitor.alert_rules
    DROP INDEX IF EXISTS uq_alert_rules_name;

ALTER TABLE monitor.alert_rules
    ADD UNIQUE KEY IF NOT EXISTS uq_alert_rules_project_name (project, name);
