-- Give notification policies a project.
--
-- Part of the tenancy completion (127-133). notification_policies was one of SEVEN
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
-- THIS IS THE LOAD-BEARING ONE OF THE SEVEN. `position` carried a ZONE-GLOBAL
-- unique key, so the routing table was a single ordered list shared by every
-- project — meaning creating a policy in one project renumbers another project's
-- routing order, silently changing where its alerts go. Scoping the key to
-- (project, position) makes each project's ordering its own.
--
-- query.reorderNotificationPolicies' negation trick survives unchanged: negation
-- is injective within a project's rows, and under (project, position) it cannot
-- collide with another project's.

ALTER TABLE monitor.notification_policies
    ADD COLUMN IF NOT EXISTS project VARCHAR(30) NULL AFTER id;

UPDATE monitor.notification_policies SET project = 'default' WHERE project IS NULL;

ALTER TABLE monitor.notification_policies
    MODIFY COLUMN project VARCHAR(30) NOT NULL;

ALTER TABLE monitor.notification_policies
    DROP INDEX IF EXISTS uq_notification_policies_position;

ALTER TABLE monitor.notification_policies
    ADD UNIQUE KEY IF NOT EXISTS uq_notification_policies_project_position (project, position);
