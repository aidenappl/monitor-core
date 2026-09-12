-- Give notification channels a project.
--
-- Part of the tenancy completion (127-133). notification_channels was one of SEVEN
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
-- A CHANNEL LOOKS ZONE-LEVEL AND IS NOT. One PagerDuty service per zone is a
-- plausible reading, but channels are referenced BY ID from
-- alert_rules.notification_channel_ids and notification_policies.channel_ids,
-- and both of those become project-scoped here. A project-scoped policy pointing
-- at a zone-scoped channel is a cross-scope reference, and it resolves to either
-- "channel not found" or a send into another tenant's destination. Sharing costs
-- one duplicate row per project and buys an unambiguous graph.
--
-- 126 encrypted this table's config and stopped serving it; this migration is
-- about who can SEE the row at all.

ALTER TABLE monitor.notification_channels
    ADD COLUMN IF NOT EXISTS project VARCHAR(30) NULL AFTER id;

UPDATE monitor.notification_channels SET project = 'default' WHERE project IS NULL;

ALTER TABLE monitor.notification_channels
    MODIFY COLUMN project VARCHAR(30) NOT NULL;

ALTER TABLE monitor.notification_channels
    DROP INDEX IF EXISTS uq_notification_channels_name;

ALTER TABLE monitor.notification_channels
    ADD UNIQUE KEY IF NOT EXISTS uq_notification_channels_project_name (project, name);
