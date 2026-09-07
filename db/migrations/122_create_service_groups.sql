-- Named sets of services, referenced by a notification policy's
-- `matchers.service_group`. Moves from a ClickHouse ReplacingMergeTree ORDER BY
-- (id) created inside alerts.InitServiceGroups; 119 carries the argument for the
-- whole move.
--
-- `services` is a JSON array of service names. alerts.ResolveServiceGroups
-- Unmarshals it on every routed alert and SKIPS any group whose value fails to
-- parse (`continue`), so an invalid array does not error — it silently makes the
-- group match nothing, and a policy keyed on that group stops routing. JSON
-- rather than TEXT turns that into a rejected write.
--
-- UNIQUE (name). A policy stores a group's ID, so a duplicate name cannot
-- misroute anything at runtime — but the policy editor picks a group by name,
-- and two "payments" groups with different member lists is a misconfiguration
-- waiting to be made rather than one already made. The live row count is
-- unknown, so if this key aborts the backfill the remedy is to rename one group
-- and re-run; the constraint is worth that.

CREATE TABLE IF NOT EXISTS monitor.service_groups (
    id          CHAR(36)        NOT NULL PRIMARY KEY,
    name        VARCHAR(255)    NOT NULL,
    description VARCHAR(1000)   NOT NULL DEFAULT '',
    services    JSON            NOT NULL,
    created_at  DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at  DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uq_service_groups_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
