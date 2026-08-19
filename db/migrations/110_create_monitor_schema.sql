-- The issue-tracking tables live in their own `monitor` schema rather than in
-- `monitor_auth`, which holds only the relational auth layer (users, identities,
-- refresh_tokens, sso_*, settings, api_keys) and would be a misleading home for
-- them.
--
-- Same MariaDB host and the same db.SQL pool, so cross-schema joins to
-- monitor_auth.users (for assignee) work natively — no second connection, no
-- second DSN. migrations_applied stays in monitor_auth.
--
-- The DSN user needs privileges on BOTH schemas. Granting `ALL PRIVILEGES ON
-- monitor.*` covers the CREATE below as well, since a database-level CREATE
-- grant permits creating that specific database.
--
-- NOTE: ClickHouse also has a database called `monitor` — same name, different
-- engine. In this repo a bare `monitor.<table>` in a squirrel query means
-- MariaDB; the ClickHouse one is reached through db.Conn.

CREATE DATABASE IF NOT EXISTS monitor
    DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
