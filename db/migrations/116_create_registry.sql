-- The tenancy registry: which zones exist, and which projects live inside them.
--
-- A ZONE is a whole ClickHouse instance. A PROJECT is a tenant inside one zone,
-- and is the dimension every event is filed under. Neither identifier ever
-- reaches ClickHouse SQL as a database or table name — the ClickHouse database
-- is always the one named by CLICKHOUSE_DATABASE — so these rows are a registry,
-- not a schema map.
--
-- PLACEMENT — UNQUALIFIED, so both tables land in the DSN's default database
-- (monitor_auth) alongside users/identities/api_keys, and NOT in the qualified
-- `monitor` schema that 110 created for the issue tables. The reason is the very
-- next migration: api_keys gains a project foreign key, and that key has to
-- point at projects. Keeping both tables in one schema keeps that FK
-- same-schema, written unqualified like every other constraint in 100-106; the
-- alternative pins a cross-schema reference by literal database name into the
-- DDL forever, which then has to survive every future rename or restore into a
-- differently-named schema.
--
-- This deliberately WIDENS the monitor_auth charter. 110 describes monitor_auth
-- as holding "only the relational auth layer (users, identities, refresh_tokens,
-- sso_*, settings, api_keys)"; from here it holds identity AND tenancy. That is
-- a considered change to the charter, not drift — tenancy is bound to api_keys
-- (a project is derived from the authenticating key, never sent by the client),
-- so the registry belongs next to the credential it hangs off, and separating
-- the two would buy tidiness and pay for it in a cross-schema foreign key.
--
--
-- SLUG RULES — lowercase letters, digits and hyphens; must start with a letter;
-- must not end with a hyphen; 3-30 characters. This is the GCP project-id rule
-- in spirit, and it is what makes a slug safe to put in a URL path segment, a
-- DNS label, and a log line without escaping or quoting any of the three.
--
-- The rule is enforced TWICE, on purpose:
--   * in Go, by tools.ValidateSlug, which also rejects the reserved names the
--     frontend's static routes would shadow (a list SQL has no business
--     carrying, because it changes when the frontend's routes change); and
--   * here, by the CHECK constraints below, which are the backstop for anything
--     that reaches the table without going through the query layer — a manual
--     INSERT during an incident, a restore, a future subcommand.
-- The two encodings of the same rule must be kept in step. tools/Slug.tool.go
-- carries the matching note.
--
-- The CHECK forces `COLLATE utf8mb4_bin` before the REGEXP because the column
-- inherits the table's utf8mb4_unicode_ci, under which REGEXP is
-- case-INSENSITIVE — so `[a-z]` would happily accept `Trailblaze` and the
-- constraint would silently enforce nothing about case. Binary collation for the
-- format test, case-insensitive collation for the UNIQUE key, is the combination
-- worth having: uppercase can never be stored at all, and `Trailblaze` can never
-- be minted as a second row that merely looks distinct from `trailblaze`.
--
--
-- IMMUTABILITY AND NON-REUSE — a slug is chosen once and is never edited and
-- never handed to a different owner. Rows are therefore NEVER DELETED: a
-- decommissioned zone or project is set to status='deleted' and the row stays
-- forever, so the UNIQUE key makes reuse structurally impossible rather than
-- merely discouraged.
--
-- The cost of getting this wrong is silent and long-lived. Events carry a 30-day
-- TTL (migrations/001_schema.sql) and monitor.issue_occurrences_daily carries NO
-- TTL at all (migrations/005). Recycling the slug `payments` for a new owner
-- would therefore reattach up to a month of the previous owner's events plus a
-- permanent per-day rollup to the new one — and every reference involved stays
-- syntactically valid, so nothing errors, nothing logs, and the only symptom is
-- a dashboard that is quietly wrong about someone else's data.
--
-- display_name is the mutable, non-unique, human-facing label. It exists so that
-- renaming a team never requires renaming the identifier underneath it, which is
-- the pressure that otherwise makes people want mutable slugs.

CREATE TABLE IF NOT EXISTS zones (
    id           BIGINT                    NOT NULL AUTO_INCREMENT PRIMARY KEY,
    slug         VARCHAR(30)               NOT NULL,
    display_name VARCHAR(255)              NOT NULL,
    status       ENUM('active','deleted')  NOT NULL DEFAULT 'active',
    created_at   DATETIME                  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME                  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_zones_slug (slug),
    CONSTRAINT ck_zones_slug_format CHECK (slug COLLATE utf8mb4_bin REGEXP '^[a-z][a-z0-9-]{1,28}[a-z0-9]$')
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- A project slug is unique WITHIN its zone, not globally: two zones may each own
-- a `payments`, and forcing them to disambiguate would leak one zone's naming
-- into another's. UNIQUE(zone_id, slug) is also the index that serves the
-- zone-scoped lookups and listings, so no separate index on zone_id is needed —
-- the composite's leading column already covers them.
--
-- The foreign key is left at the default ON DELETE RESTRICT, unlike the
-- ON DELETE SET NULL on issues.assignee_user_id in 114. That is the point: a
-- zone row is never deleted, so RESTRICT is the posture that says so — an
-- attempted DELETE fails loudly instead of taking projects (and, through
-- api_keys, every key's project binding) with it.
CREATE TABLE IF NOT EXISTS projects (
    id           BIGINT                    NOT NULL AUTO_INCREMENT PRIMARY KEY,
    zone_id      BIGINT                    NOT NULL,
    slug         VARCHAR(30)               NOT NULL,
    display_name VARCHAR(255)              NOT NULL,
    status       ENUM('active','deleted')  NOT NULL DEFAULT 'active',
    created_at   DATETIME                  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME                  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_projects_zone_slug (zone_id, slug),
    CONSTRAINT fk_projects_zone FOREIGN KEY (zone_id) REFERENCES zones (id),
    CONSTRAINT ck_projects_slug_format CHECK (slug COLLATE utf8mb4_bin REGEXP '^[a-z][a-z0-9-]{1,28}[a-z0-9]$')
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
