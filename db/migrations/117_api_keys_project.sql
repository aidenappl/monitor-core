-- Binds every API key to a project. This is the join that makes tenancy real:
-- the project an event is filed under is DERIVED from the authenticating
-- api_keys row and overwritten on ingest, never trusted from the client, so
-- without a column here there is nothing server-side to derive it FROM.
--
--
-- uq_api_keys_key_hash STAYS GLOBALLY UNIQUE. READ THIS BEFORE "FIXING" IT.
--
-- Everything else in this phase moves from global to per-project: name becomes
-- unique per project, the scope index gains a project_id prefix, lookups get
-- scoped. key_hash is the one key that must NOT follow that pattern, and the
-- reason is the direction the lookup runs in. Authentication starts from a raw
-- string in an X-Api-Key header and nothing else — no project, no zone, no
-- tenant hint of any kind, because the project is the ANSWER the lookup
-- produces, not an input it is given. A presented key therefore has to resolve
-- to exactly one row across the entire instance.
--
-- Composite it to (project_id, key_hash) and two projects can hold rows with the
-- same hash. The lookup by hash alone then matches both, and whichever the cache
-- happened to write last decides whose project every event from that credential
-- is stamped with — silently, with no error anywhere, and reversing per refresh.
-- That is cross-tenant data attribution decided by map iteration order.
--
-- keyring-api has the lived version of this exact mistake: no UNIQUE index on
-- secret_key at all, so duplicate keys are permitted and the OLDEST silently
-- wins injection, which means a "replacement" secret never takes effect and the
-- access logs cannot tell you which row is live. The failure is invisible until
-- someone reasons backwards from wrong data. Do not reproduce it here.
--
--
-- RE-RUNNABILITY is required, not decorative — the same rule 107 wrote down.
-- db.RunMigrations records a file as applied only after a clean Exec, and DDL
-- commits implicitly, so a run that dies partway is retried IN FULL on the next
-- boot with the successful half already durable. Every statement below is
-- therefore guarded (IF NOT EXISTS / IF EXISTS / INSERT IGNORE) or naturally
-- idempotent, and the WHERE project_id IS NULL predicate makes the seed and the
-- backfill no-ops on every run after the first.
--
--
-- WHY THE SEED IS HERE AND NOT LEFT TO bootstrap.EnsureZoneAndProject.
--
-- Boot order is: db.RunMigrations() -> bootstrap.EnsureZoneAndProject(). 116
-- creates zones and projects EMPTY, this file runs next, and the Go seeder only
-- runs after both. So at the moment the backfill needs a project id, there is no
-- project row and no Go code has run that could make one.
--
-- That ordering cannot simply be swapped: the seeder needs the tables 116
-- creates, and the runner applies its files as one batch. The alternative is a
-- nullable project_id backfilled later in Go — rejected, because MariaDB's
-- UNIQUE ignores NULLs, so uq_api_keys_project_name would enforce nothing at all
-- for exactly the rows that had not been bound yet, and the FK would permit a
-- key with no tenant to authenticate and ingest under a null project. NOT NULL
-- from the start is the only shape where the constraints mean what they say.
--
-- The slugs below are LITERALS because SQL cannot read the environment, and they
-- are the compiled-in defaults of MON_ZONE_SLUG and MON_DEFAULT_PROJECT
-- (env/env.go). 'trailblaze' is what the existing data actually is — all 15
-- services currently reporting to this instance are Trailblaze services — and
-- 'default' is what every api_keys row binds to, following the Mimir
-- "anonymous" / Loki "fake" precedent that the tenant dimension is never null.
--
-- The seed is gated on `EXISTS (SELECT 1 FROM api_keys WHERE project_id IS
-- NULL)` — i.e. it fires only for an EXISTING install with unbound keys. A fresh
-- install has no keys, so nothing is seeded here and bootstrap is left free to
-- create whatever slugs the operator chose in the env. The one case that does
-- not resolve cleanly is an existing install whose operator ALSO overrides
-- MON_ZONE_SLUG / MON_DEFAULT_PROJECT on the same deploy: this file binds the
-- old keys to trailblaze/default, bootstrap then creates the operator's zone
-- alongside, and the keys must be re-pointed by hand afterwards. That is
-- visible, recoverable and loud in the registry listing, which is the correct
-- trade against silently binding three live production credentials to a tenant
-- that does not exist yet.

-- Step 1 — the column, NULLable for the length of this file only.
-- Expand-then-tighten: the backfill needs somewhere to write before NOT NULL can
-- be true, so the column is born nullable and is tightened in step 4, three
-- statements later. It is never nullable at rest.
ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS project_id BIGINT NULL AFTER scope;

-- Step 2 — seed the registry, but only to serve unbound keys (see header).
--
-- INSERT IGNORE rather than a NOT EXISTS subquery on zones: MySQL/MariaDB
-- restrict selecting from the table an INSERT targets, and uq_zones_slug already
-- expresses the thing the subquery would be checking. The gate reads api_keys,
-- a different table, so it is never caught by that restriction.
INSERT IGNORE INTO zones (slug, display_name)
SELECT 'trailblaze', 'Trailblaze' FROM DUAL
WHERE EXISTS (SELECT 1 FROM api_keys WHERE project_id IS NULL);

INSERT IGNORE INTO projects (zone_id, slug, display_name)
SELECT z.id, 'default', 'Default'
FROM zones z
WHERE z.slug = 'trailblaze'
  AND EXISTS (SELECT 1 FROM api_keys WHERE project_id IS NULL);

-- Step 3 — backfill EVERY existing key. Three live keys today ("Trailblaze
-- Ingest", "Trailblaze Web Ingest", "Claude MCP Aiden's MacBook Pro"), and all
-- three must keep authenticating across this deploy, so nothing here filters by
-- scope, age or name.
--
-- The zone is joined as well as the project because a project slug is unique
-- only WITHIN its zone. Matching on 'default' alone is correct today, with one
-- zone, and silently binds to an arbitrary zone's project the moment there are
-- two.
UPDATE api_keys ak
    JOIN projects p ON p.slug = 'default'
    JOIN zones z ON z.id = p.zone_id AND z.slug = 'trailblaze'
SET ak.project_id = p.id
WHERE ak.project_id IS NULL;

-- Step 4 — tighten to NOT NULL.
--
-- This is deliberately the loud step. Under the strict sql_mode MariaDB 11.4
-- defaults to, a row the backfill failed to reach aborts the ALTER instead of
-- being coerced to 0 — and 0 would then fail the foreign key in step 6 anyway,
-- one statement later and much harder to read. A key that cannot be bound to a
-- tenant must stop the boot, not ingest under a placeholder.
ALTER TABLE api_keys
    MODIFY COLUMN project_id BIGINT NOT NULL;

-- Step 5 — the two project-scoped indexes.
--
-- UNIQUE(project_id, name) is new behaviour, not a tightening: there is no
-- uniqueness on name today at all. Per-project rather than global is the whole
-- point — every project should be able to own a key called literally "ingest"
-- without prefixing its own name into it, which is the workaround a global
-- unique key would force and the reason names drift into "acme-ingest-prod-2".
--
-- idx_api_keys_scope is REPLACED rather than kept alongside. It indexes a
-- two-value column across the whole table, so its selectivity is ~50% and the
-- optimiser has almost no reason to choose it; the composite answers the same
-- question and the questions actually asked now ("this project's keys", "this
-- project's ingest keys"), with project_id leading so it serves a project_id-only
-- lookup too.
--
-- Both are added BEFORE the foreign key on purpose: InnoDB silently
-- auto-creates an index on the referencing column when no usable one exists, so
-- adding the FK first would leave a redundant index on (project_id) behind
-- forever, and nothing would ever flag it.
ALTER TABLE api_keys
    ADD UNIQUE KEY IF NOT EXISTS uq_api_keys_project_name (project_id, name);

ALTER TABLE api_keys
    ADD INDEX IF NOT EXISTS idx_api_keys_project_scope (project_id, scope);

ALTER TABLE api_keys
    DROP INDEX IF EXISTS idx_api_keys_scope;

-- Step 6 — the foreign key, dropped-then-added for re-runnability exactly as
-- 114 does it.
--
-- Left at the default ON DELETE RESTRICT, matching fk_projects_zone in 116 and
-- deliberately NOT the ON DELETE SET NULL that 114 chose for issue assignees.
-- The distinction is what the pointer means: an unassigned issue is a normal
-- state, whereas an API key with no project is a credential that authenticates
-- into no tenant. Projects are never deleted (they are retired to
-- status='deleted' and kept forever), so RESTRICT is the posture that says so —
-- an attempted DELETE fails loudly instead of quietly unbinding live
-- credentials.
ALTER TABLE api_keys
    DROP FOREIGN KEY IF EXISTS fk_api_keys_project;

ALTER TABLE api_keys
    ADD CONSTRAINT fk_api_keys_project
        FOREIGN KEY (project_id) REFERENCES projects (id);
