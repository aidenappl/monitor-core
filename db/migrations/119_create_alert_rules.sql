-- The alerting/dashboard CONFIGURATION tables move from ClickHouse to MariaDB.
-- This file is the first of six (119-124) and carries the argument for all of
-- them; the others state only what is specific to their own table.
--
--
-- THE ARGUMENT IS 324f612's ARGUMENT, AND IT HAS NOT CHANGED.
--
-- Eight tables were created by ad-hoc `CREATE TABLE IF NOT EXISTS` inside package
-- Init() functions at boot, with no migration file anywhere — alert_rules,
-- alert_states, alert_history and notification_channels in alerts/alerts.go,
-- notification_policies in alerts/policies.go, service_groups in
-- alerts/service_groups.go, dashboards in dashboards/dashboards.go, saved_views
-- in views/views.go. All were MergeTree or ReplacingMergeTree ORDER BY (id).
--
-- 111 already condemned that shape for the issue row and said why: ClickHouse
-- cannot enforce uniqueness, and a ReplacingMergeTree deduplicates only when a
-- background merge happens to run. In production that produced four rows for one
-- fingerprint and a counter that drifted under concurrent writers. The engine is
-- not at fault — it is a column store for immutable facts, and every one of
-- these tables was using it to hold mutable configuration that a human edits.
--
-- The symptoms here are the same family. Every "update" is an INSERT of a whole
-- new version that is only eventually collapsed, so a read without FINAL can see
-- a stale row and a read with FINAL pays a merge on every query. `ALTER TABLE …
-- DELETE` is an asynchronous mutation, so a deleted rule can still be returned
-- by the very next request. And nothing anywhere could express "no two
-- notification policies may share a position", which is the defect this move
-- exists to make impossible (see 121).
--
--
-- MOVE SIX, KEEP TWO. The split is between CONFIGURATION and FACTS.
--
-- MOVED to MariaDB (119-124):
--   alert_rules            what to watch for       — edited by humans, uniqueness-bearing
--   notification_channels  where to send it        — edited by humans
--   notification_policies  which alert routes where — ORDERED, and the order must be unique
--   service_groups         named sets of services  — edited by humans
--   dashboards             saved dashboard layouts — edited by humans
--   saved_views            saved query filters     — edited by humans
--
-- KEPT in ClickHouse (alerts/alerts.go, unchanged):
--   alert_states    one row per rule per evaluation, rewritten every 15 seconds
--   alert_history   one row per firing/resolved transition, append-only
--
-- Those two are per-evaluation FACTS, not configuration. They are written by a
-- timer rather than by a person, they are time-series shaped, and they are read
-- as a series rather than as a row — exactly like issue occurrences, which 111
-- also left in ClickHouse. alert_history additionally carries
-- `TTL toDate(created_at) + INTERVAL 90 DAY`, which is a ClickHouse feature with
-- no MariaDB equivalent short of a scheduled DELETE; moving it would mean
-- inheriting the job of expiring it. It stays, and the TTL stays with it.
--
-- The consequence, stated plainly: a rule and its state now live in two
-- different stores, so `alerts.ListRules` reads MariaDB for the rules and
-- ClickHouse for the states and joins them in Go. That is why ListRules, GetRule
-- and DeleteRule remain functions on the alerts package rather than collapsing
-- into the query layer — they are genuine cross-store orchestration, which is
-- the one case the house rules allow a layer above query/ for.
--
-- KNOWN INCONSISTENCY, recorded rather than left to be discovered: alert_states
-- and alert_history are still created by an ad-hoc CREATE TABLE inside
-- alerts.Init, with no migration file, because the repo has no migration runner
-- for ClickHouse DDL that is not in migrations/*.sql and adding those two files
-- is a separate change with its own re-runnability story. They are the last two
-- tables in the repo created that way.
--
--
-- WHY THE `monitor` SCHEMA AND NOT `monitor_auth`.
--
-- 110 created `monitor` for the issue tables and described `monitor_auth` as
-- holding "only the relational auth layer". 116 then widened monitor_auth to
-- identity AND tenancy, for a specific reason: `projects` had to sit next to
-- `api_keys` so that fk_api_keys_project could be written same-schema instead of
-- pinning a literal database name into the DDL forever.
--
-- That reason does not apply here. None of these six tables carries a foreign
-- key into monitor_auth — they reference no user, no api key and no project (see
-- below) — so nothing pulls them toward that schema. What they do sit beside is
-- monitor.issues and monitor.service_repos: the observability subsystem's own
-- operational data. 115 already established that `monitor` holds more than
-- issues; service_repos is configuration by any reading of the word.
--
-- So the rule this settles, for whoever adds the next table: monitor_auth holds
-- who you are and which tenant you belong to; `monitor` holds what Monitor
-- observes and how it is configured to observe it.
--
--
-- NO PROJECT COLUMN. THIS IS A DEFERRAL WITH A DATE ON IT, NOT AN OVERSIGHT.
--
-- 117 and 118 gave api_keys and issues a tenant. These six tables do not get
-- one, and the reason is that adding it would be a behaviour change rather than
-- a move: every one of these surfaces is served by a handler that has a project
-- on its request context and currently ignores it, so a project column would
-- immediately partition six live configuration sets between tenants that today
-- share them. The alert evaluator's zone-wide gap (see the KNOWN GAP header in
-- alerts/evaluator.go) is the same question from the other end, and it is
-- explicitly the phase-after-next's.
--
-- What this move DOES do is make that change cheap when it comes: adding
-- `project VARCHAR(30) NOT NULL` and widening the unique keys to lead with it is
-- one migration per table, against a store that can actually enforce the result.
-- Under ReplacingMergeTree it was not expressible at all.
--
--
-- RE-RUNNABILITY, the rule 107, 117 and 118 already wrote down: db.RunMigrations
-- records a file as applied only after a clean Exec, and DDL commits implicitly,
-- so a run that dies partway is retried IN FULL on the next boot with the
-- successful half already durable. Every statement in 119-124 is a guarded
-- CREATE TABLE IF NOT EXISTS and is therefore naturally idempotent.
--
--
-- THE CLICKHOUSE ORIGINALS ARE LEFT IN PLACE, unread by the new code, exactly as
-- issues/issues.go left its table. `monitor-core backfill-config` copies them
-- across and can be re-run; that is only true while they still exist. Drop them
-- by hand once a release has passed — see cutover/config.go.

-- `condition` IS A RESERVED WORD in MariaDB (it is the DECLARE … CONDITION of
-- stored programs). It is backticked here, and it must be backticked at every
-- UNQUALIFIED use in the query layer — an INSERT column list, an UPDATE SET
-- clause. A QUALIFIED use does not need it: MariaDB's documented exception is
-- that a word following a period in a qualified name is always read as an
-- identifier, so `monitor.alert_rules.condition` in a SELECT list is legal bare.
-- The column keeps the name because the JSON contract monitor-web reads says
-- `condition` and the ClickHouse table the backfill reads says `condition`;
-- renaming it here would put a translation step in both. TestAlertRuleSQLQuotes-
-- ReservedWords pins the backticks so this cannot regress into a syntax error
-- that only appears on the write path.
--
-- The four enum-shaped columns become real ENUMs. They were `String DEFAULT …`
-- in ClickHouse and were policed only by Go maps in alerts.CreateRule; those
-- maps stay, because a 400 naming the field is a better answer than errno 1265,
-- but they are no longer the only thing standing between a typo and a rule that
-- silently never fires. `metric` is the one worth pointing at: an unrecognised
-- value fell through buildAggExpr's `default:` to count(), so a rule asking for
-- max() and holding a misspelling would evaluate as a count and look fine.
--
-- priority ENUM('P0','P1','P2','P3') is where the discarded ALTER goes. The old
-- code carried an inline
--   _ = db.Conn.Exec(ctx, "ALTER TABLE …alert_rules ADD COLUMN IF NOT EXISTS
--                          priority String DEFAULT 'P2'")
-- inside Init, whose error was thrown away with `_ =`. A schema change that
-- cannot report failure is a schema change you find out about from the first
-- read that returns nothing. It is a column in a migration now, and the discard
-- is not ported.
--
-- query_filters and notification_channel_ids are JSON, not TEXT, and the
-- distinction is deliberate: the SERVER parses both (parseRuleFilters,
-- router.Route), so a value that is not valid JSON is a rule that cannot be
-- evaluated. json_valid is MariaDB enforcing something the code already needs.
-- The query layer checks them first so the caller gets "query_filters must be
-- valid JSON" rather than errno 4025 (`CONSTRAINT … failed`). Client-owned blobs
-- the server never parses — dashboards.config, saved_views.query_params — are
-- deliberately left as TEXT; see 123.
--
-- UNIQUE (name) is new behaviour and is worth being explicit about, because it
-- can abort the backfill. A rule's name is not a label: it is copied verbatim
-- into every notification ("Alert 'X' is firing"), into alert_history.rule_name,
-- and into the SSE payload the dashboard renders. Two rules sharing one is an
-- alert nobody can attribute. Six rules exist on the live instance at the time
-- of writing; if two share a name the backfill stops with errno 1062 naming this
-- key, and the fix is to rename one before re-running — which is the loud
-- failure that is wanted, not a reason to drop the constraint.

CREATE TABLE IF NOT EXISTS monitor.alert_rules (
    id                          CHAR(36)                                        NOT NULL PRIMARY KEY,
    name                        VARCHAR(255)                                    NOT NULL,
    description                 VARCHAR(1000)                                   NOT NULL DEFAULT '',
    type                        ENUM('threshold','absence','rate_change')       NOT NULL,
    priority                    ENUM('P0','P1','P2','P3')                       NOT NULL DEFAULT 'P2',
    query_filters               JSON                                            NOT NULL,
    metric                      ENUM('count','sum','avg','min','max')           NOT NULL DEFAULT 'count',
    field                       VARCHAR(255)                                    NOT NULL DEFAULT '',
    `condition`                 ENUM('gt','lt','gte','lte','eq')                NOT NULL,
    threshold                   DOUBLE                                          NOT NULL DEFAULT 0,
    evaluation_interval_seconds INT UNSIGNED                                    NOT NULL DEFAULT 60,
    for_seconds                 INT UNSIGNED                                    NOT NULL DEFAULT 0,
    cooldown_seconds            INT UNSIGNED                                    NOT NULL DEFAULT 300,
    notification_channel_ids    JSON                                            NOT NULL,
    enabled                     TINYINT(1)                                      NOT NULL DEFAULT 1,
    created_at                  DATETIME(3)                                     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at                  DATETIME(3)                                     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uq_alert_rules_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- No index on `enabled`, deliberately, even though listEnabledRules filters on
-- it every 15 seconds. It is a two-value column, so its selectivity is ~50% and
-- the optimiser has almost no reason to choose it — the same argument 117 makes
-- for replacing idx_api_keys_scope. The table holds six rows on the live
-- instance; a full scan of six rows is not a query plan worth an index, and 118
-- already wrote down the rule that an index added before a query needs it is one
-- nothing uses and nothing flags.
