-- Which firing alert routes to which channels, evaluated top-down by `position`.
-- Moves from a ClickHouse ReplacingMergeTree ORDER BY (id) created inside
-- alerts.InitPolicies; 119 carries the argument for the whole move.
--
-- THIS TABLE IS THE REASON THE MOVE WAS WORTH DOING. Everything below is about
-- one column.
--
--
-- THE DEFECT. `position` is an ORDER, and an order is only meaningful if the
-- values are distinct. Two policies at position 2 give alerts/router.go a
-- non-deterministic first match: it iterates `ORDER BY position ASC`, and which
-- of the two ties it sees first decides whether `continue_matching` short-
-- circuits the rest of the list. So a duplicate position does not corrupt data —
-- it silently changes where alerts are sent, per query, with nothing logged and
-- nothing to see in the row.
--
-- Two code paths produced duplicates, and neither could be fixed where it stood:
--
--   * getNextPosition (alerts/policies.go) was `SELECT max(position) … FINAL`
--     followed by +1 in Go. Two concurrent creates read the same max and both
--     write it. Under a ReplacingMergeTree ORDER BY (id) there is no constraint
--     to catch that, and FINAL makes it worse rather than better: max() over a
--     table that has not merged yet can read a stale version of a row.
--   * ReorderPolicies rewrote rows ONE AT A TIME with no transaction, because
--     ClickHouse has none. A failure halfway through a reorder left the list
--     half in the old order and half in the new — a state with duplicates in it
--     by construction, and no way to roll back.
--
-- alerts/AGENTS.md carried both of these as open 🟡 findings. This file closes
-- them.
--
--
-- THE CONSTRAINT: UNIQUE KEY uq_notification_policies_position (position).
--
-- Single-column, and that IS the right composite today, because today the
-- ordering namespace is the whole instance: there is exactly one policy list,
-- shared by every project, evaluated by one evaluator. A composite is only
-- correct once a policy belongs to something — and the moment these tables gain
-- the `project` column 119's header defers, this key becomes
-- UNIQUE (project, position) and ReorderNotificationPolicies must take the
-- project as its scope. Writing the single-column key now and widening it later
-- is the honest order; writing (project, position) against a table with no
-- project column would be a composite over one value, which enforces the same
-- thing while pretending to a generality that does not exist.
--
--
-- CAN REORDERING STILL BE DONE ATOMICALLY? YES — IN TWO PHASES, IN ONE
-- TRANSACTION. This is the part that is not obvious, so it is written out.
--
-- The naive reorder (`for i, id := range ids { UPDATE … SET position = i+1 … }`)
-- does not survive a UNIQUE key. InnoDB checks a unique index per ROW, not per
-- statement and not at commit — MariaDB has no deferrable constraints — so
-- moving the policy at 3 to position 1 collides with the policy already at 1
-- before that one has been moved out of the way. The reorder would fail on
-- roughly every non-trivial permutation.
--
-- query.ReorderNotificationPolicies therefore does this, inside one transaction:
--
--   1. SELECT id … ORDER BY position ASC, id ASC FOR UPDATE
--      — takes the whole list under lock, so no concurrent create or reorder can
--        interleave, which is also what makes getNextPosition safe again.
--   2. UPDATE monitor.notification_policies SET position = -position
--      — vacates the entire positive space in ONE statement. Negation is
--        collision-free because it is injective and the target space is disjoint
--        from the source: every stored position is >= 1, so every negated value
--        is <= -1, and distinct inputs stay distinct. This is why the column is
--        SIGNED INT and carries no CHECK (position > 0): the staging space is
--        the negative half, and a CHECK would forbid the one statement that
--        makes the whole thing possible.
--   3. UPDATE … SET position = ? WHERE id = ?, once per policy, assigning
--      1..N in the new order. Every target is free — all rows are negative — so
--      no intermediate state can violate the key.
--   4. COMMIT. The negative positions never exist outside this transaction:
--      InnoDB's REPEATABLE READ means no other session ever reads them, and a
--      failure at any step rolls the whole list back to the order it had.
--
-- The caller may name a SUBSET of the policies. Those are placed first, in the
-- order given; every policy not named keeps its relative order and follows.
-- That is well-defined for any input, always satisfies the key, and is identical
-- to the old behaviour when the caller sends the full list (which monitor-web's
-- drag-reorder does). The alternative — rejecting a partial list — would be an
-- API break in exchange for nothing.
--
--
-- THE BACKFILL RENUMBERS. cutover.BackfillConfig cannot copy the ClickHouse
-- positions verbatim, because the whole premise here is that they may not be
-- unique. It reads them in (position, created_at, id) order and assigns dense
-- 1..N, so the ORDER is preserved exactly and the VALUES are not. Absolute
-- position numbers have never meant anything to anything — router.go only ever
-- sorts by them.
--
--
-- THE SEED MOVED, AND ITS GATE CHANGED. seedDefaultPolicies used to run inside
-- InitPolicies and insert four default policies when no row had is_default = 1.
-- It is now query.SeedDefaultNotificationPolicies, still called from alerts.Init,
-- and it fires only when the table is COMPLETELY EMPTY.
--
-- The new gate says what it means: seed a FRESH INSTALL. "No row is marked
-- is_default" was a proxy for that, and the two are not the same statement — the
-- proxy holds only for as long as whatever populated the table happens to have
-- carried is_default = 1. cutover.BackfillConfig copies the four existing
-- ClickHouse defaults across with their own ids, so a backfilled install must
-- never seed on top of them; "the table has rows" says that directly.
--
-- WHAT THE GATE DOES NOT DO IS MAKE THE DEPLOY ORDER SAFE, and that is worth
-- being exact about rather than hopeful. If the new binary serves BEFORE the
-- backfill runs, it seeds four defaults into an empty table and the backfill then
-- APPENDS the four ClickHouse originals after them — nothing collides, because
-- the backfill renumbers positions densely from MAX+1. The result is eight
-- policies, four of which match every alert by priority and route it nowhere.
-- That is a silent misconfiguration, not an error. It is one of the two reasons
-- the deployment order in AGENTS.md §8 is written down; the louder one is that an
-- unbackfilled alert_rules leaves the evaluator with nothing to evaluate.
-- Recovery is to delete the four seeded rows by hand.
--
-- It is deliberately NOT seeded here in SQL, unlike 117's registry seed. 117 had
-- no choice — the rows had to exist before any Go code ran. Here the opposite is
-- true: db.RunMigrations runs BEFORE the `backfill-config` subcommand in the same
-- process, so an SQL seed would fire first and put those four rows in the way of
-- every cutover.
--
-- KNOWN, UNCHANGED: repeat_interval_seconds is stored and surfaced and never
-- read — the re-notify cadence comes from the rule's cooldown_seconds. It is
-- carried across rather than dropped so this stays a move; alerts/AGENTS.md
-- keeps it as an open finding.

CREATE TABLE IF NOT EXISTS monitor.notification_policies (
    id                      CHAR(36)        NOT NULL PRIMARY KEY,
    name                    VARCHAR(255)    NOT NULL,
    description             VARCHAR(1000)   NOT NULL DEFAULT '',
    position                INT             NOT NULL,
    matchers                JSON            NOT NULL,
    channel_ids             JSON            NOT NULL,
    continue_matching       TINYINT(1)      NOT NULL DEFAULT 0,
    repeat_interval_seconds INT UNSIGNED    NOT NULL DEFAULT 0,
    enabled                 TINYINT(1)      NOT NULL DEFAULT 1,
    is_default              TINYINT(1)      NOT NULL DEFAULT 0,
    created_at              DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at              DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uq_notification_policies_position (position)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- No UNIQUE on `name`, unlike alert_rules (119) and notification_channels (120),
-- and the difference is what the name is FOR. A rule's name goes out in every
-- notification and into alert_history; a channel's name is how it is picked. A
-- policy's name is read only in the policy list, next to the position that
-- actually identifies it, so a duplicate is untidy rather than ambiguous — and
-- the live row count here is unknown, which makes an aborting backfill the more
-- likely outcome than a caught bug.
