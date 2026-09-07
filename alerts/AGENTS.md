# AGENTS.md — alerts/ (monitor-core)

The alerting subsystem: rules, the evaluation loop, notification channels, routing
policies, service groups, alert history, and the alert SSE hub. Read the root
`../AGENTS.md` first for repo-wide conventions.

**Since migrations 119-122 this package no longer owns its own storage.** Alert rules,
notification channels, notification policies and service groups are MariaDB rows whose SQL
lives in `query/*.query.go` and whose types live in `structs/`. What is left here is the
behaviour: the evaluation loop, the routing decision, the notifier, the SSE hub, and the two
ClickHouse tables that did NOT move (`alert_states`, `alert_history` — see root §6
*Stores*). Handlers in `routes/alerts.go` call `query.` directly for the pure-MariaDB CRUD
and `alerts.` only for the three operations that genuinely span both stores.

**This package is data plane only.** Under `MON_ROLE=app` (root `../AGENTS.md` §6 *Roles*)
`alerts.Init` never runs, so `alert_states` and `alert_history` are not created; the
evaluator goroutine and the alert SSE hub are never started; `routes.AlertNotifHub` stays
nil; and `buildRouter` registers none of the alert-rules / notification-channels /
service-groups / notification-policies / alert-history routes or `/v1/alerts/stream`.
Under the default `MON_ROLE=both` — the deployed configuration — all of it runs exactly as
before. Note that this is the *whole* alerting surface, including the four groups whose
storage is now pure MariaDB: they are gated with the evaluator that consumes them rather
than with the store they happen to sit in, because a rule that no process in this role
evaluates is worse than absent — it looks configured.

One consequence for `Init`: it still seeds the four default notification policies, into
MariaDB, on the **data plane**, rather than from `bootstrap/` where the other seeders live.
That keeps the set of processes that create those rows exactly what it was before the move,
and its gate changed from "no `is_default` row" to "the table is empty" — which is what makes
the cutover's ordering rule (root §8) work: after `backfill-config` has run, the table is
non-empty and `Init` seeds nothing.

## Files

| File | Responsibility |
|---|---|
| `alerts.go` | The **cross-store** operations (`ListRules`, `GetRule`, `DeleteRule` — each joins or removes a MariaDB row alongside its ClickHouse state), the ClickHouse `State`/`HistoryEntry` types and their reads/writes, and `Init()` (creates `alert_states` + `alert_history`, and seeds the default notification policies into MariaDB). |
| `evaluator.go` | The evaluation loop. Reads enabled rules from MariaDB, queries ClickHouse per rule, runs the firing/pending/resolved state machine, records history, publishes SSE, calls the router. |
| `router.go` | Given a firing/resolved alert, selects matching **notification policies** (read from MariaDB) and dispatches to their channels via the notifier. A channel id that no longer resolves is logged and skipped. |
| `policies.go` | Only `PolicyMatchers` now — the decoded shape of a policy's `matchers` blob, which `matchPolicy` reads. The CRUD and `ReorderPolicies` are `query/notification_policies.query.go`. |
| `service_groups.go` | Only `ResolveServiceGroups` now — the "which groups contain this service" membership test, which is a Go loop because `services` is a JSON array MariaDB cannot index into. The CRUD is `query/service_groups.query.go`. |
| `notifier.go` | Actually sends a notification for a channel type (webhook / slack / email / pagerduty). 10s timeout. |
| `alert_hub.go` | In-memory pub/sub hub fanning state-change events out to SSE subscribers (`/v1/alerts/stream`). |

## Domain model

- **Rule** — `type` ∈ {`threshold`,`absence`,`rate_change`}, `metric` ∈
  {`count`,`sum`,`avg`,`min`,`max`}, `condition` ∈ {`gt`,`lt`,`gte`,`lte`,`eq`},
  `threshold`, `field` (required for non-count metrics; must be a `data.*` key),
  `query_filters` (JSON array), `evaluation_interval_seconds`, `for_seconds` (must
  stay firing this long before notifying), `cooldown_seconds` (re-notify throttle),
  `priority`, `enabled`.
- **Notification channel** — `type` ∈ {`webhook`,`slack`,`email`,`pagerduty`} + a
  JSON `config`.
- **Notification policy** — routing rule with a `position` (evaluated in order) that
  maps matching alerts (by service group / priority / etc.) to channels.
- **Service group** — named set of services referenced by policies.
- **State** (per rule) — `ok`/`firing`, `value`, `fired_at`, `resolved_at`,
  `last_notified_at`. **ClickHouse**, rewritten every evaluation; read by `GetState`,
  written by `UpsertState`.
- **History entry** — append-only firing/resolved log surfaced at `/v1/alert-history`.
  **ClickHouse**, with a 90-day TTL — which is the single strongest reason it stayed there.

**Where each lives:** Rule, Channel, Policy and Service group are **MariaDB**
(`monitor.alert_rules`, `notification_channels`, `notification_policies`,
`service_groups`; migrations 119-122), as `structs.AlertRule`,
`structs.NotificationChannel`, `structs.NotificationPolicy`, `structs.ServiceGroup`. State
and History are **ClickHouse**. The enum-shaped fields above are now real MariaDB `ENUM`s;
the Go validation maps in `query/alert_rules.query.go` survive as the pre-flight that
returns a 400 naming the field instead of errno 1265.

## Evaluation flow (evaluator.go)

```
main.go starts Evaluator.Run(ctx)  → 15s ticker → evaluateAll
  evaluateAll: listEnabledRules → query.ListEnabledAlertRules(db.SQL)  [MariaDB, no FINAL]
    per rule: respect per-rule evaluation_interval_seconds throttle (lastEvaluated map)
      evaluateRuleState(rule) → (value, isFiring)   [branches on rule.Type — see below]
      state machine:
        ok + firing   → if for_seconds>0 stay ok + record pendingSince; else fire now
        firing+firing → re-notify only if now-last_notified >= cooldown_seconds
        firing + !firing → resolved
        pending>=for_seconds while firing → fire
      onFiring/onResolved: RecordHistory + alertHub.PublishStateChange + router.Route
```

The evaluator runs on a **single goroutine**, rules evaluated **sequentially**.
`router.Route` dispatches each channel notification on its **own goroutine** (with a
`defer recover()`), so a slow/unreachable channel can't stall evaluation.

### Type branching (`evaluateRuleState`, evaluator.go)

`evaluateRuleState(ctx, rule) → (value, isFiring, error)` builds range helpers
(`queryValueForRange`, `queryCountForRange`, both on top of `queryAggForRange`) and
branches on `rule.Type`:

- **`threshold`** (and empty/unknown) — `value` = metric agg over `[now-interval, now]`;
  `isFiring = CheckCondition(value, condition, threshold)`.
- **`absence`** — `value` = `COUNT` over `[now-interval, now]` (ignores metric/field;
  still applies `query_filters`); `isFiring = (value == 0)`.
- **`rate_change`** — `cur` over `[now-interval, now]`, `prev` over
  `[now-2*interval, now-interval]`; `value` = percent change
  (`prev==0 ? (cur>0?100:0) : (cur-prev)/prev*100`, div-by-zero guarded);
  `isFiring = CheckCondition(pct, condition, threshold)`.

`EvaluateRuleNow` (test endpoint) returns `(value, isFiring, error)` from this same
function, so the test result is type-correct.

### UpdateAlertRuleRequest (partial update)

`query.UpdateAlertRule` takes an `UpdateAlertRuleRequest` with **all pointer fields**; a
field is applied only when non-nil. Omitting `enabled` **preserves** it (no longer disables
the rule); numeric fields can be set to 0; enum fields (`type`/`condition`/`metric`/
`priority`) are validated when provided (400 on bad value).

Since the move it is a real partial `UPDATE` of only the named columns, not the old
read-merge-reinsert of the whole row — so two callers editing different fields of one rule
no longer overwrite each other, and `updated_at` is stamped by the column's
`ON UPDATE CURRENT_TIMESTAMP(3)` rather than by Go.
`TestUpdateAlertRuleTouchesOnlyTheNamedColumns` anchors the statement at both ends.

**The policy and service-group updates did NOT get this treatment**, deliberately:
`UpdateNotificationPolicyRequest` and `UpdateServiceGroupRequest` keep the "non-empty wins"
semantics they had, including the wart that omitting `enabled` on a policy **disables** it.
Porting the pointer pattern is a behaviour change and belongs in its own commit, not inside
a store move.

## SQL-injection boundary

`data.*` field names in `field` and in `query_filters` are validated with
`structs.SafeIdentifierRegex` (`^[a-zA-Z_][a-zA-Z0-9_.]*$`) before interpolation into
`JSONExtractString/JSONExtractRaw` (see `numericFieldExpr`, `buildFilterCondition`).
Non-`data.*` filter columns are whitelisted via `structs.FilterColumns`. **Preserve this
validation on any change** — it's the injection guard.

Both come from `structs/columns.go` and are shared with `services/`. This package used
to carry its own copies, and its regex was the only one that allowed dots — which is
why nested keys such as `user.id` worked in an alert rule but failed in an analytics
query. The permissive (dot-allowing) pattern won, since existing rules depend on it;
it is no weaker, as neither variant admits a quote or a backslash. **Do not re-declare
either here** — a local copy is exactly how the divergence happened.

## Known issues & gaps

**Resolved 2026-07-23:** `rule.Type` branching (`evaluateRuleState`), async
notification dispatch (`router.go`), the partial-update-disables-rule bug and the
`!=0` numeric-guard bug (both via `UpdateRuleRequest` pointer fields), and the
`/v1/alerts/stream` SSE 500 (fixed in `middleware/logging.go` — see root §9 changelog).

**Remaining:**

| Sev | Where | Issue |
|---|---|---|
| 🟡 | `evaluator.go` | **TIMER-DRIVEN evaluation is zone-wide (dated 2026-09-06).** `Evaluator.Run` has a background context, no credential and therefore no project, so `queryAggForRange` aggregates across every project and a threshold counts the whole instance's traffic. A rule **can** opt in with a `{"field":"project","value":"…"}` filter — that is why `project` joined `structs.FilterColumns` — but nothing confines a rule that does not. Closing it needs a `project` column on `alert_rules`/`alert_history` and a decision about who owns the existing rules, which belongs to the phase that gives rules an owner. Migrations 119-122 deliberately did **not** add that column: it would partition six live configuration sets between tenants that share them today, which is a behaviour change rather than a move — but the tables are relational now, so it is one migration per table when the phase comes. **The HTTP half is NOT part of this gap and is scoped:** `EvaluateRuleNow` is reached from `POST /v1/alert-rules/{id}/test` with the request's own context, which `QueryAuthMiddleware` has stamped with a project, so `scope.ProjectPredicate` applies there. It had to be — a rule carries a caller-chosen aggregation, field and filter set, so unscoped that endpoint let an admin key bound to one project read a `count()` or a `max()` over every project's events, one number per request. **Consequence, on purpose:** a rule's test value and the value that fires it can disagree once a second project exists. `queryAggForRange`'s exemption in `scope/chokepoint_test.go` was deleted when this landed. Full statement in the `KNOWN GAP` header of `evaluator.go`. |
| 🟡 | `alerts.go` | **`alert_states` and `alert_history` are still created by an ad-hoc `CREATE TABLE` in `Init()`** — the exact pattern migration 119 condemns, and the last two tables in the repo made that way. They stayed in ClickHouse for good reasons (per-evaluation facts; `alert_history` carries a 90-day TTL with no MariaDB equivalent), but that is an argument about the *store*, not about creating schema from application boot. Giving them migration files means `migrations/007…`, a different runner with its own re-runnability story. |
| 🟡 | `query/notification_policies.query.go` | `NotificationPolicy.RepeatIntervalSeconds` is stored + surfaced but **never read** by the router — re-notify cadence is driven only by the rule's `cooldown_seconds`. Dead field, carried across the move rather than dropped so that the move stayed a move. |
| 🟢 | `query/notification_policies.query.go`, `query/service_groups.query.go` | `UpdateNotificationPolicy` and `UpdateServiceGroup` still use "non-empty wins" (unlike `UpdateAlertRule`, now pointer-based), so `repeat_interval` can't be set to 0, a group's member list can't be emptied, and omitting `enabled` on a policy disables it. Port the pointer pattern if this matters. |
| 🟢 | `structs/AlertRule.struct.go` | `Type`, `Priority`, `Metric` and `Condition` are plain strings, not typed enums with `IsValid()`, even though migration 119 makes each a real MariaDB `ENUM`. Held back on purpose: the typed-enum pass touches the evaluator's type switch, `CheckCondition`'s signature, the router, the notifier and their tests. |

**Closed by migrations 119-122 (the ClickHouse → MariaDB move):**

- `position` had **no uniqueness constraint** and `getNextPosition` was a racy `max+1`. There
  is now `UNIQUE (position)`, and the allocation is a locking read inside the insert's own
  transaction.
- `ReorderPolicies` rewrote rows **one at a time with no transaction** (ClickHouse has
  none), so a mid-loop failure left positions half-updated.
  `query.ReorderNotificationPolicies` is one transaction that vacates the positive range in
  a single negating `UPDATE` before assigning `1..N` — see root §6 *Stores* for why the
  obvious loop cannot work under the new key, and
  `query/notification_policies_query_test.go` for the pins.

## Verification

`gofmt -w -s . && go build ./... && go vet ./... && go test ./...` from repo root.
`evaluator_test.go` covers `CheckCondition`, the rate/absence firing math, the
`numericFieldExpr`/`buildFilterCondition` injection guards and the project-scoping of
`buildAggQuery`. The configuration layer is covered from `query/`:
`alert_rules_query_test.go`, `notification_policies_query_test.go` and
`config_tables_query_test.go`.
