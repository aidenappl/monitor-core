# AGENTS.md — alerts/ (monitor-core)

The alerting subsystem: rules, the evaluation loop, notification channels, routing
policies, service groups, alert history, and the alert SSE hub. Read the root
`../AGENTS.md` first for repo-wide conventions.

## Files

| File | Responsibility |
|---|---|
| `alerts.go` | CRUD + validation for **rules** and **notification channels**; struct definitions; `Init()` (creates all alert tables). |
| `evaluator.go` | The evaluation loop. Queries ClickHouse per rule, runs the firing/pending/resolved state machine, records history, publishes SSE, calls the router. |
| `router.go` | Given a firing/resolved alert, selects matching **notification policies** and dispatches to their channels via the notifier. |
| `policies.go` | CRUD + `ReorderPolicies` for **notification policies** (routing rules with `position` ordering). |
| `service_groups.go` | CRUD for **service groups** (named sets of services used in policy matching). |
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
  `last_notified_at`. Persisted; read by `GetState`, written by `UpsertState`.
- **History entry** — append-only firing/resolved log surfaced at `/v1/alert-history`.

## Evaluation flow (evaluator.go)

```
main.go starts Evaluator.Run(ctx)  → 15s ticker → evaluateAll
  evaluateAll: listEnabledRules (SELECT … FINAL WHERE enabled=1)
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

### UpdateRuleRequest (partial update)

`UpdateRule` takes an `UpdateRuleRequest` with **all pointer fields**; a field is
applied only when non-nil. Omitting `enabled` **preserves** it (no longer disables the
rule); numeric fields can be set to 0; enum fields (`type`/`condition`/`metric`/
`priority`) are validated when provided (400 on bad value). `CreateRule` is unchanged.

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
| 🟡 | `evaluator.go` | **TIMER-DRIVEN evaluation is zone-wide (dated 2026-09-06).** `Evaluator.Run` has a background context, no credential and therefore no project, so `queryAggForRange` aggregates across every project and a threshold counts the whole instance's traffic. A rule **can** opt in with a `{"field":"project","value":"…"}` filter — that is why `project` joined `structs.FilterColumns` — but nothing confines a rule that does not. Closing it needs a `project` column on `alert_rules`/`alert_history` and a decision about who owns the existing rules, which belongs to the phase that gives rules an owner. **The HTTP half is NOT part of this gap and is scoped:** `EvaluateRuleNow` is reached from `POST /v1/alert-rules/{id}/test` with the request's own context, which `QueryAuthMiddleware` has stamped with a project, so `scope.ProjectPredicate` applies there. It had to be — a rule carries a caller-chosen aggregation, field and filter set, so unscoped that endpoint let an admin key bound to one project read a `count()` or a `max()` over every project's events, one number per request. **Consequence, on purpose:** a rule's test value and the value that fires it can disagree once a second project exists. `queryAggForRange`'s exemption in `scope/chokepoint_test.go` was deleted when this landed. Full statement in the `KNOWN GAP` header of `evaluator.go`. |
| 🟡 | `policies.go` | `NotificationPolicy.RepeatIntervalSeconds` is stored + surfaced but **never read** by the router — re-notify cadence is driven only by the rule's `cooldown_seconds`. Dead field. |
| 🟡 | `policies.go:297,331` | `position` has no uniqueness constraint (`getNextPosition` = max+1) and `ReorderPolicies` rewrites rows one-by-one with no transaction (ClickHouse has none) — a mid-loop failure leaves positions half-updated. |
| 🟢 | `policies.go` | `UpdatePolicy` still guards numeric fields with `!= 0` (unlike `UpdateRule`, now pointer-based) — `repeat_interval` can't be set to 0. Port the pointer pattern if this matters. |

## Verification

`gofmt -w -s . && go build ./... && go vet ./... && go test ./...` from repo root.
`evaluator_test.go` covers `CheckCondition`, the rate/absence firing math, and the
`numericFieldExpr`/`buildFilterCondition` injection guards.
