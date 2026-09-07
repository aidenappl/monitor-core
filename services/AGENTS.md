# AGENTS.md — services/ (monitor-core)

The ingestion pipeline (queue → batcher → ClickHouse), the SSE hub, and the
query/analytics SQL engines. Read the root `../AGENTS.md` first.

## Files

| File | Responsibility |
|---|---|
| `queue.go` | Bounded, non-blocking event queue (buffered channel). Drops on overflow, tracks `enqueued`/`dropped`/`pending`; `RecordDropped(n)` lets losses further down the pipeline land on the same `dropped` counter. |
| `batcher.go` | Consumes the queue, accumulates into batches, flushes to a `Writer` on `BATCH_SIZE` or `FLUSH_INTERVAL`, with retry. Stamps `lastFlush` on success (`LastFlushAt()`). |
| `hub.go` | In-memory SSE pub/sub — fans ingested events out to `/v1/events/stream` subscribers (capped by `MAX_SSE_SUBSCRIBERS`), applying each subscriber's filter map. |
| `query.go` | Event search + label/data-value autocomplete SQL (squirrel → ClickHouse). |
| `analytics.go` | Aggregation / time series / top-N / gauge / compare SQL engine. |

## This package is the data plane

`MON_ROLE` (root `../AGENTS.md` §6 *Roles*) splits the binary into a control plane (`app`)
and a data plane (`zone`), and **everything in this package is data plane** — it is very
nearly the definition of one. Under `MON_ROLE=app` `main.go` constructs no `Hub`, no `Queue`
and no `Batcher`, starts neither the batcher goroutine nor the alert evaluator, and never
opens the ClickHouse connection `query.go` and `analytics.go` issue their SQL over. Under
the default `MON_ROLE=both`, which is the deployed configuration, all of it runs exactly as
before; nothing in this package changed shape.

The consequence that reaches into other packages: **`routes.Queue`, `routes.Batcher` and
`routes.EventHub` are nil in an app process.** `buildRouter` registers no route that reads
them, so the nil is unreachable over HTTP — with one exception, and it is the one that
matters operationally. `/health` is registered in **every** role (a liveness probe that
disappears when a process is configured differently is not a probe), and it reads
`Queue.Stats()`. `HealthHandler` therefore nil-checks and reports zeroes, which is the
honest answer for a process that runs no queue: nothing has been enqueued, dropped, or is
pending. Without that check the nil dereference would panic on every request to the endpoint
the container `HEALTHCHECK` polls, and Docker would restart a control plane that was working
perfectly. **Anything else added here that `main.go` wires into a `routes` global inherits
the same rule** — either gate its route to the data plane, or make the reader tolerate nil;
a package-level `var` that only main sets is not a guarantee that main set it.

`/ready` makes the matching judgement in the other direction: `pingClickHouse` returns false
on a nil `Conn`, so an app process would be permanently 503 and un-routable. `ReadyHandler`
requires ClickHouse for every role *except* exactly `app`, deliberately spelled as
`env.MonRole != env.RoleApp` rather than `!RunsDataPlane()` so that an unset or
not-yet-invented role keeps the event store as a hard readiness dependency. Being wrongly
un-ready costs a routing decision; being wrongly ready hands live traffic to a replica that
drops everything it accepts.

## Ingestion pipeline

```
IngestEventsHandler → Queue.Enqueue(event)          (non-blocking; false = dropped)
Batcher.Run(ctx) [goroutine, started in main.go]:
  ├─ event from queue → append to batch → flush when len >= BatchSize
  ├─ FlushInterval ticker → flush partial batch
  └─ ctx.Done / channel closed → final flush, return
Batcher.flush: writer.WriteBatch with up to maxFlushRetries(5) × linear backoff
               (flushRetryBaseWait×attempt); drops the batch after 5 failures or on ctx cancel.
               A success stamps `lastFlush` (read back via `Batcher.LastFlushAt()`, surfaced
               as `/health`'s `last_flush_at`); BOTH drop paths call
               `Queue.RecordDropped(len(batch))` before truncating.
```

- **Queue overflow no longer over-counts** — the ingestion handler now counts only
  events `Enqueue` returned `true` for, so `accepted` is accurate; `/health` still
  surfaces the real `dropped` counter via `Queue.Stats()`. The handler also parses the
  whole body before enqueuing anything, so a malformed line is all-or-nothing (400,
  retry-safe).
- **`dropped` counts every way an event dies, not just overflow.** The Batcher abandons a
  batch in two places — retries exhausted, and shutdown mid-retry — and both used to
  return silently, so a zone with a dead ClickHouse reported `{"status":"ok","dropped":0}`
  while destroying everything. `Queue.RecordDropped(n)` feeds the *same* atomic counter
  `Enqueue`'s overflow path uses, chosen over a second counter on the Batcher because
  `/health` already reads `Queue.Stats()` and the Batcher already holds a `*Queue`: no new
  wiring, one number. Anything else that loses accepted events should call it too.
- **`Writer` is an interface** (`batcher.go:12`) — the real impl is `db.Writer`
  (ClickHouse batch insert), passed in `main.go`. It is the mocking seam, and
  `batcher_test.go` uses it: an always-failing writer plus an already-cancelled context
  reaches the abandonment path immediately. The **other** abandonment path (5 exhausted
  retries) is the same two lines but costs 30s of real backoff to reach — `maxFlushRetries`
  and `flushRetryBaseWait` are package consts and the wait is a raw `time.After` — so it is
  untested. Making the backoff injectable is the fix if that ever matters.

## SSE hub

`hub.go` maintains a set of subscriber channels; `Publish` non-blockingly sends each
event to every subscriber. `routes/stream.go` registers a subscriber and streams
`data: <json>\n\n` frames with a 15s `: keepalive`.

> `/v1/events/stream` works: `middleware/logging.go`'s `loggingResponseWriter` now
> forwards `Flush`/`Hijack`/`Unwrap`, and the handler clears its write deadline so the
> 30s `WriteTimeout` doesn't cut the stream (root §9 changelog).

**`matchesFilters` fails closed, and the filter contract spans two files.** A subscriber's
filters are ANDed, an empty/nil map matches everything, and the switch understands exactly
`service`, `env`, `level`, `name` and `project` — anything else returns `false`, so that
subscriber receives nothing. Before the `default:` arm the loop skipped the unrecognised key
and fell through to `return true`: a subscriber who filtered on a key nobody had implemented
got **every** event instead of none, with no error and no log line, which is the sort of bug
a happy-path test never sees. `routes/stream.go` builds the map from `clientStreamFilters`,
its own allowlist of the first four, so nothing unrecognised can reach here over HTTP — the
two lists are in sync, in different files, with nothing enforcing it. **Adding a filter key
means editing both plus `hub_test.go`**, which asserts the full contract (known keys matching
and mismatching, ANDing, and the unknown-key cases) precisely so a half-done addition fails
in CI rather than silently streaming nothing.

**`project` is the one filter a client cannot set.** It is the tenancy boundary for the live
tail, not a convenience: `routes.subscriptionFilters` takes it from the authenticated
credential, applies it *after* the client's filters (so it wins even if `project` ever
reached `clientStreamFilters` by mistake), and refuses the request outright when no project
resolves. Membership is delegated to `scope.Matches` rather than compared here, so the stream
and the stored query cannot disagree — including during the empty-string transition window,
where the default project also matches unstamped pre-006 events. That arm is removed at the
same moment as its twin in `scope.ProjectPredicate`; both are dated in `scope/scope.go`.
A missing `case` here would not merely widen one subscriber's filter — it would stream every
project's events in real time, leaving no query, no audit row and no error behind it.

## Query / analytics engines

- **Every read is project-scoped, at one chokepoint per file.** `query.go` starts every
  builder at `selectEvents(ctx, cols…)`; `analytics.go` builds its entire `WHERE` through
  `buildEventWhere(ctx, from, to, filters)`. Both attach `scope.ProjectPredicate(ctx)` and
  both **return an error** when the context carries no project, so an unscoped read is not a
  mistake that can be made — there is no other way to name the table (`query.go`) or to
  produce a `WHERE` clause (`analytics.go`). `QueryCompare` deliberately has no chokepoint
  call of its own: it issues no SQL, running both periods through `QueryGauge`.
  **Do not add a `.Where(project…)` at a call site** — that is the pattern this replaced,
  and the one whose omissions are invisible.
- **Argument order is clause order, not call order.** squirrel emits args as
  columns → from → where, so the project argument now sits between `GetDataValues`'s two
  bindings of the data key. That function therefore binds through `Column(expr, key)` and
  `Where(expr, key)` rather than hand-prepending to the arg slice, which would slide every
  binding one position along: valid SQL, no error, wrong rows. `scope_test.go` pins the exact
  arg vector.
- `scope_test.go` drives all nine read entry points against a recording `driver.Conn` and
  asserts on the **generated SQL text** — the only way to prove a chokepoint fires on every
  path, since a builder that stopped applying it still runs and still returns rows. It also
  checks *every* statement a call issues, because `QueryEvents` emits a count plus a page and
  `QueryCompare` emits two gauges, and a predicate on one of a pair is a real bug.
- Both build SQL with squirrel and run it via the ClickHouse driver.
- **Both `analytics.go` and `query.go` validate `data.*` field names** with
  `structs.SafeIdentifierRegex` and reject unknown columns against
  `structs.QueryableColumns` / `structs.GroupByColumns` (`GetLabelValues` uses
  `structs.LabelColumns`). All four live in `structs/columns.go` — the single source of
  truth, shared with `alerts/`, and all four now include `project`: naming it is allowed
  precisely because the mandatory predicate is ANDed on top, so a caller asking for another
  project's rows gets an empty result rather than that project's data. They used to be local copies here, and this package's
  regex was the strict no-dot variant, so a nested key like `user.id` that an alert rule
  accepted was rejected by a query. Extend the shared sets; don't re-declare them.
  In `query.go`, `applyDataFilter`
  and `applyColumnFilter` (and `applyFilters`) return an `error`; a bad `data.*` key or
  unknown column is rejected (not silently dropped) and propagated through
  `QueryEvents`/`GetLabelValues`/`GetDataKeys`/`GetDataValues` → the handler maps it to
  400 via `isFilterValidationError` (`routes/query.go`). Keep this guard on any change.
- Response shapes (consumed by `monitor-web`): analytics `{data:[{value,groups}],total}`;
  timeseries `{series:[{name,groups,data_points:[{timestamp,value}]}]}`; topn
  `{data:[{key,value}]}`; gauge `{value}`; compare `{current,previous,change,change_percent}`.

## Known issues & gaps

**Resolved 2026-07-23:** the `applyDataFilter` SQL injection and the silently-dropped
unknown-column filters — both now validated + error-threaded (see above).
`query_test.go` covers the injection rejection and unknown-column error.

## Verification

`gofmt -w -s . && go build ./... && go vet ./... && go test ./...` from repo root.
