# AGENTS.md — issues/ (monitor-core)

Sentry-style **issue tracking**: groups `error`/`fatal` events into deduplicated
"issues" by a normalized fingerprint, tracks occurrence counts + first/last seen, and
exposes resolve/ignore. Read the root `../AGENTS.md` first.

## File

- `issues.go` — the subsystem (`Init` + worker pool, `TrackError` / `processError`,
  `recordRegression`, `List` / `Get` / `UpdateStatus` shims over `query`, fingerprinting
  helpers). Persistence lives in `query/issues.query.go`; `Issue` is an alias for
  `structs.Issue`.
- `backfill.go` — one-time `BackfillFromClickHouse`, run via the `backfill-issues`
  subcommand.
- `history.go` — `GetOccurrenceHistory`, reading the no-TTL daily rollup.
- `issues_test.go` — unit tests for `normalizeMessage` + `generateFingerprint`.
- `grouping_test.go` — regression tests for the three grouping bugs fixed
  2026-08-07: fingerprint-derived issue ids, status-code preservation in
  `normalizeMessage`, and `FingerprintForEvent` separating tenants that share an
  event name.

## How it works

- **Ingestion hook:** the ingestion path calls `issues.TrackError(&event)` for each
  `error`/`fatal` event (`routes/events.go`). `TrackError` is a **non-blocking
  enqueue** onto a buffered channel (cap `trackQueueSize` = 1000) drained by a fixed
  pool of `trackWorkers` = 4 goroutines started in `Init(ctx)`; workers exit on ctx
  cancel and each wraps `processError` in a `recover()`. If the queue is full the
  event is dropped with a log line — bounded under an error storm (was previously one
  detached goroutine per error on `context.Background()`). The actual create/update
  logic lives in `processError(ctx, event)`.
- **Fingerprint** = `sha256(service | name | path | normalizeMessage(message))`.
  `normalizeMessage` strips UUIDs → `<UUID>`, hex → `<HEX>`, URLs → `<URL>`, numbers →
  `<N>` so structurally-identical errors collapse into one issue. **Exception: a
  3-digit number introduced as an HTTP status (`status 502`, `code 404`,
  `status_code=503`, `HTTP 418`) is preserved.** A status code is the failure
  *class*, not an incidental identifier — collapsing it merged a Bad Gateway and a
  rate-limit into a single issue whose displayed message flip-flopped to whichever
  occurrence arrived last. Offsets, retry counts and durations still collapse. See
  `replaceNumbersPreservingStatusCodes` (RE2 has no lookbehind, so matches are
  located by index and the preceding text inspected directly).
- **Issue id is derived from the fingerprint**, not random —
  `issueIDFor(fingerprint)` returns a UUIDv5 under a fixed namespace. This is what
  makes identity correct: the table is `ORDER BY (id)`, so ClickHouse only collapses
  rows agreeing on `id` and **cannot** enforce uniqueness on `fingerprint`. Minting
  `uuid.New()` per insert let two workers that both saw "no issue yet" create two
  permanently distinct issues (production carried four rows for one fingerprint).
  Deriving the id makes racing creators converge without coordinating — including
  across processes, which a mutex cannot do. **`issueNamespace` must never change**;
  changing it re-keys every issue and orphans all history.
- **`FingerprintForEvent(event)`** is the exported way to ask which issue an event
  belongs to. Anything matching events to issues must use it — `service`+`name`
  alone is only a pre-filter, since many issues share one event name.
- **`issue_id` is stamped on the event row at ingest.** `IssueIDForEvent` runs
  *synchronously* in `routes/events.go` before the event is enqueued (fingerprinting is
  a pure sha256 with no I/O — the worker pool exists for `processError`'s database
  round-trip, not for this). It is always assigned and never merged, so an `issue_id`
  supplied by a client is overwritten and no caller can file events under another
  issue. Non-error levels get `""`.
- **`GET /issues/{id}/events` is an indexed equality match**, not a scan. The old path
  pre-filtered on `service`+`name`(+`path`) then recomputed each candidate's fingerprint
  in Go, bounded by `candidateScanLimit` — which truncated and logged when a sparse issue
  was buried among high-volume siblings. That legacy scan survives only as a fallback,
  restricted to `issue_id = ''` so it can return only rows the fast path could not, and
  **is removable 30 days after the deploy of `004`** once the events TTL has aged out
  every unstamped row.
- **Message/path extraction** pulls `path`/`uri`, `error`/`error_message`/`message`,
  and `method` out of the event's `data` map to build a descriptive title.
- **Storage: MariaDB `monitor.issues`** (moved off ClickHouse). The table is created by
  `db.RunMigrations` (`db/migrations/111_create_issues.sql`), not by `Init`. ClickHouse
  keeps only `events`. The legacy ClickHouse `issues` table is left in place, unread —
  drop it once a release has passed.
- **`processError` is a single atomic statement.** `query.UpsertIssueOccurrence` is one
  `INSERT … ON DUPLICATE KEY UPDATE`, so `occurrence_count = occurrence_count + 1`
  happens under a row lock. **This is the fix for the drift bug**, and it is why the
  64-way `fingerprintLocks` shard is gone: that only ever serialized one process, and
  the read-then-write it guarded no longer exists. **Do not reintroduce a lock here** —
  if one seems necessary, the upsert has probably been split back into a read and a
  write. `query/issues_query_test.go` fails the build if it is.
- **SET-clause ordering in the upsert is load-bearing.** MariaDB evaluates left to
  right, so `regression_count`, `regressed_at` and `resolved_at` all read the *previous*
  `status` and must precede `status`'s own reassignment. Moving `status` earlier
  compiles, passes casual review, and silently stops counting regressions.
- **Status** ∈ {`unresolved`,`in_progress`,`resolved`,`ignored`} — one axis, not two.
  `unresolved` is both the default and the backlog. `resolved` stamps `resolved_at`.
- **Regression** is a status flip on the same issue, never a new issue. On recurrence
  the upsert transitions **only** out of `resolved` → `unresolved`, stamping
  `regressed_at` and incrementing `regression_count`; `in_progress`, `unresolved` and
  `ignored` are left alone, so an agent mid-work is never clobbered. `recordRegression`
  then appends a `regressed` timeline entry keyed on the stored `regressed_at`, so
  racing workers compute the same `dedupe_key` and collapse to one row.

## API surface (wired in main.go → routes/issues.go)

| Method | Path | Function |
|---|---|---|
| GET | `/v1/issues?status=&service=&limit=&offset=` | `List` (limit default 50, max 500; returns total count) |
| GET | `/v1/issues/{id}` | `Get` |
| PUT | `/v1/issues/{id}` (body `{status}`) | `UpdateStatus` |
| GET | `/v1/issues/{id}/events?limit=` | events matching the issue's fingerprint |

This is the primary error-investigation surface (mirrored by
`mcp__monitor__monitor_list_issues` / `monitor_get_issue`).

## Known issues & gaps (2026-07-23)

| Sev | Where | Issue |
|---|---|---|
| 🟢 | subsystem-wide | **No auto-resolve.** Regression reopen exists but nothing ages an issue out after N days of silence. |
| 🟢 | `issues.UpdateStatus` | **Records no actor** — a legacy shim kept only for callers not yet moved. `routes/issues.go` no longer uses it; `HandleUpdateIssue` calls `query.UpdateIssue` with `middleware.GetActor`. Delete the shim once nothing calls it. |

**Resolved 2026-08-25:** the full filter/sort surface and every mutation endpoint are wired.
`GET /v1/issues` takes status, service, assignee (`none` for unassigned), `has_pr`, `q`, `from`,
`to`, `sort` and `order`; the detail read returns links, assignee, repository, comment count and
the sparkline. **Every mutation records its actor** — a status change from monitor-mcp is
attributed to `monitor-mcp` rather than left anonymous.

Two things worth knowing about the write path. `HandleUpdateIssue` reads the body **twice** — once
typed, once as a raw map — because a single pointer cannot distinguish an explicit
`{"priority": null}` (clear it) from an omitted key (leave it). And `appendUpdateTimeline`
compares before/after rather than trusting the request, so setting status to what it already was
leaves no entry: the timeline records changes, not attempts.

**Resolved 2026-08-19:** occurrence-count drift is **fixed**, not mitigated. The issue
row moved to MariaDB and the fold became a single atomic statement, so concurrent
workers — across processes and replicas — can no longer lose increments. The
`lockFingerprint` mutex shard was deleted as redundant.

**Resolved 2026-07-23:** the unbounded detached-goroutine `TrackError` was replaced by
a bounded channel + fixed worker pool tied to the shutdown context (see above).

## Cutting over from the ClickHouse issues table

`monitor-core backfill-issues` copies the legacy ClickHouse rows into MariaDB and exits
without serving. Safe to re-run: rows match on the primary key, which is the
deterministic UUIDv5 from the fingerprint, so both stores agree on identity and no id
remapping is needed. Conflicts resolve additively — the greater `occurrence_count` wins
and the seen-window widens — so a backfill running alongside live ingestion cannot walk
a live counter backwards. Triage state already set in MariaDB is never reverted.

**Resolved 2026-08-07:**
- **Duplicate issues per fingerprint** — ids are now derived from the fingerprint
  (`issueIDFor`), so racing creators converge on one row. Pre-existing duplicates are
  reconciled by `migrations/manual/dedupe_issues_by_fingerprint.sql` — **manual and
  one-off, deliberately outside the auto-applied `migrations/*.sql` set.** The
  ClickHouse runner has no applied-tracking table and re-runs every embedded file on
  every boot, so a non-idempotent reconciliation placed there would compound on each
  restart (and its in-comment semicolons would crash startup, since the runner splits
  on `;` and errors are fatal). Read its header before running.
- **`getByFingerprint` treated a query error as "issue does not exist"**, so a
  transient ClickHouse blip minted a duplicate. It now distinguishes no-rows
  (`sql.ErrNoRows` → `nil, nil`) from a real failure, and `processError` returns
  without creating on the latter.
- **`GET /issues/{id}/events` returned other issues' events** — it filtered on
  `service`+`name` and discarded the fingerprint entirely, so one tenant's failures
  returned every tenant's. It now recomputes `FingerprintForEvent` per candidate and
  keeps only true matches; `service`+`name`(+`path`) remains a pre-filter that bounds
  the scan. When the candidate window is exhausted before the page fills, it logs
  rather than silently returning a short page.

## Verification

`gofmt -w -s . && go build ./... && go vet ./... && go test ./...` from repo root.
