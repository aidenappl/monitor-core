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
- `history.go` — `GetOccurrenceHistory` (one issue) and `GetOccurrenceHistoryBulk` (many, in
  ONE grouped query). The list view renders an activity strip per row, so per-issue fetching
  would be a query per row; `GET /v1/issues?history=true` opts into the bulk read, and callers
  that only want counts do not pay for it.
- `issues_test.go` — unit tests for `normalizeMessage` + `generateFingerprint`.
- `grouping_test.go` — regression tests for the three grouping bugs fixed
  2026-08-07 (fingerprint-derived issue ids, status-code preservation in
  `normalizeMessage`, `FingerprintForEvent` separating tenants that share an event
  name), plus the project dimension: two projects with identical service, name and
  message must derive **different issue ids**, an unstamped project resolves to the
  default, and `issueNamespace` is pinned to its literal.

## How it works

- **Ingestion hook:** the ingestion path calls `issues.TrackError(&event)` for each
  `error`/`fatal` event (`routes/events.go`). `TrackError` is a **non-blocking
  enqueue** onto a buffered channel (cap `trackQueueSize` = 1000) drained by a fixed
  pool of `trackWorkers` = 4 goroutines started in `Init(ctx)`; workers exit on ctx
  cancel and each wraps `processError` in a `recover()`. If the queue is full the
  event is dropped with a log line — bounded under an error storm (was previously one
  detached goroutine per error on `context.Background()`). The actual create/update
  logic lives in `processError(ctx, event)`.
- **Fingerprint** = `sha256(project | service | name | path | normalizeMessage(message))`.
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
- **`project` is the first fingerprint component, and it is in the STRING — not
  just in a database key.** This is the whole of the project-scoping fix and the
  reason the obvious version of it does not work. `monitor.issues` has *both*
  `id CHAR(36) PRIMARY KEY` and `UNIQUE KEY uq_issues_fingerprint (fingerprint)`,
  and `id` is a pure function of the fingerprint — so two projects with matching
  `service`/`name`/`path`/message would compute one fingerprint, therefore one id,
  therefore a **primary-key** collision. `INSERT … ON DUPLICATE KEY UPDATE` fires on
  the *first* unique index violated, which is the PK, so project B's occurrence would
  fold into project A's row — counter incremented, message overwritten, a resolved
  issue reopened — before `UNIQUE (project, fingerprint)` was ever consulted. That
  composite key (migration 118) documents the intent; **this line is the mechanism.**
  An empty project is fingerprinted as `MON_DEFAULT_PROJECT`, not as a tenant named
  `""` — the same rule `scope.ProjectPredicate` applies to the pre-006 rows that are
  the only source of one. See `fingerprintProject`.
- **`env` is deliberately NOT in the fingerprint.** Production and staging errors of
  the same shape merge into one issue, which is what Sentry does on purpose — an error
  is one bug regardless of where it fired, and the per-event `env` column still splits
  the occurrences on demand. Adding it would be a *second* full re-key with no default
  value to anchor it. Recorded so it is not rediscovered as a bug.
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

## API surface (wired in main.go → routes/issues.go + routes/issue_timeline.go)

| Method | Path | Notes |
|---|---|---|
| GET | `/v1/issues?status=&service=&assignee=&has_pr=&q=&from=&to=&sort=&order=&history=&limit=&offset=` | limit default 50, max 500; returns the total count |
| GET | `/v1/issues/{id}` | verbose detail — links, assignee, repository, comment count, sparkline |
| PUT | `/v1/issues/{id}` | status / priority / title / assignee |
| GET | `/v1/issues/{id}/events?limit=` | the issue's own events (indexed on `issue_id`, legacy scan as fallback) |
| GET | `/v1/issues/{id}/timeline` | the activity feed, oldest first |
| GET | `/v1/issues/{id}/history` | per-day occurrence sparkline |
| POST/PATCH/DELETE | `/v1/issues/{id}/comments[/{commentID}]` | add / edit / soft-delete a note |
| GET/POST/DELETE | `/v1/issues/{id}/links[/{linkID}]` | linked PRs, issues and commits |

This is the primary error-investigation surface (mirrored by
`mcp__monitor__monitor_list_issues` / `monitor_get_issue`).

**Every one of them is project-scoped, and none of them scope themselves.** The list resolves
the caller's project through `routes.requireProject` and passes it into `query.ListIssues`;
the other eleven resolve `{id}` through `routes.requireIssue`, which reads the issue **in the
caller's project** and 404s otherwise. A new `/v1/issues/{id}/…` route must go through
`requireIssue` too — fetching the issue any other way reintroduces the leak for that route
alone, and no test in this package would notice.

## Known issues & gaps (2026-07-23)

| Sev | Where | Issue |
|---|---|---|
| 🟢 | `history.go` | **`monitor.issue_occurrences_daily` has no `project` column** (005 keys it on `(issue_id, day)`), so it cannot be aggregated per project without joining `monitor.issues`. This is now a schema nicety rather than a boundary: since issue ids are derived from a fingerprint that includes the project, **one rollup key can no longer accumulate two tenants' occurrences**. Adding the column would need a table rebuild plus a `DROP`/`CREATE` of the materialized view, which the boot runner (idempotent DDL, replayed every boot) cannot perform. A predicate on these reads would still be wrong — every caller already holds an issue read from `monitor.issues`, so it would narrow a histogram under an issue the caller can read in full. **That argument depends entirely on the issue read path being scoped**, which it now is (`query.scopeIssues`) and was not when this row was first written: an unscoped list handed out any tenant's issue id, and these two functions turned it into that tenant's per-day event counts. Widen the issue read path and these widen with it. Full statement in the `KNOWN GAP` header of `history.go`. **Event reads under an issue *are* scoped** — see `routes.selectIssueEvents`. |
| 🟢 | subsystem-wide | **No auto-resolve.** Regression reopen exists but nothing ages an issue out after N days of silence. |
| 🟢 | `issues.UpdateStatus` | **Records no actor** — a legacy shim kept only for callers not yet moved. `routes/issues.go` no longer uses it; `HandleUpdateIssue` calls `query.UpdateIssue` with `middleware.GetActor`. Delete the shim once nothing calls it. |

**Project-scoped as of `118_issues_project.sql`.** An issue belongs to exactly one project.
`monitor.issues` gained a `NOT NULL` `project` column and a `UNIQUE (project, fingerprint)`,
and — the part that actually does the work — `project` became the first component of the
fingerprint string, so two tenants reporting the same failure derive different primary keys
instead of sharing one row.

**Writing the column is only half of it — the READ path is scoped too, by
`query.scopeIssues`.** `ListIssues`, `CountIssues`, `GetIssue` and `UpdateIssue`'s own `WHERE`
all carry the caller's project, and an empty project returns `query.ErrNoIssueProject` rather
than a query without the predicate. This is a separate mechanism from `scope.ProjectPredicate`
on purpose: `monitor.issues` is MariaDB, reached through `db.SQL`, so no ClickHouse chokepoint
covers it and `scope/chokepoint_test.go` does not look at it either. Two things follow.

- **An issue row carries event data**, so this is a leak surface and not a metadata one:
  `message` is the error text lifted off the event by `extractMessage`, alongside the service,
  the path, the occurrence count and the seen-window. A per-project column that only the WRITE
  path honours means the listing still answers "what is failing, where and how often" for every
  tenant at once.
- **Scoping the id-addressed reads is not redundant** with the id being a UUIDv5 over a
  project-bearing fingerprint. That makes a foreign id impossible to *derive*, not impossible
  to *hold* — ids travel into monitor-web URLs, alert payloads, comments and GitHub links. A
  foreign id returns 404, the same as one that never existed.
- **`issues.List`, `issues.Get` and `issues.UpdateStatus` read the project off the `ctx`** they
  already take, rather than adding a parameter, because their callers are handlers holding a
  request whose context `QueryAuthMiddleware` has already stamped. All three refuse when it
  carries none. Two of the three have no callers today and are scoped anyway — an unscoped
  exported reader is a leak waiting for its first caller.

**There was deliberately no re-keying migration.** Every pre-118 issue keeps its old id and
project-less fingerprint, which means they **stop receiving occurrences**: the next occurrence
of the same failure mints a fresh row counting from 1, so a failure that spans the deploy shows
as two rows, one frozen and one live, until the frozen one is resolved by hand. Do not write a
re-key subcommand, a remap table, or a legacy-fingerprint fallback — the owner deals with them
manually. There were 43 rows on 2026-09-06 (12 unresolved, 3 ignored, 28 resolved); re-count
rather than trusting that figure. The **ignored** ones are the quiet case: their replacement rows
are new ids that default to `unresolved`, so a silenced error comes back looking like a fresh
regression — re-ignore it rather than re-investigating. Their rows in the **no-TTL**
`monitor.issue_occurrences_daily` become permanent
orphans once the issues are deleted; the sweep is
`migrations/manual/delete_orphaned_issue_rollups.sql`, run by hand, keyed on absence from
`monitor.issues` so it is a no-op while those issues still exist.

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
