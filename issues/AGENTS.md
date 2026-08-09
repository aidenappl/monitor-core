# AGENTS.md — issues/ (monitor-core)

Sentry-style **issue tracking**: groups `error`/`fatal` events into deduplicated
"issues" by a normalized fingerprint, tracks occurrence counts + first/last seen, and
exposes resolve/ignore. Read the root `../AGENTS.md` first.

## File

- `issues.go` — the whole subsystem (struct, `Init` + worker pool, `TrackError` /
  `processError`, `List`, `Get`, `UpdateStatus`, fingerprinting helpers).
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
- **Message/path extraction** pulls `path`/`uri`, `error`/`error_message`/`message`,
  and `method` out of the event's `data` map to build a descriptive title.
- **Storage:** ClickHouse `monitor.issues`, `ReplacingMergeTree(updated_at) ORDER BY
  (id)`. Reads use `FINAL` to collapse duplicate versions. `TrackError` does a
  read-then-write (get by fingerprint → insert new version with count+1). Resolved
  issues that recur are flipped back to `unresolved` (regression detection).
- **Status** ∈ {`unresolved`,`resolved`,`ignored`}. `resolved` stamps `resolved_at`.

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
| 🟢 | `issues.go` (`processError`) | **Occurrence-count drift** — read-then-write into a ReplacingMergeTree. `lockFingerprint` now serializes this per fingerprint *within a process*, so single-instance drift is largely gone, but concurrent increments across replicas can still lose updates. Approximate counts remain acceptable for error tracking. |
| 🟢 | `GET /issues` | **No time filter or sort.** Hardcoded `ORDER BY last_seen DESC`, no `from`/`to`/`sort` params — server-side, not just in the MCP. Callers wanting "issues created in the last 24h" must filter on `first_seen` client-side. |
| 🟢 | subsystem-wide | **No auto-resolve.** Regression reopen exists (`resolved` → `unresolved` on recurrence) but nothing ages an issue out after N days of silence. |

**Resolved 2026-07-23:** the unbounded detached-goroutine `TrackError` was replaced by
a bounded channel + fixed worker pool tied to the shutdown context (see above).

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
