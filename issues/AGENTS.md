# AGENTS.md — issues/ (monitor-core)

Sentry-style **issue tracking**: groups `error`/`fatal` events into deduplicated
"issues" by a normalized fingerprint, tracks occurrence counts + first/last seen, and
exposes resolve/ignore. Read the root `../AGENTS.md` first.

## File

- `issues.go` — the whole subsystem (struct, `Init` + worker pool, `TrackError` /
  `processError`, `List`, `Get`, `UpdateStatus`, fingerprinting helpers).
- `issues_test.go` — unit tests for `normalizeMessage` + `generateFingerprint`.

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
  `<N>` so structurally-identical errors collapse into one issue.
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
| 🟢 | `issues.go` (`processError`) | **Occurrence-count race** — read-then-write into a ReplacingMergeTree; concurrent increments for the same fingerprint can lose updates or go backward. Documented in-code as accepted (approximate counts are fine for error tracking). Left as-is intentionally. Note the worker pool uses 4 workers, so concurrent processing of the same fingerprint is still possible. |

**Resolved 2026-07-23:** the unbounded detached-goroutine `TrackError` was replaced by
a bounded channel + fixed worker pool tied to the shutdown context (see above).

## Verification

`gofmt -w -s . && go build ./... && go vet ./... && go test ./...` from repo root.
