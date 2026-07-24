# AGENTS.md — services/ (monitor-core)

The ingestion pipeline (queue → batcher → ClickHouse), the SSE hub, and the
query/analytics SQL engines. Read the root `../AGENTS.md` first.

## Files

| File | Responsibility |
|---|---|
| `queue.go` | Bounded, non-blocking event queue (buffered channel). Drops on overflow, tracks `enqueued`/`dropped`/`pending`. |
| `batcher.go` | Consumes the queue, accumulates into batches, flushes to a `Writer` on `BATCH_SIZE` or `FLUSH_INTERVAL`, with retry. |
| `hub.go` | In-memory SSE pub/sub — fans ingested events out to `/v1/events/stream` subscribers (capped by `MAX_SSE_SUBSCRIBERS`). |
| `query.go` | Event search + label/data-value autocomplete SQL (squirrel → ClickHouse). |
| `analytics.go` | Aggregation / time series / top-N / gauge / compare SQL engine. |

## Ingestion pipeline

```
IngestEventsHandler → Queue.Enqueue(event)          (non-blocking; false = dropped)
Batcher.Run(ctx) [goroutine, started in main.go]:
  ├─ event from queue → append to batch → flush when len >= BatchSize
  ├─ FlushInterval ticker → flush partial batch
  └─ ctx.Done / channel closed → final flush, return
Batcher.flush: writer.WriteBatch with up to maxFlushRetries(5) × linear backoff
               (flushRetryBaseWait×attempt); drops the batch after 5 failures or on ctx cancel.
```

- **Queue overflow no longer over-counts** — the ingestion handler now counts only
  events `Enqueue` returned `true` for, so `accepted` is accurate; `/health` still
  surfaces the real `dropped` counter via `Queue.Stats()`. The handler also parses the
  whole body before enqueuing anything, so a malformed line is all-or-nothing (400,
  retry-safe).
- **`Writer` is an interface** (`batcher.go:12`) — the real impl is `db.Writer`
  (ClickHouse batch insert), passed in `main.go`. This is the seam a unit test would
  mock; do it here first if adding tests.

## SSE hub

`hub.go` maintains a set of subscriber channels; `Publish` non-blockingly sends each
event to every subscriber. `routes/stream.go` registers a subscriber and streams
`data: <json>\n\n` frames with a 15s `: keepalive`.

> `/v1/events/stream` works: `middleware/logging.go`'s `loggingResponseWriter` now
> forwards `Flush`/`Hijack`/`Unwrap`, and the handler clears its write deadline so the
> 30s `WriteTimeout` doesn't cut the stream (root §9 changelog).

## Query / analytics engines

- Both build SQL with squirrel and run it via the ClickHouse driver.
- **Both `analytics.go` and `query.go` validate `data.*` field names** with
  `safeIdentifierRegex` and reject unknown columns. In `query.go`, `applyDataFilter`
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
