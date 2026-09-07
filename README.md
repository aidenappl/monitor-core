# monitor-core

The event ingestion, query, and observability API for the Monitor platform — a
high-performance service that ingests monitoring events into ClickHouse and owns
everything relational in MariaDB: identity/authentication (native accounts + pluggable
SSO), the tenancy registry, issue tracking, and the alerting and dashboard configuration.

> **Monitor platform** · Go API · `monitor.appleby.cloud` (Lattice)

## Architecture

```
go services (go-monitor / monitor-js SDK)          monitor-web (Next.js)
  ↓ (batched NDJSON over HTTP, X-Api-Key)             ↓ (proxy: mon-* cookies + X-CSRF-Token)
monitor-core ──────────────────────────────────────────────────────────
  ↓ (batched inserts)          ↓ (rows: identity, tenancy, issues, alert + dashboard config)
ClickHouse (events)          MariaDB (monitor_auth + monitor)
```

**Which store owns what** — the rule is *facts vs configuration*. ClickHouse holds rows
written by machines, appended at volume and expired by a TTL: `events`,
`issue_occurrences_daily`, `alert_states`, `alert_history`. MariaDB holds rows a human
edits and whose correctness depends on constraints: users, api keys, zones/projects,
issues, service→repo mappings, and — since migrations 119-124 — alert rules, notification
channels, notification policies, service groups, dashboards and saved views. See
[AGENTS.md](AGENTS.md) §6 *Stores — configuration vs facts* for the argument and for the
two production defects that produced it.

## Features

- **HTTP ingestion endpoint**: `POST /v1/events` accepts NDJSON (newline-delimited JSON)
- **Gzip support**: Automatically handles gzip-compressed request bodies
- **Streaming parser**: Processes events line-by-line without loading entire body into memory
- **Batched writes**: Collects events and writes to ClickHouse in configurable batches
- **Non-blocking ingestion**: HTTP handler enqueues events and returns immediately
- **Query + analytics API**: search, autocomplete, aggregation, time series, top-N, gauge, compare
- **Alerting, issues, dashboards, saved views** and two SSE streams (live tail + alert feed)
- **Native authentication**: email+password accounts (bcrypt 12) + Monitor-owned HS512
  JWT sessions (15m access / rotating 7d refresh with reuse detection)
- **Pluggable SSO**: config-driven OIDC/OAuth2 providers with true account linking (one
  user, many identities) — any IdP is a config row, not a code change
- **API-key authentication** for ingest/query: via the `X-Api-Key` header

## Quick Start

### 1. Start local ClickHouse + MariaDB

```bash
dev up
```

`dev up` brings up both datastores from `docker-compose.yml`: ClickHouse (events) and
MariaDB (`monitor_auth`, host port 3336 — the auth data layer).

### 2. Schema migrations (automatic)

Both schemas are applied **automatically at startup**, fail-fast:

- **ClickHouse** — `migrations/embed.go` embeds `migrations/*.sql` (idempotent
  `IF NOT EXISTS`), run from `main.go` right after connecting. The DDL is written against
  the literal database `monitor` and rewritten to `CLICKHOUSE_DATABASE` before it runs, so
  the migrated database is always the one being served; `main.go` then reads the `events`
  table once and refuses to start if it can't.
- **MariaDB** — `db/sql.go`'s `db.RunMigrations()` embeds `db/migrations/*.sql`, runs each
  once, tracked in a `migrations_applied` table.

A fresh deploy self-migrates — no manual step is required. It then seeds the first admin
user and the tenancy registry (one zone, one project — `MON_ZONE_SLUG` /
`MON_DEFAULT_PROJECT`), both no-ops once the rows exist.

`migrations/manual/` is the exception: one-off reconciliation SQL that is **not** embedded,
**not** rewritten for `CLICKHOUSE_DATABASE`, and run by hand. Each file's header carries its
own before/watch/after queries; the operator runbook for all of them — what each does, when
to run it, and how to confirm the mutation landed — is [AGENTS.md](./AGENTS.md) §8.

### 3. Configure environment

`MONITOR_API_KEY` is **required** — the master ingest/query key (header `X-Api-Key`); the
server refuses to start without it. In a **production profile** (`MON_COOKIE_INSECURE`
unset/false) the server also refuses to start unless `MON_JWT_SIGNING_KEY` and a 32-byte
`MON_CRYPTO_KEY` are set to non-default values.

```bash
# Ingest/query
export MONITOR_API_KEY="your-secret-key"

# Auth (local dev — allows the dev-default session/crypto keys)
export MON_COOKIE_INSECURE=true
export MON_DB_DSN="monitor:monitor@tcp(127.0.0.1:3336)/monitor_auth"
export MON_ADMIN_EMAIL="you@example.com"     # seeds the first admin on a fresh DB
export MON_ADMIN_PASSWORD="a-strong-password"
```

To sign in with SSO locally, add a provider via `POST /admin/sso-providers` (or the
`/admin/sso` page in `monitor-web`). See the full env table under
[Configuration](#configuration) and the auth model in
[AGENTS.md](./AGENTS.md) §6. All other defaults work with `dev up`.

### 4. Run the service

```bash
dev run
```

Or build and run:

```bash
go build -o bin/monitor-core .
./bin/monitor-core
```

### Docker (Production)

```bash
docker-compose up -d
```

## API

### Health Check

```bash
curl http://localhost:8080/health
```

Response:

```json
{
  "status": "ok",
  "enqueued": 0,
  "dropped": 0,
  "pending": 0,
  "clickhouse_ok": true,
  "mariadb_ok": true,
  "last_flush_at": "2026-09-06T12:00:00Z",
  "role": "both",
  "zone": "trailblaze"
}
```

`/health` is **liveness** — it always returns `200 / "ok"`, even with a store down (the
container healthcheck points here, and a restart cannot repair ClickHouse). The
dependency booleans and `last_flush_at` are diagnostics; `dropped` counts both queue
overflow and batches the writer gave up on. `role` echoes `MON_ROLE` (see **Roles** under
Configuration) so the plane a process thinks it is can be asked rather than inferred.

`zone` echoes `MON_ZONE_SLUG` — this process's **identity**. It is what makes
`POST /admin/zones/{id}/probe` able to tell "a monitor-core answered" from "the
monitor-core I meant answered": every zone runs this same binary and every one of them
answers `200`, so without it a registry row pointed at the wrong box looks perfectly
healthy while every read through it returns another tenant's data. Read `role` alongside
it — a control plane carries a zone slug too and serves no events at all.

### Readiness

```bash
curl -i http://localhost:8080/ready
```

`200` when ClickHouse and MariaDB both answer a 2s ping, otherwise `503`:

```json
{ "status": "not_ready", "clickhouse_ok": false, "mariadb_ok": true, "role": "both", "failing": ["clickhouse"] }
```

Under `MON_ROLE=app` there is no ClickHouse connection at all, so it is not counted as a
dependency and `clickhouse_ok: false` does not make the process un-ready — that is what
`role` on this response is for. Every other role still requires it.

### Ingest Events

```bash
curl -X POST http://localhost:8080/v1/events \
  -H "Content-Type: application/x-ndjson" \
  -H "X-Api-Key: your-secret-key" \
  -d '{"timestamp":"2026-02-06T23:01:02.123Z","service":"users","job_id":"job_x","request_id":"req_y","trace_id":"trc_z","name":"user.created","data":{"user_id":42}}
{"timestamp":"2026-02-06T23:01:02.456Z","service":"users","job_id":"job_x","request_id":"req_y","trace_id":"trc_z","name":"db.query","data":{"table":"users"}}'
```

Response:

```json
{ "accepted": 2 }
```

### Event Format

Each event must be a JSON object on its own line with these fields:

| Field        | Type             | Required | Description                                   |
| ------------ | ---------------- | -------- | --------------------------------------------- |
| `timestamp`  | string (RFC3339) | Yes      | When the event occurred                       |
| `service`    | string           | Yes      | Service name that generated the event         |
| `name`       | string           | Yes      | Event type/name                               |
| `env`        | string           | No       | Environment (e.g., production, staging)       |
| `job_id`     | string           | No       | Groups related requests within a service      |
| `request_id` | string           | No       | Unique identifier per incoming request        |
| `trace_id`   | string           | No       | Spans across services for distributed tracing |
| `user_id`    | string           | No       | User identifier for user-scoped queries       |
| `level`      | string           | No       | Log level (info, warn, error, debug)          |
| `data`       | object           | No       | Additional event data                         |

### Query Events

Query events with filters (Grafana-style):

```bash
curl "http://localhost:8080/v1/events?service=users&level=error&limit=50" \
  -H "X-Api-Key: your-secret-key"
```

**Query Parameters:**

| Parameter    | Description                                    |
| ------------ | ---------------------------------------------- |
| `service`    | Filter by service name                         |
| `env`        | Filter by environment                          |
| `job_id`     | Filter by job ID                               |
| `request_id` | Filter by request ID                           |
| `trace_id`   | Filter by trace ID                             |
| `user_id`    | Filter by user ID                              |
| `name`       | Filter by event name                           |
| `level`      | Filter by log level                            |
| `from`       | Start time (RFC3339 or Unix timestamp)         |
| `to`         | End time (RFC3339 or Unix timestamp)           |
| `data.<key>` | Filter by data field (e.g., `data.user_id=42`) |
| `limit`      | Results per page (default: 100, max: 1000)     |
| `offset`     | Pagination offset                              |

**Filter Operators:**

Filters support operators using Django-style syntax: `field__operator=value`

| Operator     | Example                          | Description             |
| ------------ | -------------------------------- | ----------------------- |
| `eq`         | `service=users` or `service__eq` | Equals (default)        |
| `neq`        | `level__neq=debug`               | Not equals              |
| `lt`         | `data.count__lt=100`             | Less than               |
| `gt`         | `data.count__gt=10`              | Greater than            |
| `lte`        | `data.latency__lte=500`          | Less than or equal      |
| `gte`        | `data.latency__gte=100`          | Greater than or equal   |
| `contains`   | `name__contains=user`            | Contains substring      |
| `startswith` | `service__startswith=auth`       | Starts with             |
| `endswith`   | `name__endswith=.error`          | Ends with               |
| `in`         | `level__in=error,warn`           | Matches any (comma-sep) |

**Examples:**

```bash
# Find errors and warnings
curl "http://localhost:8080/v1/events?level__in=error,warn"

# Find events with latency > 500ms
curl "http://localhost:8080/v1/events?data.latency_ms__gt=500"

# Find user-related events
curl "http://localhost:8080/v1/events?name__contains=user"

# Exclude debug logs
curl "http://localhost:8080/v1/events?level__neq=debug"
```

Response:

```json
{
  "success": true,
  "message": "request was successful",
  "pagination": { "count": 150, "next": "/v1/events?offset=100&limit=100", "previous": "" },
  "data": [{ "timestamp": "...", "service": "users", ... }]
}
```

### Label Autocomplete

Get distinct values for a label (service, env, name, level):

```bash
curl "http://localhost:8080/v1/labels/service/values" \
  -H "X-Api-Key: your-secret-key"
```

Response:

```json
{
  "success": true,
  "message": "request was successful",
  "data": ["users", "orders", "payments"]
}
```

### Data Keys Autocomplete

Get available keys from the `data` JSON column:

```bash
curl "http://localhost:8080/v1/data/keys?service=users" \
  -H "X-Api-Key: your-secret-key"
```

Response:

```json
{
  "success": true,
  "message": "request was successful",
  "data": ["client_ip", "host", "method", "path", "user_agent"]
}
```

### Data Values Autocomplete

Get values for a specific data key:

```bash
curl "http://localhost:8080/v1/data/values?key=method&service=users" \
  -H "X-Api-Key: your-secret-key"
```

Response:

```json
{
  "success": true,
  "message": "request was successful",
  "data": ["GET", "POST", "PUT", "DELETE"]
}
```

## Analytics API

The analytics API provides Grafana-compatible endpoints for building dashboards, charts, and gauges.

### Analytics Query

Aggregate data with optional grouping:

```bash
# Count events grouped by service
curl -X POST "http://localhost:8080/v1/analytics" \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: your-secret-key" \
  -d '{
    "aggregation": "count",
    "group_by": ["service"],
    "from": "2026-02-01T00:00:00Z",
    "to": "2026-02-06T23:59:59Z"
  }'
```

**Request Body:**

| Field         | Type     | Required | Description                                                   |
| ------------- | -------- | -------- | ------------------------------------------------------------- |
| `aggregation` | string   | No       | Aggregation type (default: `count`)                           |
| `field`       | string   | \*       | Field to aggregate (required for sum/avg/min/max/percentiles) |
| `group_by`    | string[] | No       | Fields to group by (max 10)                                   |
| `filters`     | object[] | No       | Filter conditions                                             |
| `from`        | string   | No       | Start time (RFC3339 or Unix)                                  |
| `to`          | string   | No       | End time (RFC3339 or Unix)                                    |
| `order_by`    | string   | No       | Field to order by (`value` or group field)                    |
| `order_desc`  | boolean  | No       | Order descending                                              |
| `limit`       | integer  | No       | Max results (default: 100, max: 10000)                        |

**Aggregation Types:**

| Type           | Description              | Requires Field |
| -------------- | ------------------------ | -------------- |
| `count`        | Count of events          | No             |
| `count_unique` | Count of unique values   | Yes            |
| `sum`          | Sum of numeric field     | Yes            |
| `avg`          | Average of numeric field | Yes            |
| `min`          | Minimum value            | Yes            |
| `max`          | Maximum value            | Yes            |
| `p50`          | 50th percentile          | Yes            |
| `p90`          | 90th percentile          | Yes            |
| `p95`          | 95th percentile          | Yes            |
| `p99`          | 99th percentile          | Yes            |

**Filter Format:**

```json
{
  "filters": [
    { "field": "service", "operator": "eq", "value": "users" },
    { "field": "data.status_code", "operator": "gte", "value": 400 }
  ]
}
```

Response:

```json
{
  "success": true,
  "data": {
    "data": [
      { "value": 1523, "groups": { "service": "users" } },
      { "value": 892, "groups": { "service": "orders" } }
    ],
    "total": 2
  }
}
```

**GET endpoint** (query-string based):

```bash
curl "http://localhost:8080/v1/analytics?aggregation=count&group_by=service&from=2026-02-01T00:00:00Z"
```

### Time Series Query

Get time-bucketed data for charts:

```bash
curl -X POST "http://localhost:8080/v1/timeseries" \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: your-secret-key" \
  -d '{
    "aggregation": "count",
    "interval": "hour",
    "filters": [{ "field": "name", "operator": "eq", "value": "user.login" }],
    "from": "2026-02-05T00:00:00Z",
    "to": "2026-02-06T23:59:59Z",
    "fill_zeros": true
  }'
```

**Request Body:**

| Field         | Type     | Required | Description                                  |
| ------------- | -------- | -------- | -------------------------------------------- |
| `aggregation` | string   | No       | Aggregation type (default: `count`)          |
| `field`       | string   | \*       | Field to aggregate                           |
| `interval`    | string   | Yes      | Time bucket size                             |
| `group_by`    | string[] | No       | Fields to group by (creates multiple series) |
| `filters`     | object[] | No       | Filter conditions                            |
| `from`        | string   | No       | Start time                                   |
| `to`          | string   | No       | End time                                     |
| `fill_zeros`  | boolean  | No       | Fill empty buckets with zero                 |

**Interval Types:** `minute`, `hour`, `day`, `week`, `month`

Response:

```json
{
  "success": true,
  "data": {
    "series": [
      {
        "name": "",
        "data_points": [
          { "timestamp": "2026-02-05T00:00:00Z", "value": 42 },
          { "timestamp": "2026-02-05T01:00:00Z", "value": 38 }
        ]
      }
    ]
  }
}
```

**GET endpoint:**

```bash
curl "http://localhost:8080/v1/timeseries?interval=hour&name=user.login&fill_zeros=true"
```

### Top N Query

Get top N values for a dimension:

```bash
curl -X POST "http://localhost:8080/v1/topn" \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: your-secret-key" \
  -d '{
    "aggregation": "count",
    "group_by": "data.endpoint",
    "limit": 10,
    "from": "2026-02-01T00:00:00Z",
    "to": "2026-02-06T23:59:59Z"
  }'
```

Response:

```json
{
  "success": true,
  "data": {
    "data": [
      { "key": "/api/users", "value": 5234 },
      { "key": "/api/orders", "value": 3891 }
    ]
  }
}
```

### Gauge Query

Get a single aggregated value:

```bash
curl -X POST "http://localhost:8080/v1/gauge" \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: your-secret-key" \
  -d '{
    "aggregation": "count",
    "filters": [{ "field": "level", "operator": "eq", "value": "error" }],
    "from": "2026-02-06T00:00:00Z",
    "to": "2026-02-06T23:59:59Z"
  }'
```

Response:

```json
{
  "success": true,
  "data": { "value": 127 }
}
```

### Compare Query

Compare current period with a previous period:

```bash
curl -X POST "http://localhost:8080/v1/compare" \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: your-secret-key" \
  -d '{
    "aggregation": "count",
    "filters": [{ "field": "name", "operator": "eq", "value": "http.request" }],
    "from": "2026-02-06T00:00:00Z",
    "to": "2026-02-06T23:59:59Z"
  }'
```

Response:

```json
{
  "success": true,
  "data": {
    "current": 1523,
    "previous": 1342,
    "change": 181,
    "change_percent": 13.49
  }
}
```

If `compare_from`/`compare_to` are not specified, the previous period is auto-calculated based on the duration of the current period.

## Authentication

Monitor owns identity end-to-end — it delegates to no external identity provider. Two
credential kinds coexist:

- **API keys** (`X-Api-Key`) — for ingestion (`POST /v1/events`, **`ingest` scope only** —
  an `admin` key is a query credential and is refused here with a `401`) and machine query
  callers (`admin` scope). The env master key is `MONITOR_API_KEY` and still does both.
  Revoking a key takes effect within 30s, the cache refresh interval. Every key also
  **binds to a project** (`api_keys.project_id`, `NOT NULL`), which is where ingestion
  derives an event's tenant from; `POST /v1/api-keys` accepts an optional `project_slug`
  and defaults to `MON_DEFAULT_PROJECT`. Key names are unique **per project**, so each
  project may own an `ingest` key — but the key *hash* stays unique instance-wide, because
  a presented key must resolve to exactly one row. That binding also decides what an
  `admin` key can **read**: every event query is scoped to the key's own project (see
  below). The env master key and dashboard sessions read `MON_DEFAULT_PROJECT`.
- **Sessions** — for the `monitor-web` dashboard. Native email+password accounts (bcrypt
  cost 12) or pluggable SSO mint a Monitor-owned HS512 JWT session: a 15-minute access
  token and a rotating 7-day refresh token with reuse detection. Delivered as the `mon-*`
  cookies (`mon-access-token`, `mon-refresh-token`, `mon-logged-in`, `mon-csrf`); unsafe
  requests are CSRF-protected via a double-submit `mon-csrf` ↔ `X-CSRF-Token` check.

Auth/identity data lives in MariaDB (`monitor_auth`): `users`, `identities`
(`UNIQUE(provider, provider_user_id)` — true account linking, one user ↔ many sign-in
methods), `refresh_tokens`, `sso_providers`, `sso_sessions`, `settings`, `api_keys`, plus
the tenancy registry (`zones`, `projects`). The second MariaDB schema, `monitor`, holds
observability data on the same connection: `issues`, `issue_timeline`, `issue_links`,
`service_repos`, and the alerting/dashboard configuration (`alert_rules`,
`notification_channels`, `notification_policies`, `service_groups`, `dashboards`,
`saved_views`).

Endpoint surface (browser/cookie-oriented, outside `/v1`):

```
POST   /auth/login | /auth/register | /auth/refresh | /auth/logout
GET/PUT /auth/self
GET    /auth/self/identities   POST/DELETE /auth/self/identities/{slug}
GET    /auth/sso/config | /auth/sso/{slug}/login | /auth/sso/{slug}/callback
GET/POST /admin/sso-providers   PUT/DELETE /admin/sso-providers/{slug}   (admin only)
```

The SSO protocol itself lives in [`go-forta/sso`](https://github.com/aidenappl/go-forta/tree/main/sso)
(v1.6.0) — this repo keeps only the parts that library refuses to know: the provider-row mapping,
secret resolution, state and session storage, and the identity-resolution rules.

SSO providers are config rows (OIDC via discovery, or explicit-URL OAuth2). Adding one —
Google, Okta, Entra, Forta, anything else — is a `POST /admin/sso-providers`, never a
build. Linking is nOAuth-safe: identity is keyed on `(provider, subject)`, and
link-on-login only fires when both the IdP email and the existing account email are
verified. Full details in [AGENTS.md](./AGENTS.md) §6.

## Configuration

| Environment Variable  | Default          | Description                                   |
| --------------------- | ---------------- | --------------------------------------------- |
| `HTTP_PORT`           | `8080`           | HTTP server port                              |
| `MON_ROLE`            | `both`           | Which plane this process runs: `app` (control plane — identity/config, **no ClickHouse**), `zone` (data plane — events, ingest, alerts, issues) or `both`. Leave it unset unless you are deliberately splitting the planes; an unrecognised value **refuses to boot** rather than falling back. See **Roles** below |
| `CLICKHOUSE_ADDR`     | `localhost:9000` | ClickHouse server address                     |
| `CLICKHOUSE_DATABASE` | `monitor`        | ClickHouse database name; must match `^[a-z][a-z0-9_]{2,62}$` (it is interpolated into SQL, and the migrations are rewritten to it) |
| `CLICKHOUSE_USERNAME` | `default`        | ClickHouse username                           |
| `CLICKHOUSE_PASSWORD` | ``               | ClickHouse password                           |
| `CLICKHOUSE_MAX_MEMORY_USAGE` | `2147483648` | Per-**query** memory ceiling in bytes (2 GiB), sent as the `max_memory_usage` setting. Unset, the effective limit is ~90% of host RAM, so one high-cardinality `GROUP BY` can OOM-kill the server and take ingest with it |
| `MONITOR_API_KEY`     | *(required)*     | Master API key (header `X-Api-Key`); server refuses to start if unset |
| `BATCH_SIZE`          | `1000`           | Number of events per batch insert             |
| `FLUSH_INTERVAL`      | `5s`             | Max time to wait before flushing batch        |
| `QUEUE_SIZE`          | `100000`         | Max events in memory queue                    |
| `MON_DB_DSN`          | `monitor:monitor@tcp(127.0.0.1:3336)/monitor_auth` | MariaDB DSN (auth data layer); from Keyring in prod |
| `MON_JWT_SIGNING_KEY` | *(dev default)*  | HS512 session-token key; **prod must override** |
| `MON_CRYPTO_KEY`      | *(dev default)*  | **Exactly 32 bytes** — AES-256-GCM key for SSO secrets/tokens; **prod must override** |
| `MON_COOKIE_DOMAIN`   | ``               | Domain set on the `mon-*` cookies             |
| `MON_COOKIE_INSECURE` | `false`          | `true` for local dev: drops `Secure`, allows dev-default secrets |
| `MON_PUBLIC_URL`      | `https://monitor.appleby.cloud` | Origin used to build each SSO `redirect_uri`; must match the IdP registration |
| `MON_ADMIN_EMAIL` / `MON_ADMIN_PASSWORD` | `` | Seed the first admin on a fresh DB; empty = no seed |
| `MON_ALLOW_REGISTRATION` | `false`       | Gates `POST /auth/register` (self-registration) |
| `MON_ZONE_SLUG`       | `trailblaze`     | Slug of the single zone seeded at boot. Slugs are immutable — changing it later seeds a second zone, it does not rename the first |
| `MON_DEFAULT_PROJECT` | `default`        | Slug of the project seeded inside that zone; every API key binds to it, the env master key stamps events with it, and it is the project the master key and dashboard sessions **read**. Also the only project whose reads still match pre-006 events (see Tenancy). **Not boot-only — do not change it on a running install:** it is read on every request, query, streamed event and error fingerprint, and repointing it hides every event the manual backfill has not stamped |

### Roles — the two planes

`monitor-core` does two unrelated jobs in one binary, and `MON_ROLE` says which of them a
process is running.

| | `app` | `zone` | `both` *(default)* |
| --- | --- | --- | --- |
| Identity: `/auth/*`, SSO, admin bootstrap, tenancy seeder | ✅ | — | ✅ |
| Events: ClickHouse, ingest, batcher, SSE hubs, alert evaluator, issues | — | ✅ | ✅ |
| MariaDB, API-key cache, `/health` + `/ready` | ✅ | ✅ | ✅ |

- **Unset means `both`**, which is exactly what the service did before roles existed — so
  deploying this changes nothing until someone opts in.
- **A route the current role cannot serve is not registered**, so it answers `404` instead
  of failing on a dependency the process does not have. Under `MON_ROLE=app` there is no
  ClickHouse connection at all.
- **A typo stops the boot.** `MON_ROLE=zonr` is refused by name rather than quietly
  defaulting — a silent fallback would run the control plane on a host meant to be a zone.
- The resolved role is logged once at startup and reported by `GET /health`.
- Splitting the planes across hosts needs more than this switch (config distribution and
  per-zone credentials are not built); today `both` is the supported topology.

Full per-subsystem breakdown in [AGENTS.md](AGENTS.md) §6 *Roles — the two planes*.

### Tenancy — every event read is project-scoped

An event's project is derived server-side at ingest from the authenticating API key and
overwritten, never trusted from the client. Reads mirror that: `QueryAuthMiddleware`
resolves a project on every accepted branch, and **every** read of `monitor.events` ANDs a
mandatory predicate built by `scope.ProjectPredicate` — the event and label queries, all
five analytics builders, both issue-event lookups, and the SSE live tail. A context with no
project yields an **error**, not an unscoped query, so a route registered outside the
middleware fails loudly instead of serving every project.

**Where that project comes from differs by credential, and the difference is deliberate.**
An **API key** — ingest or admin — gets it from its own `api_keys` row, which is a real,
unforgeable tenancy boundary; an admin key reads only its own project, because "admin" is a
scope over *verbs*, not over tenants. A **logged-in session** instead *selects* one with
`?project=<slug>`, validated against the registry and defaulting to `MON_DEFAULT_PROJECT`.
That selection is a **chooser, not a boundary**: Monitor has no per-user project membership
table, so there is nothing that could constrain it — roles (`admin`/`editor`/`viewer`/
`pending`) gate verbs, which is the authorisation that does exist. Sentry and Grafana work
the same way. A query parameter rather than a header because `EventSource` cannot send
headers and both SSE streams need the same mechanism; an unknown or retired slug is refused
with a `400` naming it rather than silently falling back, which would show one project's
events under the label the user asked for. `AGENTS.md` §6 has the full register, including
what changes when a membership table lands.

`GET /v1/zones` and `GET /v1/zones/{zone}/projects` list the registry so a project switcher
can populate. Both are reads, available to any authenticated session, and neither should be
called with `?project=`, so a stale selection never blocks discovering a valid one. Both hide
retired rows unless asked with `?include_deleted=true` — which the admin registry page does,
because a spent slug that is simply missing from the list looks free.

**Writing the registry is a different privilege and a different plane.** The write surface
lives under `/admin`, is registered only in a role that runs the control plane, and sits
behind `RequireAdmin`:

| Route | What it does |
|-------|--------------|
| `POST /admin/zones` | record a zone — both URLs required |
| `PUT /admin/zones/{id}` | edit `display_name` / the URLs; a `slug` or `status` in the body is **refused**, not ignored. Changing `query_url` **discards the stored probe verdict** (back to `unknown`) — it measured the old address |
| `POST /admin/zones/{id}/retire` | soft-delete; `409` while the zone still owns active projects |
| `POST /admin/zones/{id}/probe` | probe the zone's `query_url` now and persist the verdict |
| `POST /admin/zones/{id}/projects` | create a project inside a zone |
| `PUT /admin/projects/{id}` | edit `display_name`; `slug`, `zone_id` and `status` are refused |
| `POST /admin/projects/{id}/retire` | soft-delete |

Three things about that table are load-bearing. **A zone row records infrastructure; it
does not create any** — the stack, its ClickHouse, its DNS and its certificate are all
provisioned by hand first, and a row whose `query_url` points at the wrong box is
syntactically perfect and renders another tenant's data under this zone's name. **There is
no `DELETE`** anywhere on it: retiring keeps the row forever so the `UNIQUE` key on slug
makes a recycled slug impossible, which is what stops a month of surviving events (30-day
TTL) and a permanent daily rollup reattaching to the next owner. And **slugs are immutable**
at the API, not merely in the UI — a rename that answered `200` and changed nothing would be
invisible here and permanent everywhere else.

`POST /admin/zones/{id}/probe` compares what the far end says it is (`zone` + `role` on its
`/health`) against what the registry expected, and stores one of
`unknown / unconfigured / unreachable / unverified / mismatched / degraded / healthy`.
**An unreachable zone is a `200`** — the probe succeeded and the zone is what is broken —
and `mismatched` is the verdict that pays for the whole mechanism: a `200`-only check would
pass a `query_url` aimed at another zone and mislabel every read made through it.

A verdict belongs to the URL it measured, so **editing `query_url` throws it away** rather
than carrying it forward: `query.UpdateZone` resets `reachability`, `reachability_detail`,
`reported_zone` and `last_probe_at` whenever the address actually changes. Keeping them
would leave the row wearing a green `healthy` earned by a box it no longer points at —
worse than never probing, because `unknown` admits what it does not know. It lives in the
query layer rather than in the handler so it holds for every caller, not just a UI that
remembers to re-probe.

> **Transition, expiring 2026-10-06 at the earliest.** Rows written before
> `migrations/006_events_project.sql` read back with an empty project, and the fill is a
> manual ClickHouse mutation (`migrations/manual/backfill_events_project.sql`). Until it has
> run and the 30-day TTL has recycled the rest, **the default project also matches those
> untagged rows**; every other project matches exactly. Both arms are dated in
> `scope/scope.go` and removed together.

Issues are scoped the same way. `monitor.issues` lives in MariaDB and never meets
`scope.ProjectPredicate`, so `query.scopeIssues` is its counterpart: the list, the count,
every id-addressed read and the update's own `WHERE` all carry the caller's project. An
issue row is not metadata — it holds the error `message` taken off the event — so the
listing is scoped for the same reason the events behind it are.

**Known gap, deliberate:** **timer-driven** alert evaluation is zone-wide — `Evaluator.Run`
has no request and therefore no project, so a rule's threshold counts the whole instance
(a rule can opt in with a `project` filter). The HTTP test endpoint
(`POST /v1/alert-rules/{id}/test`) *does* have a request context and **is** scoped, so the
two can report different numbers once a second project exists. Details in `AGENTS.md` §6.

## Limits

- **Request body size**: 10 MB for ingestion, 1 MB for analytics queries
- **Time series query**: Max 90 days range, max 10,000 data points
- **Analytics query**: Max 10,000 results, max 10 group by fields
- **Top N query**: Max 1,000 results
- **ClickHouse per-query memory**: 2 GiB (`CLICKHOUSE_MAX_MEMORY_USAGE`) — over it, that
  one query fails with `MEMORY_LIMIT_EXCEEDED` (code 241) and nothing else is affected
- **ClickHouse connection retry**: 10 attempts with linear backoff (1s, 2s, ... 10s)

## Development

Use the `dev` CLI for common tasks:

```bash
dev help                  # List available commands
dev up                    # Start local ClickHouse + MariaDB
dev run                   # Run the app (auto-migrates both schemas at startup)
dev check                 # Format, vet, and test
dev down                  # Stop the local stack

# One-time cutovers (argv subcommands — they migrate, copy, then exit)
go run . backfill-issues  # legacy ClickHouse issues → monitor.issues
go run . backfill-config  # legacy ClickHouse alerting/dashboard config → MariaDB.
                          # Must run BEFORE the new binary serves — the server REFUSES
                          # TO BOOT until it has. See AGENTS.md §8
```

## Project Structure

```
monitor-core/
  main.go                     # Entry: config, MON_ROLE validation, DB connects + migrations, bootstrap, sso.Install(), goroutines
  router.go                   # buildRouter(role) — the HTTP surface, each route gated on the plane that can serve it
  Devfile.yaml                # Dev CLI commands
  Dockerfile                  # Multi-stage production build (builds the package, `go build .`)
  docker-compose.yml          # Production stack: monitor-core + ClickHouse + MariaDB (+ monitor-web)
  docker-compose.dev.yml      # Local development stack
  db/
    clickhouse.go             # ClickHouse connection and batch writer (events)
    sql.go                    # MariaDB connection (db.SQL), db.Queryable, db.RunMigrations
    migrations/               # MariaDB DDL: identity + tenancy + issues + alert/dashboard config (100_users … 124_saved_views)
  env/env.go                  # Environment configuration + RequireProductionSecrets guard
  env/role.go                 # MON_ROLE: the Role type (app/zone/both) and the fail-fast RequireValidRole guard
  jwt/jwt.go                  # Monitor-owned HS512 access/refresh JWTs (alg-pinned)
  tools/                      # Password.tool.go (bcrypt 12), Crypto.go (AES-256-GCM), Validate.tool.go (+ NormalizeEndpointURL), Slug.tool.go
  probe/zone.go               # Zone reachability probe: calls a zone's /health + /ready and reports healthy/degraded/unverified/MISMATCHED — the check that catches a registry row pointed at the wrong box
  bootstrap/                  # First-run seeding: admin.go (first admin user), registry.go (zone + default project)
  sso/                        # Pluggable SSO wiring onto go-forta/sso: config.go, resolve.go, checkpoint.go, statestore.go, sessionstore.go, backchannel.go
  query/                      # MariaDB query layer (squirrel): users/identities/refresh_tokens/sso_*/api_keys/zones/projects/issues/service_repos/alert_rules/notification_channels/notification_policies/service_groups/dashboards/saved_views
  scope/                      # The request's project: context plumbing + the predicate every event read must carry
  structs/                    # User/Identity/SSOProvider/SSOSession/RefreshToken/APIKey/Zone/Project/Issue/AlertRule/NotificationChannel/NotificationPolicy/ServiceGroup/Dashboard/SavedView + event/analytics + columns.go (shared identifier regex + column allowlists)
  middleware/
    session.go                # Monitor session auth + Protected/RequireAdmin/RequireEditor/RejectPending
    csrf.go                   # Double-submit CSRF (mon-csrf ↔ X-CSRF-Token)
    ingest_auth.go            # X-Api-Key auth for POST /v1/events (env master key OR ingest-scope key)
    query_auth.go             # X-Api-Key (admin) OR Monitor session for /v1/* reads; injects the project (credential-derived for keys, a validated ?project selector for sessions)
    logging.go header.go      # Request logging (SSE-safe) + Server header
  responder/responder.go      # Standardized JSON response utilities
  routes/                     # HTTP handlers (thin) — auth + SSO + events/query/analytics/… (see routes/AGENTS.md)
  services/                   # queue.go batcher.go hub.go query.go analytics.go (ingestion + query engines)
  registry/                   # Tenancy-registry cache (zone + active projects, 30s refresher) — validates a session's ?project selector
  apikeys/ alerts/ issues/    # Subsystems (each Init()s from main.go)
  dashboards/ views/          # EMPTY — their tables moved to MariaDB (123/124); the files are tombstones pointing at query/ and structs/
  cutover/config.go           # One-time ClickHouse → MariaDB copy of the six config tables (`monitor-core backfill-config`)
  cutover/guard.go            # Refuses the boot while that copy is outstanding — a mis-ordered cutover would otherwise be silent
  migrations/
    embed.go                  # In-app ClickHouse migration runner (//go:embed *.sql), rewrites the database name
    001_schema.sql 002_add_user_id.sql 003_api_keys.sql 004_events_issue_id.sql
    005_issue_occurrences_daily.sql 006_events_project.sql
    manual/                   # One-off reconciliation SQL — not embedded, not rewritten, run by hand
```

## Querying Events

```sql
SELECT * FROM monitor.events LIMIT 10;

-- Find events by trace
SELECT * FROM monitor.events WHERE trace_id = 'trc_z';

-- Find events by service and time range
SELECT * FROM monitor.events
WHERE service = 'users'
  AND timestamp >= '2026-02-06 00:00:00';
```
