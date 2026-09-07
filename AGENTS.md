# AGENTS.md — monitor-core

> The comprehensive working document for this repo. An agent that reads only this
> file should be able to work in monitor-core correctly. Keep it current — see
> **Keeping this file updated** at the bottom.

---

## 1. What this repo is

`monitor-core` is the **event ingestion + query + observability API** for the Monitor
platform (`monitor.appleby.cloud`) — the self-hosted replacement for Datadog / Sentry
in the `appleby.cloud` ecosystem.

It **owns**:

- The HTTP ingestion endpoint that receives NDJSON events from services (via the
  `go-monitor` and `monitor-js` SDKs) and batch-writes them to ClickHouse.
- The query + analytics API (events search, label/data autocomplete, aggregation,
  time series, top-N, gauge, compare) that powers the `monitor-web` dashboard.
- The **alerting engine** — rules, evaluation loop, notification channels, routing
  policies, service groups, alert history.
- **Issue tracking** — grouping error/fatal events by fingerprint (Sentry-style).
- **Tenancy** — the zone/project registry, the project every API key is bound to, and the
  scoping that binding puts on every event and issue read (see §6 Tenancy).
- **Dashboards**, **saved views**, and **API-key management** persistence.
- **Identity & authentication** — native accounts (email+password), Monitor-owned
  session tokens, and a pluggable, config-driven SSO subsystem — all owned here
  (see §6 Auth).
- Two Server-Sent-Events (SSE) streams: a live event tail and a live alert feed.

It **does not** own: the UI (that's `monitor-web`) or the client SDKs (`go-monitor`,
`monitor-js`).

Those responsibilities divide into **two planes**, and since Phase 2 the binary says which
of them it is running via **`MON_ROLE`** (`app` / `zone` / `both`, default `both`):

- **Control plane (`app`)** — identity and configuration: accounts, sessions, SSO, the
  admin bootstrap and the tenancy seeder. Holds **no ClickHouse connection at all**.
- **Data plane (`zone`)** — events: ingestion, the batcher, both SSE hubs, the alert
  evaluator, the issue tracker. Verifies sessions but issues none.
- **`both`** — one process doing both jobs. **This is the deployed configuration and the
  default**, and it behaves exactly as it did before roles existed.

See §6 *Roles — the two planes* for what each one runs, which routes it registers, and
what is deliberately **not** built yet.

---

## 2. Stack & dependencies

- **Language:** Go 1.25 (`go.mod` — `go 1.25.5`).
- **Router:** `github.com/gorilla/mux`.
- **SQL builder:** `github.com/Masterminds/squirrel` (imported as `sq`) — **no ORM**.
- **Datastores — two of them:**
  - **ClickHouse** (`github.com/ClickHouse/clickhouse-go/v2`) — events + analytics.
    The columnar workload needs it. DB `monitor` by default and `CLICKHOUSE_DATABASE`
    otherwise — the name is checked by `db.ValidateDatabaseName` before it is used and
    the migration runner follows it (§4) — accessed via the ClickHouse connection in
    `db/clickhouse.go`.
  - **MariaDB** (`github.com/go-sql-driver/mysql`) — everything that is a **row** rather
    than an event, across two schemas on one connection: `monitor_auth` (identity and
    tenancy — users, identities, refresh_tokens, sso_providers, sso_sessions, settings,
    api_keys, zones, projects) and `monitor` (issue tracking, service_repos, and since
    migrations 119-124 the whole alerting/dashboard **configuration**). Accessed via
    `db.SQL` (`db/sql.go`); this is the standard-shape `db.Queryable` +
    squirrel-against-`database/sql` stack. **Which store owns what, and why, is §6
    *Stores — configuration vs facts*.**
- **CORS:** `github.com/rs/cors`.
- **Sessions/JWT:** `github.com/golang-jwt/jwt/v5` (Monitor-owned HS512 tokens).
- **SSO:** `github.com/aidenappl/go-forta/sso` **v1.6.0** — the shared SSO module. It brings
  `coreos/go-oidc/v3` and `golang.org/x/oauth2` transitively; this repo no longer imports
  either directly.
- **Passwords/crypto:** `golang.org/x/crypto/bcrypt` (cost 12) + AES-256-GCM (`tools/Crypto.go`).
- **Secrets:** `github.com/aidenappl/go-keyring` (optional at startup — see §4).
- **IDs:** `github.com/google/uuid`.

> ⚠️ Monitor has **no identity-provider SDK dependency** — every IdP is a config row
> in `sso_providers` (see §6). ClickHouse is still accessed through its native driver,
> not `db.Queryable`; squirrel builds SQL for both stores.

---

## 3. Project structure

Flat, package-per-concern layout (no `cmd/`/`internal/`/`pkg/`).

```
monitor-core/
  main.go                  # Entry: config, MON_ROLE validation, both DB connects + migrations, bootstrap, sso.Install(), router, goroutines, shutdown
  router.go                # buildRouter(role) — the whole HTTP surface, with each route gated on the plane that can serve it (§6 Roles)
  env/env.go               # Env config (getEnv/getEnvInt/getEnvDuration) + RequireProductionSecrets fail-fast guard
  env/role.go              # MON_ROLE: the Role type (app/zone/both), ParseRole, RunsControlPlane/RunsDataPlane, RequireValidRole
  db/
    clickhouse.go          # ClickHouse connection (db.Connect/db.Close) + batch Writer + ValidateDatabaseName + the per-query max_memory_usage ceiling
    sql.go                 # MariaDB connection (db.SQL), db.Queryable, db.RunMigrations (embeds db/migrations/*.sql)
    migrations/            # MariaDB DDL: identity + tenancy + issues + alert/dashboard config (100_users … 124_saved_views)
  jwt/jwt.go               # Monitor-owned HS512 access/refresh JWTs (mint + validate, alg-pinned)
  tools/                   # Password.tool.go (bcrypt 12), Crypto.go (AES-256-GCM), Validate.tool.go (SSRF guard), Slug.tool.go (zone/project slug rule + reserved names)
  bootstrap/               # First-run seeding: admin.go (first admin user), registry.go (the single zone + default project)
  sso/                     # Thin wiring onto go-forta/sso — see §6
  query/                   # MariaDB query layer (squirrel): users, identities, refresh_tokens, sso_providers, sso_sessions, settings, api_keys, zones, projects, issues, service_repos, alert_rules, notification_channels, notification_policies, service_groups, dashboards, saved_views
  scope/scope.go           # The request's project: context key + WithProject/GetProject, ProjectPredicate (the SQL every event read must carry) and Matches (its in-memory twin for the SSE hub)
  structs/                 # User/Identity/SSOProvider/SSOSession/RefreshToken/APIKey/Zone/Project/Issue/AlertRule/NotificationChannel/NotificationPolicy/ServiceGroup/Dashboard/SavedView (.struct.go) + event.go + analytics.go + columns.go (shared identifier regex & column allowlists)
  middleware/
    session.go             # Monitor session auth (Bearer/mon-access-token JWT) + Protected/RequireAdmin/RequireEditor/RejectPending
    csrf.go                # Double-submit CSRF (mon-csrf ↔ X-CSRF-Token); Bearer/X-Api-Key/safe-method exempt
    ingest_auth.go         # X-Api-Key auth for POST /v1/events (env master key OR ingest-scope DB key); injects the credential's project
    query_auth.go          # X-Api-Key (admin) OR a Monitor session for /v1/* reads; injects the project EVERY read is scoped to — credential-derived for keys, a validated ?project SELECTOR for sessions (§6)
    logging.go header.go   # RequestID + logging (SSE-safe); Server header
  responder/responder.go   # Standard JSON envelope helpers
  routes/                  # HTTP handlers (thin) — see routes/AGENTS.md
    HandleLogin/Register/Refresh/Logout/GetSelf/Identities.router.go   # native auth + self + identities
    HandleSSOConfig/Login/Callback.router.go, HandleAdminSSOProviders.router.go, RegisterSSORoutes.go
    cookies.go session.go  # mon-* cookie writing; issueSession (mint + persist refresh-token family)
    events.go query.go analytics.go stream.go api_keys.go dashboards.go views.go issues.go alerts.go
    HandleListZones/HandleListProjects.router.go   # GET /v1/zones, GET /v1/zones/{zone}/projects — the switcher's registry reads
    HandleAdminZones.router.go     # POST /admin/zones, PUT /admin/zones/{id}, POST .../retire, POST .../probe — plus the shared registryWriteError / registryPathID / decodeJSONBody helpers
    HandleAdminProjects.router.go  # POST /admin/zones/{id}/projects, PUT /admin/projects/{id}, POST .../retire
    health.go              # GET /ready (readiness) + the 2s dependency pings /health also reports
    HandleGitHubWebhook.router.go  # POST /webhooks/github — root-mounted, HMAC-authenticated
  github/                  # GitHub link parsing, live-state client, webhook signature verification
    parse.go client.go verify.go
  apikeys/                 # API-key cache (backed by MariaDB, was ClickHouse) + a 30s background refresher
  probe/zone.go            # Zone reachability probe: GET {query_url}/health + /ready, compares the zone the far end REPORTS against the slug the registry expected, returns a structs.ZoneReachability. 3s total budget (probe.ZONE_PROBE_TIMEOUT); writes nothing itself
  registry/registry.go     # Tenancy-registry cache: this install's zone + its active projects, same 30s ticker shape as apikeys — what a session's ?project selector is validated against
  alerts/ issues/          # Subsystems (each Init()s from main.go). alerts/ keeps only what did not move to MariaDB: the evaluator, router, notifier, hub, and the ClickHouse alert_states/alert_history
  dashboards/ views/       # EMPTY — their tables moved to MariaDB (123/124); the files are tombstones that say where everything went
  cutover/config.go        # One-time ClickHouse → MariaDB copy for the six config tables (`monitor-core backfill-config`). Its own package so it can be deleted whole
  cutover/guard.go         # Boot refusal when that copy has not run — the only thing that makes a mis-ordered cutover loud (§8)
  migrations/              # ClickHouse DDL (001_schema … 006_events_project) + embed.go (in-app runner, rewrites the database name)
    manual/                # one-off reconciliation SQL — NOT embedded, NOT rewritten, run by hand
  Devfile.yaml Dockerfile docker-compose.yml docker-compose.dev.yml
```

Sub-package deep dives: `routes/AGENTS.md`, `services/AGENTS.md`, `alerts/AGENTS.md`,
`issues/AGENTS.md`.

---

## 4. Running, building & testing

Uses the `dev` CLI (`Devfile.yaml`). Prerequisites: Go 1.25, Docker (for local
ClickHouse **and** MariaDB — both are in `docker-compose.yml`).

```bash
dev up          # start local ClickHouse + MariaDB
dev run         # sources .env then `go run .`
dev build       # go build -o bin/monitor-core .
dev check       # gofmt -w -s . && go vet ./... && go test ./...
dev down        # stop the local stack

# One-time cutovers, as argv subcommands (never a second `package main` — §9).
# Both apply the migrations first, then copy, then exit without serving.
go run . backfill-issues   # legacy ClickHouse issues  → monitor.issues        (324f612)
go run . backfill-config   # legacy ClickHouse config  → monitor.alert_rules,
                           # notification_channels, notification_policies,
                           # service_groups, dashboards, saved_views (§8 — ORDER MATTERS)
```

- **No `.env` is created for you** — set the vars in §6/§8. The server **refuses to
  start** if `MON_ROLE` names something that is not a role (`env.RequireValidRole`, the
  first guard `main` applies after `env.Load` — unset is fine and means `both`), if
  `MONITOR_API_KEY` is unset, and (in a non-dev profile) if
  `MON_JWT_SIGNING_KEY` / `MON_CRYPTO_KEY` are unset or still the committed dev
  defaults, or `MON_CRYPTO_KEY` isn't exactly 32 bytes (`env.RequireProductionSecrets`,
  called from `main.go`). Set `MON_COOKIE_INSECURE=true` for local dev to permit the
  fallbacks.
- **Both schemas migrate automatically at startup, fail-fast:**
  - **ClickHouse** — `migrations.RunMigrations(ctx)` (`migrations/embed.go`) embeds
    `migrations/*.sql`, runs each in sorted order (all `IF NOT EXISTS`, no tracking table).
    Every file runs on **every boot**, and files are split naively on `;` — so never put
    a semicolon inside a comment, and never leave a comment after the final statement.
    Either produces a comment-only fragment that ClickHouse rejects, which is fatal at
    startup.
    Each file's text is **rewritten from the literal `monitor` to `CLICKHOUSE_DATABASE`
    before the split** (`migrations.rewriter`), because ingestion and every query
    interpolate `db.Database`: without it a non-default `CLICKHOUSE_DATABASE` migrates one
    database and serves another, and looks healthy while losing every event. **Write new
    files against the literal `monitor.` prefix** — an unqualified table name resolves
    against the connection's default database and defeats the rewrite. Files under
    `migrations/manual/` are not rewritten.
    `main.go` then **probes `SELECT count() FROM <database>.events`** and `log.Fatalf`s if
    it fails, so a mismigrated zone crash-loops instead of booting green.
  - **MariaDB** — `db.RunMigrations()` (`db/sql.go`) embeds `db/migrations/*.sql`, runs
    each once, tracked in a `migrations_applied` table. The DSN needs `multiStatements`
    (added automatically by `ensureDSNParams`).
- **Every statement in a MariaDB migration must be re-runnable** — `IF NOT EXISTS` on
  creates and column adds, `DROP … IF EXISTS` before any `ADD CONSTRAINT`. A file is
  recorded in `migrations_applied` only after a clean `Exec`, and DDL commits implicitly
  in MariaDB, so a run that dies partway leaves that work durable and is retried **in
  full** on the next boot. An unguarded `ADD COLUMN` then fails with errno 1060, an
  unguarded `ADD CONSTRAINT` with errno 121 — and because migrations are fail-fast, that
  wedges startup rather than degrading. The guarantee is at-least-once, not exactly-once.
- **Both connects are role-gated** (§6 *Roles*): ClickHouse — connection, migrations and
  boot probe — happens only on the data plane, so under `MON_ROLE=app` `db.Conn` stays
  `nil` by design. MariaDB is opened and migrated in every role.
- **Bootstrap runs after MariaDB migrations** (`main.go`). `bootstrap.EnsureAdminUser`
  seeds the first admin from `MON_ADMIN_EMAIL`/`MON_ADMIN_PASSWORD` (no-op once any user
  exists) and is **control-plane only** — skipped under `MON_ROLE=zone`, which has no
  login surface. `bootstrap.EnsureZoneAndProject` seeds the single zone
  (`MON_ZONE_SLUG`) and its default project (`MON_DEFAULT_PROJECT`) and runs in
  **every role, the data plane included**: a zone owns its own registry rows, so it must
  be able to seed them without a control plane present. Also a no-op once
  both rows exist, and it never resurrects a zone an operator retired. Both are
  fail-fast.
  - The zone's `ingest_url` and `query_url` are both seeded from **`MON_PUBLIC_URL`** —
    the origin this process answers on, since a zone serves ingest and query from one
    base. They are required (`query.CreateZone` rejects an empty `ingest_url` since
    `125_zone_endpoints.sql`), so a fresh zone with no `MON_PUBLIC_URL` fails the boot
    rather than registering an unreachable row. Both columns stay mutable through the
    admin registry, so a wrong-but-valid URL is recoverable — unlike the slug. No SSO provider is seeded from env — providers are created through the admin
  API / `/admin/sso`, never from Go code.
- **Unit tests** cover pure logic and the auth layer with a mocked `Queryable`
  (`jwt/jwt_test.go`, `tools/Password_test.go`, `tools/Slug_test.go`,
  `sso/resolve_test.go`, `routes/HandleIdentities_test.go`, `db/sql_test.go`,
  `bootstrap/registry_test.go`, `registry/registry_test.go`, `query/…`, `scope/…`,
  `alerts/…`, `issues/…`, `services/query_test.go`). No integration tests (no ClickHouse/MariaDB harness); `dev
  test` runs the unit tests. The MariaDB-backed packages are tested against a mocked
  `Queryable` that records the generated SQL, which is why so many of the tenancy tests
  below assert on statement *text*: there is no database here to ask whether the predicate
  was really applied.
- **The tests that pin a contract rather than a function** are worth knowing about before
  changing the thing they guard, because each one exists because the failure it catches is
  invisible at runtime:
  - `router_test.go` — the **route inventory per role**, and above all the full `both`
    surface. `MON_ROLE` defaults to `both`, so that list *is* production: gating a route
    behind a role and deleting it from the running install are the same edit until this
    test refuses it. It also asserts that `both` is exactly the union of `app` and `zone`,
    so no route can fall out of the gating and 404 only in a split deployment.
  - `env/role_test.go` — that an unset `MON_ROLE` resolves to `both` and a typo does
    **not**. A parser that fell back on junk would turn every misspelling into a working
    process — running the control plane on a host meant to be a zone.
  - `services/hub_test.go` — the *whole* SSE filter contract, known keys included, so
    adding a filter key without a `matchesFilters` case fails here instead of in
    production (see the SSE note in `services/AGENTS.md`).
  - `services/batcher_test.go` — that an abandoned batch is counted as `dropped`, and that
    `LastFlushAt` only moves on a successful write. It drives the `ctx.Done()` abandonment
    path; the exhausted-retries path is identical code but would cost 30s of real backoff
    to reach, so it is **not** covered.
  - `migrations/embed_test.go` — runs against the *real* embedded `*.sql`, so it cannot
    drift from the DDL: the database-name rewrite is byte-for-byte identity at the default
    `monitor`, leaves no literal `monitor` behind at another name, and never changes the
    semicolon count the runner splits on.
  - `db/clickhouse_test.go` — what `CLICKHOUSE_DATABASE` values are accepted, since the
    name reaches SQL as text.
  - `middleware/ingest_auth_test.go`, `middleware/query_auth_test.go` — that an empty
    `X-Api-Key` never authenticates, on both middlewares. `ConstantTimeCompare("","")`
    returns 1, so this is one guard clause away from open. The DB-key half of ingest is
    only covered negatively (the `apikeys` cache is package-private and needs a live DB to
    populate); `apikeys/identity_test.go` covers the scope resolution — and, since the
    project binding landed, that the cached `Identity` carries the right project per key.
  - `middleware/query_auth_test.go` (the `TestSessionProject*` group) — the three outcomes of
    the session's project selector: a valid slug is honoured, an unknown or retired one is
    **refused** rather than silently defaulted, and absence falls back to
    `MON_DEFAULT_PROJECT`. The silent fallback is the failure worth a test — it would answer
    with one project's events under the label the user asked for, and nothing about the
    response would look wrong. `TestQueryAuthMasterKeyIgnoresTheProjectSelector` is the other
    half: the selector must not have leaked into the credential-derived branches.
  - `registry/registry_test.go` — that the cache loads active projects for the *configured*
    zone, **pages past `db.DEFAULT_LIMIT`** (a single-shot load would make project 51
    unselectable with no error anywhere), keeps the previous map when a refresh fails, and
    answers `false` on a cold cache. `refreshCacheFrom` takes a `db.Queryable` precisely so
    this is testable — `apikeys` does not, which is why its DB half is only covered
    negatively above.
  - `scope/chokepoint_test.go` — an **AST audit**, not a behavioural test: it walks `main`,
    `services`, `routes`, `issues` and `alerts` and fails when a function names the events
    table without calling a scoping helper. Every other test here can only prove that
    *today's* reads are scoped; this is the one that catches the next read someone adds.
    `go test ./scope/ -v -run TestEveryEvents` prints its exemption map, which is the
    authoritative list of deliberately unscoped event reads.
  - `services/scope_test.go`, `routes/issue_events_scope_test.go`,
    `routes/stream_scope_test.go` — that the predicate actually reaches the generated SQL
    (and, for the SSE path, the hub's filter map) on **every** read entry point, asserted on
    the SQL text. A builder that stopped applying the predicate still runs and still returns
    rows, so nothing short of reading the statement can tell. `services/scope_test.go`
    checks every statement a call issues, because `QueryEvents` emits a count *and* a page
    and `QueryCompare` emits two gauges — a predicate on one of a pair is a real leak.
  - `routes/events_test.go` — that a client-supplied `project` in an NDJSON body is
    overwritten unconditionally, in both halves: that the value genuinely reaches the
    parsed struct first (without which the test would pass vacuously), and that it is gone
    by the time the event is queued.
  - `query/notification_policies_query_test.go` — that the reorder is
    **lock, vacate, then assign, in one transaction**. Under `UNIQUE (position)` the
    obvious loop (`SET position = i+1` per row) violates the key on almost every
    permutation, because InnoDB checks a unique index per *row* and MariaDB has no
    deferrable constraints. The test asserts statement ORDER, so dropping the negating
    `UPDATE`, moving it after the assignments, or taking the whole thing out of its
    transaction each fail here rather than the first time somebody drags a policy. It also
    pins that the seeder fires only on an empty table — the condition that keeps it and
    `backfill-config` from both populating the routing list.
  - `query/alert_rules_query_test.go` — that `condition` is **backticked** wherever it
    appears unqualified. It is a MariaDB reserved word; the reads all keep working, so the
    first symptom of losing the quotes is a 500 the next time somebody edits a rule. Also
    that the partial `UPDATE` touches only the columns the caller named (the omitted
    `enabled` must not disable a rule), and that no MariaDB read carries a leftover
    `FINAL`.
  - `query/config_tables_query_test.go` — that JSON validation applies to the columns the
    **server** parses (`config`, `services`) and deliberately **not** to the client-owned
    blobs (`dashboards.config`, `saved_views.query_params`). "Making them consistent" would
    either start rejecting dashboard configs monitor-web has always sent, or stop catching
    a policy that can never route.
  - `db/clickhouse_test.go`'s `TestEventInsertColumnsMatchRowOrder` — that
    `eventInsertColumns` and `eventInsertRow` stay paired position-for-position. Every event
    column is a string, so a transposition inserts cleanly and shows up only as wrong data.
  - `query/issues_query_test.go` — the issue upsert's bound-parameter count and that
    `project` is in the INSERT but **not** in the `ON DUPLICATE KEY UPDATE` set list: a
    project is part of an issue's identity, so a fold that could rewrite it would be a fold
    across tenants.

---

## 5. How code is written here

- **Handlers are thin.** Two-layer flow: handler → `query.*` (auth data) or a
  service/subsystem (events/alerts). SSO orchestration lives in `sso/` (an adapter layer,
  not a per-handler service). Handlers assume middleware already authorized the caller.
- **Auth data uses the standard stack:** `db.Queryable` (satisfied by `*sql.DB` and
  `*sql.Tx`) as every query function's first arg, squirrel to build SQL, pagination
  constants `DEFAULT_LIMIT=50`/`MAX_LIMIT=500` in `db/sql.go`. Multi-step writes (register,
  admin bootstrap, SSO provision) run in a transaction so a failed identity insert can't
  orphan a `UNIQUE(email)` user row.
- **ClickHouse SQL** is built with squirrel then run via the native driver. **All
  caller-controlled `data.*` field names MUST be validated** against
  `structs.SafeIdentifierRegex` before interpolation (`services/analytics.go`,
  `services/query.go`, `alerts/evaluator.go`) — `JSONExtractString` takes the key as a SQL
  string literal, so an unvalidated key is an injection vector.
- **The identifier regex and the column allowlists live in `structs/columns.go` and
  nowhere else** — `SafeIdentifierRegex`, plus `QueryableColumns`, `GroupByColumns`,
  `FilterColumns` and `LabelColumns`. They were previously copy-pasted per package and the
  copies drifted: the two regexes disagreed about dots, so a nested key such as `user.id`
  was accepted by an alert rule and rejected by an analytics query. Add a new event column
  in `structs/columns.go`; never re-declare one of these in a consuming package.
- **Response envelope** (`responder/responder.go`):
  - Success: `{ "success": true, "message": <lowercased>, "data": <payload>, "pagination"?: {count,next,previous} }`
  - Error: `{ "success": false, "message": <lowercased>, "data": null, "error": <StatusText>, "error_message": <lowercased msg>, "error_code": <int> }` — via
    `responder.Error(w, status, msg)`, `responder.ErrorWithCause(w, status, msg, err)`, or
    `responder.ErrorWithCode(w, status, msg, code)` for an explicit application code.
  - Errors carry BOTH `message` (back-compat for the dashboard's fetch layer) and the
    standard `error`/`error_message`/`error_code` fields (added additively). `error_code`
    defaults to the HTTP status unless set explicitly.
  - Application `error_code`s the web clients route on: **`4003`** (forbidden — `RequireAdmin`/
    `RequireEditor` role denial → web redirects to `/unauthorized`), **`4004`** (`RejectPending`
    — pending account → web redirects to `/pending`), and CSRF **`4030`** (missing cookie) /
    **`4031`** (token mismatch).
  - Three endpoints bypass `responder`: `GET /health`
    (`{status,enqueued,dropped,pending,clickhouse_ok,mariadb_ok,last_flush_at,role,zone}`),
    `GET /ready` (`{status,clickhouse_ok,mariadb_ok,role[,failing]}`) and `POST /v1/events`
    (`{accepted:<int>}`). Ingest errors are plain text.
  - **`/health` is liveness, `/ready` is readiness — do not merge them.** `/health` always
    returns **200** with `status:"ok"`, even with both stores dead, because the container
    HEALTHCHECK points at it and restarting the process cannot repair ClickHouse; the
    dependency booleans there are diagnostics, not a verdict. `/ready` returns **503** and
    names the failing store in `failing`. **`/ready`'s verdict is role-aware:** ClickHouse
    counts as a dependency in every role *except* `app`, which holds no connection by
    design and would otherwise be permanently un-ready for doing exactly what it was
    configured to do. `clickhouse_ok` still reports `false` there — it is the true answer —
    which is why `role` is on this response too. The condition is written as "unless the
    role is exactly `app`" so it fails **closed**: any other value keeps ClickHouse
    required. Both ping with a 2s timeout
    (`routes.DEPENDENCY_PING_TIMEOUT`), concurrently, so a total outage costs one timeout.
    The first four `/health` keys are frozen — monitor-web's transport and the container
    healthcheck read that exact shape; extend it additively.
  - **Both probes are memoised for 5s (`routes.DEPENDENCY_PING_TTL`), and must stay that
    way.** `/health` and `/ready` sit on the root router ahead of every auth middleware and
    nothing in the stack rate-limits, so an unmemoised ping would take one connection from
    the ClickHouse pool (`MaxOpenConns=10`, shared with the batcher's write path) and one
    from MariaDB on *every* request — letting anyone who can reach the port starve
    ingestion by polling a liveness endpoint. The TTL sits well under the container
    HEALTHCHECK's 30s interval, so the memo never costs an orchestrator freshness. Before
    the first probe returns, both stores report **down**: a readiness probe answering 200
    because it has not looked yet is the one wrong answer it can give.
- **Subsystem `Init(ctx)`** is the idempotent startup entry point, called once from
  `main.go`. `apikeys.Init` now loads its cache from **MariaDB** (table created by
  `db.RunMigrations`, not by the package) and starts a goroutine that re-reads it every
  `apikeys.CACHE_REFRESH_INTERVAL` (30s) until the shutdown context is cancelled. That
  interval is the upper bound on how long a revoked key keeps working: `Delete` evicts only
  from the local map, so without the refresher a key revoked on another replica — or straight
  in the database — stayed valid until the container restarted. The refresher starts even when
  the initial load fails, since `main.go` treats `Init`'s error as a warning and serves on.

---

## 6. Domain & architecture

### Auth model (Monitor-owned)

Monitor owns identity end-to-end. Every external identity provider is a row in
`sso_providers` — Google, Okta, Entra, Forta and anything else are indistinguishable
as far as the code is concerned. There is **no provider-specific Go code**; adding an
IdP means filling in the form at `/admin/sso`, not shipping a build.

**Issue tracking (MariaDB `monitor`) — the issue row is relational, events stay columnar:**

| Table | Holds |
|---|---|
| `monitor.issues` | one row per (project, fingerprint) — identity, counts, first/last seen, and the mutable triage state (status, priority, title, assignee) |
| `monitor.issue_timeline` | append-only polymorphic feed: comments, status transitions, regressions, assignment, PR events |
| `monitor.issue_links` | linked GitHub PRs/issues/commits with a cached state, refreshed by webhook |
| `monitor.service_repos` | which source repository each reporting service is built from |

**Monitor watches many services across more than one GitHub org**, so an issue in
`scraper-service` and an issue in `website` do not belong to the same repo — and neither belongs
to `monitor-core`. `monitor.service_repos` is what makes an issue know where its code lives.

The mapping is **many-to-one and explicit**. The estate runs `auth-service-v1` beside
`auth-service-v2`, and `team-service` at v1/v2/v3 — all versions of one service, each reporting
under its own name, all built from one repo. Stripping a `-vN` suffix would be a guess that
silently mislabels anything off-convention, so nothing is derived from the service name. A service
with no row is simply unmapped: links still work, they just cannot be resolved from a bare `#123`.

**Tokens are per-owner**, because a fine-grained PAT is scoped to a single org. `github.TokenFor`
derives the env var name from the owner — `TeamTrailblaze` → `MON_GITHUB_TOKEN_TEAMTRAILBLAZE`,
`aidenappl` → `MON_GITHUB_TOKEN_AIDENAPPL` — and falls back to `MON_GITHUB_TOKEN_TRAILBLAZE`, so
adding an org is one Keyring secret with a derivable name and no code change. **Never fall back to
another org's token**: it 404s confusingly at best, and at worst sends a credential somewhere it
was not scoped for.

`github.ParseRef(input, fallbackOwner, fallbackRepo)` is what makes the mapping pay off — `#42` on
an `auth-service-v2` issue resolves against that service's repo. A shorthand with no mapping is
**refused, never guessed**: attaching a link to the wrong repository is worse than declining to
attach one. A bare number always means a pull request, since GitHub shares one numbering sequence
between PRs and issues and the number alone cannot disambiguate.

`issues.id` is **not** auto-increment — it is the deterministic UUIDv5 from
`issues.issueIDFor(fingerprint)`, so concurrent creators converge on one row. That namespace must
never change.

**An issue belongs to one project, and the project is inside the fingerprint STRING** — not just
in a key (migration 118). This is worth being precise about, because the obvious version of the
change protects nothing. `monitor.issues` has two unique indexes, `id` (the PK) and
`uq_issues_fingerprint`, and `id` is a *pure function* of the fingerprint. So without the project
in the string, two tenants reporting the same `service`/`name`/`path`/message compute one
fingerprint → one id → a **primary-key** collision; `ON DUPLICATE KEY UPDATE` fires on the first
unique index violated, so project B's occurrence folds into project A's row before
`UNIQUE (project, fingerprint)` is ever consulted. The composite key documents the invariant;
`issues.generateFingerprint` enforces it. **`env` is deliberately excluded** — prod and staging
errors of one shape stay one issue, as in Sentry.

**No re-keying migration was written**, by decision: every pre-118 issue keeps its old id, stops
receiving occurrences, and is dealt with by hand (43 rows on 2026-09-06 — 12 unresolved, 3
ignored, 28 resolved; re-count before planning that work). The 3 **ignored** rows are the trap:
their replacements are new ids defaulting to `unresolved`, so a deliberately silenced error
reappears in the backlog looking like a regression. Their rows in the no-TTL
`monitor.issue_occurrences_daily` become orphans once those issues are deleted — swept by
`migrations/manual/delete_orphaned_issue_rollups.sql`.

`status` is a single four-value enum (`unresolved`, `in_progress`, `resolved`, `ignored`), not two
axes. `unresolved` is both the default and the backlog. **Automated recurrence only ever
transitions out of `resolved`** — see `query.UpsertIssueOccurrence` — so an agent holding
`in_progress` is never clobbered.

The occurrence fold is a **single** `INSERT … ON DUPLICATE KEY UPDATE`. That is what fixes the
`occurrence_count` drift documented in `issues/AGENTS.md`, which came from a read-then-write
against a ClickHouse `ReplacingMergeTree`. **Do not turn it back into a read-then-write** — the
SET-clause ordering is load-bearing too, since MariaDB evaluates left to right and every clause
testing the previous `status` must precede `status`'s own reassignment. `query/issues_query_test.go`
pins both properties.

Neither `assignee_user_id` nor the child tables carry foreign keys. A default RESTRICT constraint
would make any referenced user undeletable — the exact defect keyring-api shipped, where every FK
into `access_logs` was RESTRICT and a secret that had ever been read could never be removed.

**Data model (MariaDB `monitor_auth`) — true account linking, one user ↔ many identities:**

| Table | Purpose |
|---|---|
| `users` | The account: `email` (`UNIQUE`), `email_verified`, `name`, `display_name`, `role` (`admin`\|`editor`\|`viewer`\|`pending`), `active`. |
| `identities` | One sign-in method per row, `UNIQUE(provider, provider_user_id)`. `provider="password"` stores a bcrypt hash in `password_hash`; SSO rows store the claim envelope in `identity_data`. **`(provider, provider_user_id)` is the ONLY identity key — never email.** `FK → users ON DELETE CASCADE`. |
| `refresh_tokens` | Rotating-refresh store. Only the SHA-256 `token_hash` (`BINARY(32)`) is persisted; tokens minted from one login share a `family_id`; rotation stamps `replaced_by`. |
| `sso_providers` | One configured IdP per row (see below). |
| `sso_sessions` | One row per SSO-backed user (`user_id` PK) caching the AES-256-GCM-encrypted IdP tokens so the checkpoint can re-introspect the upstream grant. Also carries `subject` and `sid` (migration 109) — the two things a back-channel logout token can name. **Neither can be backfilled**: `sid` exists only in the id_token of the login that created the row, so a session written without one is unreachable by a session-scoped logout for its whole life. |
| `settings` | Key/value app settings. |
| `api_keys` | Ingest/admin API keys (`scope` = `admin`\|`ingest`). Moved here from ClickHouse. `project_id` is **`NOT NULL`, `FK → projects`** (migration 117) — the tenant binding ingestion derives an event's project from, so there is no such thing as a key that authenticates into no tenant. `UNIQUE(project_id, name)`, so each project may own a key literally called `ingest`. **`uq_api_keys_key_hash` stays GLOBALLY unique** and must never be made composite — see below. |

**Sessions — Monitor-owned HS512 JWTs (`jwt/jwt.go`):** access token 15m, refresh token
7d. `Claims` carries `user_id` + `type` (`access`\|`refresh`); the parser is **pinned to
HS512** via `WithValidMethods` and the keyfunc re-checks the method (defeats
`alg:none`/confusion), and the issuer (`monitor`) is verified. Signed with
`MON_JWT_SIGNING_KEY`.

**Cookies (`routes/cookies.go`), `SameSite=Lax`, `Secure` unless `MON_COOKIE_INSECURE`:**

| Cookie | Flags | Scope | Contents |
|---|---|---|---|
| `mon-access-token` | HttpOnly | `/`, 15m | access JWT |
| `mon-refresh-token` | HttpOnly | `/auth/refresh`, 7d | refresh JWT |
| `mon-logged-in` | JS-readable | `/`, 7d | `"true"` (cheap client-side login gate) |
| `mon-csrf` | JS-readable, `SameSite=Strict` | `/` | double-submit CSRF token |

**Middleware:**

| Middleware | Applies to | Accepts |
|---|---|---|
| `IngestAuthMiddleware` | `POST /v1/events` | env master key (`MONITOR_API_KEY`) **or** a DB **ingest-scope** key. Admin keys → `401`: they are query credentials that live in dashboards and MCP configs, and reading events does not imply permission to forge them. |
| `QueryAuthMiddleware` | all other `/v1/*` | env master key **or** DB **admin-scope** key **or** a valid Monitor session (`mon-access-token`/Bearer, incl. SSO checkpoint). Ingest keys → `403`. Injects the project every read is scoped to: **credential-derived** for the two key branches, a validated **`?project=` selector** (defaulting to `MON_DEFAULT_PROJECT`) for a session — see §6 *The project asymmetry*. |
| `SessionMiddleware` (`Protected`) | `/auth/*`, `/admin/*` | a valid access JWT (Bearer or `mon-access-token`) → loads the active user into context. `RequireAdmin`/`RequireEditor`/`RejectPending` gate on `role`. |
| `CSRFMiddleware` (global) | all unsafe methods | `mon-csrf` cookie == `X-CSRF-Token` header (constant-time). Safe methods, Bearer clients, `X-Api-Key` clients, and `/auth/{login,register,refresh}` + SSO callbacks are exempt. |

Both `X-Api-Key` middlewares compare the env master key with `subtle.ConstantTimeCompare` via
the shared `middleware.matchesEnvMasterKey`, which also refuses an empty key on both sides —
`ConstantTimeCompare("", "")` returns `1`, so an unset `MONITOR_API_KEY` would otherwise let
every header-less request through.

**The env master key deliberately still grants both ingest and full query.** It is not demoted
to one scope, because every deployed service presents it and the fleet cannot be enumerated
from here — narrowing it blind stops all ingestion at once. The follow-up is to give services
real ingest-scope keys once API keys carry a project binding, then reduce this to a break-glass
credential.

**Actor identity — use `GetActor`, not `GetUserFromContext`, to record who did something.**

Every authenticated request carries a `*structs.Actor` in context, injected by the auth
middleware and read with `middleware.GetActor(ctx)`. There are three kinds:

| `Kind` | Set by | Carries | Label |
|---|---|---|---|
| `user` | `SessionMiddleware` (via `withUser`) | `UserID` | display name → name → email |
| `api_key` | `QueryAuthMiddleware`, admin-scope DB key | `APIKeyID` | the key's `Name` (e.g. `monitor-mcp`) |
| `system` | `QueryAuthMiddleware`, env master key; automated transitions | neither | `middleware.EnvMasterKeyLabel`, or a caller-supplied label |

`GetUserFromContext` is **not** a substitute: agents and CI authenticate by API key and have no
user at all, so a handler that attributes an action to the context user silently records nothing
for exactly the callers most likely to be acting. `structs.Actor` exists to make that
unrepresentable.

`Actor.Label` must be **denormalised onto any row that records it**, not looked up by id at read
time. An API key can be deleted and a user deactivated; the audit row still has to name who
acted. `apikeys.ValidateWithIdentity` (not `ValidateWithScope`) is what resolves a key to its
name on the auth path — which only works because `apikeys.Create` writes the `Name` into the
cache alongside the id and scope. It once wrote only the id and scope, so a key minted through
`POST /v1/api-keys` labelled every action it authenticated with an empty string until the next
cache refresh replaced the entry from the database.

**`apikeys.Create` must write every field the cache carries**, not just the ones the `INSERT`
needed — its entry serves live requests for up to `CACHE_REFRESH_INTERVAL` before the database
overwrites it. `Name` is where this was learned; `ProjectID`/`ProjectSlug` are the same trap
with a worse blast radius, because an omitted project files the key's first 30 seconds of
events under no tenant at all, and nothing downstream errors on a blank one.

**Endpoint surface** (registered in `router.go` + `RegisterSSORoutes.go`). Every route
below exists under the default `MON_ROLE=both`; which of them a single-plane process
registers is the table in §6 *Roles*, and a route this role cannot serve is **not
registered at all**, so it answers `404` rather than failing on a nil dependency:

```
POST   /auth/login                          native email/password → session      [public, CSRF-exempt]
POST   /auth/register                       self-provision (role=pending)         [public, only if MON_ALLOW_REGISTRATION=true]
POST   /auth/refresh                        rotating refresh + reuse detection    [public, CSRF-exempt, reads mon-refresh-token]
POST   /auth/logout                         revoke refresh family + clear cookies [Protected]
GET    /auth/self                           current user + its identities         [Protected]
PUT    /auth/self                           update name / set-or-change password  [Protected]
GET    /auth/self/identities                list linked sign-in methods           [Protected]
POST   /auth/self/identities/{slug}         begin an authenticated LINK flow      [Protected] → {authorize_url}
DELETE /auth/self/identities/{slug}         unlink (refuses the last identity)    [Protected]
GET    /auth/sso/config                     enabled providers for the login page  [public] → [{slug,button_label,login_url}]
GET    /auth/sso/{slug}/login               302 to the IdP                        [public]
GET    /auth/sso/{slug}/callback            IdP redirect target → session         [public, CSRF-exempt]
POST   /auth/sso/forta/backchannel-logout   OIDC Back-Channel Logout 1.0 §2.5     [public — signature IS the auth]
GET    /admin/sso-providers                 list providers (secrets never returned)  [Protected + RequireAdmin]
POST   /admin/sso-providers                 create a provider                     [Protected + RequireAdmin]
PUT    /admin/sso-providers/{slug}          update a provider                     [Protected + RequireAdmin]
DELETE /admin/sso-providers/{slug}          delete a provider                     [Protected + RequireAdmin]
POST   /admin/zones                         record a zone (both URLs required)    [Protected + RequireAdmin, control plane only]
PUT    /admin/zones/{id}                    display_name / ingest_url / query_url [Protected + RequireAdmin, control plane only] — slug & status are REFUSED
POST   /admin/zones/{id}/retire             soft-delete; 409 with live projects   [Protected + RequireAdmin, control plane only]
POST   /admin/zones/{id}/probe              probe query_url now + persist verdict [Protected + RequireAdmin, control plane only]
POST   /admin/zones/{id}/projects           create a project inside a zone        [Protected + RequireAdmin, control plane only]
PUT    /admin/projects/{id}                 display_name                          [Protected + RequireAdmin, control plane only] — slug, zone_id & status are REFUSED
POST   /admin/projects/{id}/retire          soft-delete                           [Protected + RequireAdmin, control plane only]
POST   /webhooks/github                     GitHub pull_request deliveries        [public — HMAC signature IS the auth]
GET    /v1/issues?status=&service=&assignee=&has_pr=&q=&from=&to=&sort=&order=&history=  filtered list  [QueryAuthMiddleware]
GET    /v1/issues/{id}                      verbose detail (+links, assignee, repo, history)  [QueryAuthMiddleware]
PUT    /v1/issues/{id}                      status / priority / title / assignee  [QueryAuthMiddleware]
GET    /v1/issues/{id}/timeline             the activity feed, oldest first       [QueryAuthMiddleware]
GET    /v1/issues/{id}/history              per-day occurrence sparkline          [QueryAuthMiddleware]
POST   /v1/issues/{id}/comments             add a note (optional dedupe_key)      [QueryAuthMiddleware]
PATCH  /v1/issues/{id}/comments/{commentID} edit a comment body                   [QueryAuthMiddleware]
DELETE /v1/issues/{id}/comments/{commentID} soft-delete a comment                 [QueryAuthMiddleware]
GET    /v1/issues/{id}/links                list linked PRs/issues/commits        [QueryAuthMiddleware]
POST   /v1/issues/{id}/links                link a url, owner/repo#n, or #n       [QueryAuthMiddleware]
DELETE /v1/issues/{id}/links/{linkID}       remove a link                         [QueryAuthMiddleware]
GET    /v1/service-repos                    every service → repository mapping    [QueryAuthMiddleware]
GET    /v1/service-repos/{service}          one mapping (404 when unmapped)       [QueryAuthMiddleware]
PUT    /v1/service-repos/{service}          create/replace a mapping              [QueryAuthMiddleware]
DELETE /v1/service-repos/{service}          remove a mapping                      [QueryAuthMiddleware]
```

Native flows: `HandleLogin` returns a neutral 401 on every failure (no email
enumeration; missing accounts still pay a dummy bcrypt cost). `HandleRefresh` implements
rotating refresh with **reuse detection** — presenting a spent (rotated) or revoked
token revokes the entire `family_id` and clears cookies (OAuth 2.0 Security BCP §4.14).
`HandleLogout` revokes the family (or all of the user's tokens when the path-scoped
refresh cookie isn't sent). `HandleUpdateSelf` can create a `password` identity for an
SSO-only account.

**Back-channel logout (`POST /auth/sso/forta/backchannel-logout`).** Forta pushes a signed
`logout_token` the instant a grant is revoked, closing the checkpoint's 5-minute window.

⚠️ **It is public on purpose and must never move under `SessionMiddleware`.** The caller is
Forta, which holds no Monitor session and no cookie; its only authentication is the signature on
the token, verified against Forta's JWKS and against this `client_id` inside go-forta's handler.
Session-gating it would reject every genuine notification — invisibly, and indistinguishably from
"nothing has been revoked yet". `routes/backchannel_route_test.go` pins both that failure mode and
the not-mounted-at-all one (a 404 that Forta retries to exhaustion while nothing here logs a thing).

⚠️ **It is exempt from `CSRFMiddleware`, and must stay exempt.** The notification is a
server-to-server POST with no cookie, no Bearer token and no `X-Api-Key`, so it falls through
every other exemption and is refused **403 `missing csrf cookie` (4030)**. That shipped on
2026-08-08 and made the feature silently dead: Forta retried six times, marked each delivery
exhausted, and revocation stayed at poll speed while the endpoint looked like a broken receiver.
Exempting is correct rather than a concession — CSRF defends ambient cookie authority, and this
endpoint has none; its only authentication is the signature on the logout token, which is
strictly stronger. See `isBackchannelLogoutPath`.

⚠️ **The routing test alone is not sufficient**, and this is the reason the bug shipped.
`routes/backchannel_route_test.go` builds a bare `mux.Router`, so it cannot see the global
middleware stack — every one of its assertions passed while production returned 403.
`middleware/csrf_test.go` covers that half. **Changing this endpoint's path means changing both**,
or one of them silently tests a URL that no longer exists.

⚠️ **It does NOT replace the checkpoint.** Back-channel logout is best-effort *by specification* —
notifications can be lost and retries can be exhausted — so the 5-minute poll stays the guarantee
and this is the fast path. Do not relax `CheckpointInterval` because this exists.

**GitHub webhook (`POST /webhooks/github`) — the same shape, and the precedent above is why it
works.** Deliveries are server-to-server POSTs with no cookie, no Bearer token and no `X-Api-Key`,
so they land in exactly the trap that made back-channel logout silently dead. It is therefore
mounted on the **root router** (never the `v1` subrouter, so `QueryAuthMiddleware` cannot run on
it) and listed in `csrfExemptPaths`. `middleware/csrf_test.go` pins both halves.

⚠️ **The HMAC-SHA256 signature is the only authentication.** `github.VerifySignature` compares
with `hmac.Equal` — constant-time, because a byte-wise early return leaks through response timing
how much of a forged signature was right, which is enough to recover a valid one. An **unset**
`MON_GITHUB_WEBHOOK_SECRET_TRAILBLAZE` **rejects** every delivery rather than accepting it: failing
open would leave an unauthenticated write into the issue timeline.

⚠️ **A webhook never changes issue status.** A merged PR appends a `pr_merged` timeline entry and
refreshes the link's cached state — nothing more. GitHub itself only honours closing keywords on
the default branch, and silently resolving someone's issue is a surprising thing for a webhook to
do; resolution stays a deliberate human or agent action. `TestWebhookNeverEmitsStatusChange` pins
this.

Unknown event types return **200**, not 4xx: GitHub retries on failure and eventually disables a
webhook that keeps erroring, so acknowledging what we ignore is what keeps the ones we care about
flowing. Timeline writes are keyed on `gh:{owner}/{repo}#{n}:{type}`, so GitHub's at-least-once
redelivery collapses to one entry.

**The public SSO config contract (`GET /auth/sso/config`) — shared shape:**

One response shape across monitor-core, lattice-api and openbucket-api, so a login
page written once renders against any of them. Modelled on Zulip's
`server_settings`, which solves the same problem: an unauthenticated page that
must know which providers exist before anyone has logged in.

```json
{ "providers": [ { "name": "forta", "display_name": "Forta",
    "display_icon": "/auth/sso/icon/forta", "button_color": null,
    "button_text_color": null, "login_url": "/auth/sso/forta/login",
    "sort_order": 0 } ] }
```

⚠️ **This endpoint is UNAUTHENTICATED.** It carries display data only — never an
issuer URL, `client_id`, or scope list. Adding a field here is publishing it.

Three properties are security decisions rather than style:

| Property | Why |
|---|---|
| `login_url` is **computed from the slug**, never stored | a stored URL is an admin-controlled value an unauthenticated page turns into a link the user is told to click — an open redirect with your domain on it, and a phishing lure that survives review because the page is genuine |
| `display_icon` is **never the admin's third-party URL** | hot-linking leaks every unauthenticated visitor's IP, UA and Referer to that party, makes the page depend on their uptime, and lets them swap the image after review. The bytes are fetched once at save time and served from this origin |
| Colours are **re-validated on render**, not only on write | validating only on write trusts that every row ever written passed that check — false for a row inserted before it existed, by a migration, or by hand. These end up inside CSS on an unauthenticated page |

⚠️ **`display_icon: null` is CONTRACTUAL, not an error.** It means *render a plain
text button*. Every client must handle it: it is the state before an icon is
configured, and the state a provider returns to when a fetch fails. A cached asset
resolves to `/auth/sso/icon/{slug}`; a bundled icon resolves to `bundled:<slug>`,
a short opaque identifier the frontend maps to an asset it ships — deliberately
not a path, which would be an admin-controlled string the page turns into a URL.

**`GET /auth/sso/icon/{slug}`** serves the cached bytes with `X-Content-Type-Options:
nosniff`, `Content-Disposition: inline`, and a 24h `Cache-Control` (not
`immutable` — the URL carries no content hash, so the bytes genuinely can change).
The fetch, SSRF defence and SVG rejection all live in `go-forta/sso`'s
`FetchIcon`; this repo only stores and serves.

**SSO subsystem (`sso/`) — thin wiring onto `go-forta/sso`:**

⚠️ **THE PROTOCOL NO LONGER LIVES HERE.** Discovery, PKCE, state, nonce, id_token
verification, UserInfo, introspection and the revocation checkpoint are all in
`github.com/aidenappl/go-forta/sso`. This code was where they were written, and it was lifted
out because `lattice-api` and `openbucket-api` had forked thinner copies that drifted into real
vulnerabilities. `sso/adapter.go`, `oidc.go`, `oauth2.go`, `introspect.go` and `state.go` were
**deleted** — do not re-add a local copy.

What remains is everything the library refuses to know:

| File | Role |
|------|------|
| `sso/config.go` | Maps an `sso_providers` row → `ssolib.Provider`; resolves the client secret (Keyring ref → env of the same name → AES-GCM column). |
| `sso/statestore.go` | `ssolib.StateStore` over the settings KV. ⚠️ Its `ConsumeState` atomicity is the replay defence — the DELETE decides the winner, not the read. |
| `sso/sessionstore.go` | `ssolib.SessionStore` over `sso_sessions`, with AES-256-GCM at rest. Returns `(nil, nil)` for a native login, which is what stops the checkpoint denying every password user. Also implements `ssolib.BackchannelLogoutTarget` (`DeleteSessionsBySID`/`BySubject`) — the same store the checkpoint uses, so push and poll end sessions through one code path. |
| `sso/backchannel.go` | Builds the OIDC Back-Channel Logout 1.0 receiver. Mounted at `POST /auth/sso/forta/backchannel-logout`. |
| `sso/resolve.go` | `ssolib.UserResolver` — the link/provision decision matrix, plus `LinkIdentity`. **The most security-sensitive file here.** |
| `sso/checkpoint.go` | Installs the library `Checkpointer` into the session middleware. |

- An `sso_providers` row still fully describes an IdP; `kind` selects the library's OIDC or
  OAuth2 adapter. A login normalizes to `ssolib.Identity{Provider, Subject, Email,
  EmailVerified, …}`. **Subject (the OIDC `sub`), paired with Provider, is the identity —
  email is only a linking hint.**
- **What the migration fixed for free:** the old local `oauth2` adapter accepted a PKCE
  verifier and then discarded it, hand-rolling three token requests in sequence. So PKCE was
  configured, appeared in logs, and defended nothing — and every login performed a failing
  request first. The library sends the verifier on one standard request, asserted end-to-end by
  a test against a fake IdP that enforces PKCE.
- ⚠️ **A known gap, recorded rather than hidden:** `middleware.SSOCheckpoint` is a `bool` hook,
  so `CheckpointUnavailable` — "the IdP could not be reached and the 30-minute grace window has
  elapsed" — is mapped to **deny**, surfacing as a 401 where it should be a 503 with
  `Retry-After`. Denying is the right call of the two available (allowing would be the
  unbounded fail-open the library exists to prevent), but the status is wrong. Widening the
  hook to carry a status is the fix, and it belongs with the middleware.
- **Login flow:** `HandleSSOLogin` mints a single-use server-side `{state, nonce, PKCE
  verifier}` record (10-min TTL, carries a sanitized same-origin `return_url`) and 302s to
  the authorize URL. `HandleSSOCallback` single-use-consumes the state (CSRF/replay gate,
  bound to the slug), exchanges the code (OIDC verifies id_token signature/iss/aud/exp
  **and nonce**), resolves the identity, caches the encrypted IdP tokens, then issues
  Monitor's own session and redirects to the sanitized `return_url`.
- **Identity resolution (`sso/resolve.go`) — the nOAuth / pre-account-takeover defense:**
  1. **Known identity** — `(provider, subject)` exists → log in as its user.
  2. **Safe link (link-on-login)** — only if the provider has `allow_auto_link`, the IdP
     asserts a **verified** email, **and** it matches an existing user whose **own** email
     is verified. Both-sides-verified is mandatory; linking onto an unverified account is
     refused.
  3. **Provision** — else, if `auto_provision`, create a fresh `role=pending` user + its
     identity; otherwise reject.
- **`trust_email_verified`** (per-provider, default off) treats a provider's emails as
  verified when it returns no `email_verified` claim. Trust is explicit per-provider
  config, never a hardcoded slug in adapter code.
- **Authenticated LINK flow:** `POST /auth/self/identities/{slug}` mints a link-state
  carrying the current user id; the callback attaches the returned identity to that user,
  refusing to steal one already owned by a different account, and redirects to
  `/settings/security`.
- **Revocation checkpoint (`sso/checkpoint.go`, wired by `sso.Install()`):** for
  SSO-backed sessions, `SessionMiddleware` periodically (5-min TTL) re-introspects the
  cached IdP token; `active=false` kills the local session, network/DB errors fail **open**.
- **Client secrets** are never stored/echoed in the clear: either a Keyring reference
  (`client_secret_ref`, resolved Keyring→env at login) or an AES-256-GCM value
  (`client_secret_enc`). The admin API exposes only a `has_secret` boolean.
- **SSRF guard:** every provider URL in the admin create/update body is checked by
  `tools.ValidateExternalURL` before it is saved.

### Roles — the two planes (`MON_ROLE`)

`monitor-core` is one binary doing two unrelated jobs, and `MON_ROLE` makes which one
explicit. It is **not** two binaries on purpose: two `main`s drift, and the way they drift
is that a route added to one and forgotten in the other stays invisible until somebody
deploys the split topology and finds a 404 in production. One router, gated at the line of
registration, cannot drift that way.

| | `app` (control plane) | `zone` (data plane) | `both` (default, deployed) |
|---|---|---|---|
| ClickHouse connection + migrations + boot probe | **no** (`db.Conn` stays nil) | yes | yes |
| MariaDB + its migrations | yes | yes | yes |
| `bootstrap.EnsureAdminUser` | yes | **no** | yes |
| `bootstrap.EnsureZoneAndProject` | yes | yes | yes |
| `sso.Install()` (revocation checkpoint) | yes | **no** | yes |
| `apikeys.Init` (key cache + refresher) | yes | yes | yes |
| `registry.Init` (zone/project cache) | yes | yes | yes |
| `alerts`/`issues` `Init` | **no** | yes | yes |
| Event hub, queue, batcher, alert hub, alert evaluator | **no** | yes | yes |
| `/auth/*`, `/admin/sso-providers` routes | yes | **no** | yes |
| `/admin/zones*`, `/admin/projects*` (registry WRITES) | yes | **no** | yes |
| Ingest, query, analytics, dashboards, views, alerts, issues, `/webhooks/github` routes | **no** | yes | yes |
| `/health`, `/ready`, `/v1/zones*`, `/v1/service-repos*`, `/v1/api-keys*` | yes | yes | yes |

- **The default is `both`, and it must stay `both`.** An unset `MON_ROLE` on the live
  install has to keep the process doing exactly what it does today; a default that changed
  behaviour on deploy would make the first rollout of the split an outage.
- **An unrecognised value refuses to boot** (`env.RequireValidRole`, called from `main`
  straight after `env.Load`). It does *not* fall back. A typo that degraded to the default
  would run the **control plane** on a machine meant to be a zone — seeding a zone row,
  minting sessions, installing SSO — while the operator reads their config back and sees a
  data plane. That is a second control plane, not a degraded one.
- **Registration is the gate, not a check inside the handler.** `db.Conn` is an interface,
  so a nil one does not return an error — it panics on the method call, and `net/http`
  answers a panicking handler by closing the connection with no response at all. A route
  that cannot work in this role is therefore not registered, and answers `404`.
- **`/ready` is role-aware** so an `app` process is not permanently 503: ClickHouse counts
  as a readiness dependency in every role except `app`. Written as "unless the role is
  exactly `app`", so it fails closed.
- **The role is visible in two places on purpose**: one `log.Printf` at the very top of
  boot (before the first fatal, so a process that dies still says what it was trying to
  be) and a `role` key on `GET /health`. The risk this design carries is that `both`
  becomes an unexamined permanent default; neither of those fixes that, but they make it a
  state someone can see.
- **Asymmetry worth knowing:** a zone still *verifies* access tokens — `QueryAuthMiddleware`
  falls back to `SessionMiddleware` on `/v1` — it just does not *issue* them. Issuing and
  verifying are different jobs; the control plane issues, both planes verify.
- **The three shared `/v1` surfaces** (`zones`, `service-repos`, `api-keys`) are pure
  MariaDB and stay registered in **both** roles. That is a deferral, not a verdict: which
  plane *owns* them is the config-pull question, and inventing an answer with no second
  zone to test it against would be worse than leaving today's behaviour alone.
- **Deliberately NOT built in this phase** — do not add them speculatively: a config-pull
  protocol, a zone agent, per-zone credentials, zone fan-out, cross-zone queries, per-user
  project memberships, or a second zone. Consequently a `zone`-only process today expects
  its zone/project rows and its `users` rows to already be present in its MariaDB; nothing
  yet replicates them from the control plane.
- **`backfill-issues` and `backfill-config` need the data plane** and refuse to run under
  `MON_ROLE=app`, naming the variable — both read legacy ClickHouse tables.
- **`alerts.Init` is still data-plane, and it now seeds MariaDB.** The four default
  notification policies are a MariaDB write made from the data-plane block rather than from
  `bootstrap/`, so that the set of processes creating those rows is exactly what it was
  before the tables moved. Moving it to the control plane would be a silent behaviour
  change dressed as tidying.
- Pinned by `router_test.go` (the full `both` inventory, the app/zone splits, and that
  `both` is exactly the union of the two) and `env/role_test.go` (parsing, the fail-fast
  refusal, the capability table).

### Tenancy — zones and projects

The registry lives in **`monitor_auth`** (`zones`, `projects`;
`db/migrations/116_create_registry.sql`), not the `monitor` schema. That widens
`monitor_auth` from "identity" to "identity **and** tenancy" — a deliberate change to the
charter 110 wrote down, made because `api_keys` carries the project binding and a
same-schema foreign key is worth more than the tidiness of separating them.

- A **zone** is a whole ClickHouse instance. The database inside it is **always** the one
  named by `CLICKHOUSE_DATABASE`. **No zone or project identifier ever enters SQL as an
  identifier**, which is what keeps the whole dimension off the injection surface
  `structs/columns.go` polices.
- A **project** is a tenant inside one zone, unique **within** that zone rather than
  globally — two zones may each own a `payments`.
- **The project is derived server-side** from the authenticating `api_keys` row and
  **overwritten on ingest**, exactly like `Event.IssueID`. It is never trusted from the
  client: a caller that could name its own project could file events under someone else's.
- **Slugs are immutable and never reusable.** Rows are never `DELETE`d — retire with
  `status='deleted'` and the `UNIQUE` key keeps the name spent forever. This is not
  fastidiousness: events carry a 30-day TTL and `monitor.issue_occurrences_daily` has
  **no** TTL, so a recycled slug silently reattaches a month of one owner's events plus a
  permanent rollup to another, with every reference still syntactically valid. There is
  deliberately no `DeleteZone`/`DeleteProject`, and no `Slug` field on either
  `Update…Request` — the absence *is* the enforcement.
- **Slug rule** (GCP project-id in spirit): lowercase letters, digits and hyphens, starts
  with a letter, does not end with a hyphen, 3-30 chars. Enforced twice — `tools.ValidateSlug`
  in Go, and the `ck_*_slug_format` CHECK constraints in the DDL as the backstop for
  anything written outside the query layer. Keep the two in step.
- **Reserved slugs** (`tools.reservedSlugs`) are Go's alone and are *not* in the DDL: the
  frontend resolves static route segments before dynamic ones, so a project named
  `settings` would be shadowed by the app's own page and unreachable with nothing logged
  anywhere. The list changes when the frontend's routes change, which is not a schema
  migration.
- **Phase 1 is single-zone.** There is no fan-out, no zone routing, no config pull and no
  role switch — `bootstrap.EnsureZoneAndProject` guarantees exactly one of each so
  everything downstream can assume a project exists.

- **On the ClickHouse side the dimension is one column**, `events.project`, added by
  `migrations/006_events_project.sql` as a plain `LowCardinality(String)` **not** in the
  sorting key and with **no skip index**. Both omissions are deliberate: the table is
  roughly 209 granules end to end, so key pruning has nothing to prune and a bloom filter
  over a handful of distinct values matches every block and skips none — and changing a
  sorting key means rebuilding a table, which a runner that replays every file on every
  boot cannot do. `db.eventInsertColumns` / `db.eventInsertRow` carry it on the write
  path, paired position-for-position and pinned by a test, because every event column is
  a string and a transposition would insert cleanly.
- **A row written before 006 reads back as the empty string.** Adding a column is
  metadata-only — ClickHouse does not rewrite existing parts, and 006 deliberately does
  **not** emit `MATERIALIZE COLUMN` because that is a mutation and would be re-queued on
  every boot. Empty therefore means "pre-tenancy", not "no project", and the read path
  must treat it as `MON_DEFAULT_PROJECT`. The one-off fill is
  `migrations/manual/backfill_events_project.sql` — run by hand, asynchronous, watched
  through `system.mutations`. The condition expires on its own: the 30-day events TTL
  ages out the last unstamped row.

**The binding lives on `api_keys.project_id`** (`NOT NULL`, `FK → projects`, migration 117).
`apikeys.refreshCache` reads keys and projects in **one joined query** every 30s, so
`Identity` carries `ProjectID` *and* `ProjectSlug` — the slug for ClickHouse's
`LowCardinality(String)` project column, the id for anything relational. Caching a slug is
safe precisely because slugs are immutable and never reused; caching a `display_name` would
not be. `apikeys.Create` takes a project slug, resolves it through `env.ZoneSlug` +
`query.GetProjectBySlug` (never by slug alone — a project slug is unique only *within* a
zone), and writes **every** cached field, project included: an entry serves requests for up
to `CACHE_REFRESH_INTERVAL` before the database replaces it. An omitted slug falls back to
`MON_DEFAULT_PROJECT`, which is what keeps the admin UI and `monitor-mcp` — neither of which
sends a project yet — working unchanged.

**`IngestAuthMiddleware` resolves the full `Identity`, not just the scope**, because the row
that authorises the write is the row that names the tenant — one lookup, so "may you write?"
and "as whom?" can never be answered against different rows. It injects the slug with
`middleware.WithProject` and the ingest handler reads it back with `middleware.GetProject`:
deliberately the same context idiom as `WithActor`/`GetActor` in `session.go`, because a
package with two ways to read an auth result out of a request is a package where the next
handler reaches for the wrong one. The env master key has no `api_keys` row, so that branch
injects `MON_DEFAULT_PROJECT` instead of nothing — Mimir's `anonymous` and Loki's `fake`, for
the same reason: a null tenant is unqueryable by every tenant-scoped filter, invisible right
up until a second project exists and it quietly becomes someone else's data.

> **`uq_api_keys_key_hash` must stay a single-column, instance-wide UNIQUE key.** Everything
> else in this phase went per-project; this one must not, and pattern-matching it into
> `(project_id, key_hash)` is the tempting mistake. Authentication runs *from* a raw
> `X-Api-Key` string *to* a project — the tenant is the answer, not an input — so a presented
> key has to resolve to exactly one row across the whole instance. Composite it and two
> projects can hold the same hash, the hash-only lookup matches both, and whichever the cache
> wrote last decides whose project every event from that credential is stamped with:
> cross-tenant attribution settled by map iteration order, with no error anywhere.
> `keyring-api` shipped the same class of bug (no UNIQUE on `secret_key`, oldest duplicate
> silently wins injection) and it was invisible until someone reasoned backwards from wrong
> data.

#### The project asymmetry — three credentials, three resolutions, two boundaries

This is the single most misread thing in the tenancy design, and it looks like a bug until
the missing premise is stated. **Three credentials reach Monitor and each resolves a project
a different way. Two of those resolutions are tenancy boundaries. One is not.**

| # | Credential | Project comes from | Boundary? |
|---|---|---|---|
| 1 | **Ingest** — `X-Api-Key` on `POST /v1/events` | the `api_keys` row, **overwriting** whatever the client sent | ✅ unforgeable |
| 2 | **API-key reads** — an admin key on `/v1/*` | that key's own row; an admin key reads **only** its own project | ✅ credential-derived |
| 3 | **Session reads** — a logged-in human in `monitor-web` | the `?project=` **selector on the request**, validated against the registry | ❌ **not a boundary** |

(3) reads as a flat contradiction of (1) — the whole ingest path exists to stop a caller
naming its own project — so the premise has to be said out loud: **Monitor has no per-user
project membership table.** There is no row on this install that could say "this user may see
`payments` but not `billing`"; every account belongs to the operator or a colleague of the
operator. A check that consults nothing is not a boundary, it is a decoration, and the danger
of shipping one is that the next reader trusts it. **Roles still gate VERBS**
(`admin`/`editor`/`viewer`/`pending`) — that is the authorisation that genuinely exists here.
Sentry and Grafana both work exactly this way: the org/tenant selector is a *chooser*, and
membership is the thing that constrains it.

**The change that turns (3) into a boundary is a per-user membership table, and
`middleware.withSessionProject` is where it lands.** Until then, validating the selection
against the registry is the whole of what can honestly be enforced.

Three properties of the selector, each load-bearing:

- **It is a QUERY PARAMETER, not a header.** Monitor has two SSE surfaces (the live event
  tail and the alert feed) and the browser's `EventSource` **cannot set a custom header**. A
  header-based selector would work for every axios call and silently leave both streams
  tailing whichever project the fallback chose — forever, with no error on either side, and
  nothing in the UI to distinguish a quiet project from a misdirected subscription. One
  mechanism that works for both beats two that agree only while someone keeps them in step.
  (The **zone**, by contrast, is a *path* segment in `monitor-web` — `/{zone}/errors?project=atlas`.
  A zone selects which backend answers, so it must survive a bookmark and drive the proxy's
  upstream choice; a project is a filter inside one backend and may reasonably become
  multi-valued. That is Sentry's shape: org slug in the path, project as a repeatable query
  param.)
- **An unknown or retired slug is REFUSED — 400, naming the slug — never silently defaulted.**
  A fallback would answer with the default project's events under the label the user asked
  for: a chart that is wrong while looking right, which is the worst of the three available
  outcomes (right answer, honest error, silent wrong answer). It is a **400**, not a 403,
  because there is no membership to fail — 403 would assert the boundary this section says
  plainly is not there, and would send `monitor-web`'s `error_code` 4003 handler to
  `/unauthorized` for what is really a stale bookmark.
- **Two selections are refused, not resolved by taking the first.** A repeatable `project`
  param is the shape this grows into; until it does, honouring one of two named projects is
  the same silent wrong answer the fallback is refused for.

**Branches 1 and 2 ignore the selector entirely.** They are credential-derived and stay that
way — a request-supplied slug must never move a real boundary. `?project=` still reaches them
as an ordinary filter column, which is ANDed onto the mandatory predicate and can therefore
only narrow a result, never widen one.

**Validation is cached, not queried per request.** `registry/registry.go` is `apikeys`'
twin — an in-memory map of this zone's **active** projects, rebuilt on the same 30s ticker,
loaded at `Init`, keeping the previous map when a refresh fails. Without it, every dashboard
poll, SSE reconnect and analytics panel would cost a MariaDB round trip to validate a string.
Four things about it are deliberate:

- **Active-only, by construction.** `query.GetProjectBySlug` returns retired rows on purpose
  (a spent slug must never read as free), but "does this row exist" and "may a session select
  it" are different questions — conflating them would make retiring a project have no
  observable effect at all.
- **A cold or failed cache fails CLOSED.** An unloaded registry answers `false`, so an
  explicit selection is refused while the default project keeps working. The alternative
  would trust a client-supplied slug during exactly the window in which the server knows
  least about the registry.
- **A failed *refresh* keeps the previous map** and logs. A MariaDB blip must not make every
  project unselectable at once and blank the dashboard for everyone holding one in their URL.
- **The load pages at `db.MAX_LIMIT`.** `query.ListProjects` clamps an unset `Limit` to
  `db.DEFAULT_LIMIT` (50), which is right for a UI page and a silent truncation for a cache:
  project 51 would simply never be selectable, with no error and nothing pointing at
  pagination. `MAX_PROJECT_PAGES` bounds the other side — a query that stopped advancing
  would hold the refresher goroutine, so 20 full pages is reported as an error rather than
  absorbed.

**`GET /v1/zones` and `GET /v1/zones/{zone}/projects`** are what a switcher populates from —
reads for any authenticated session. ⚠️ **Clients must not append `?project=` to either.** Both run through
`QueryAuthMiddleware`, so a stale selection would refuse the exact request needed to discover
a valid one — answering the registry without a selection is what keeps a bad selection
recoverable. See `routes/AGENTS.md` for the rest. The projects listing also
carries **`default_project_slug`**, naming which project an unset `?project` resolves to, so
the switcher renders that tenant once rather than twice.

**The registry WRITES are a separate surface: `/admin/zones*` and `/admin/projects*`,
control plane only, `RequireAdmin` on every route** (the table in §5 lists them). Four rules
govern that surface, and each of them is a failure that has no runtime symptom:

- **A zone row RECORDS infrastructure; it does not create any.** The stack, its ClickHouse,
  its MariaDB, the DNS record and the certificate are provisioned by hand first. Nothing
  reconciles a row against reality, so a row whose `query_url` points at the wrong box is
  syntactically perfect and renders another tenant's data under this zone's name — which is
  what the probe below exists to catch.
- **No hard delete, ever.** Retiring sets `status='deleted'` and keeps the row forever, so
  the `UNIQUE` key on slug makes reuse structurally impossible: events outlive a row by up
  to 30 days and the occurrence rollup outlives it permanently, so a recycled slug
  reattaches one tenant's history to another with every reference still valid. There is no
  `DELETE` verb on the surface and `query/registry_query_test.go` walks the repository to
  keep it that way. `POST /admin/zones/{id}/retire` refuses with **409** while the zone
  still owns active projects — the guard is a `NOT EXISTS` inside `RetireZone`'s own
  `UPDATE`, not a count in the caller, so there is no window to lose a race in.
- **Slugs are immutable at the API.** A `slug` in an update body is answered **400**, never
  dropped: a rename that returned `200` and changed nothing would be invisible on this side
  and permanent on the other. `status` (and `zone_id`, on a project) are refused for the
  same reason — retirement has a guard, and a second way to reach it is a bypass.
- **Reserved slugs come from `tools.ValidateSlug`.** The list guards monitor-web's *static*
  route table; a project called `settings` is shadowed by the app's own page and is
  permanently unreachable with nothing logged.
- **A probe verdict belongs to the URL it measured, and dies with it.** `PUT
  /admin/zones/{id}` that changes `query_url` clears `reachability`,
  `reachability_detail`, `reported_zone` and `last_probe_at` back to `unknown` — in
  `query.UpdateZone`, via `invalidateZoneProbeOnRepoint`, so it holds for *every* caller and
  not just the one client that remembers to re-probe. Leaving them standing is the same
  mislabelling failure as a wrong `query_url`, only with a green tick on top: the row keeps
  a `healthy` from before the edit, a `last_probe_at` that predates it, and a `reported_zone`
  naming the box it *used* to reach, while every read now goes somewhere else. A stale
  `healthy` is strictly worse than never probing, because `unknown` admits what it does not
  know. It is a **separate statement guarded on `query_url <> ?`**, run *before* the URL
  moves: the guard means an unchanged URL (the admin form resubmits every field) keeps its
  verdict rather than resetting on every save — a surface that cries `unknown` after every
  edit trains operators to ignore the one time it matters — and the ordering fails closed,
  since a crash between the two leaves the *old* URL with no verdict, which is true and
  harmless. It is not folded into the main `UPDATE` as a `CASE` because MariaDB evaluates
  `SET` assignments left to right, which would make correctness depend on the clause staying
  textually above `Set("query_url", …)`. `query/registry_query_test.go` pins both halves.

**`POST /admin/zones/{id}/probe`** answers "is the box at the end of this row's `query_url`
actually this zone?" It GETs `{query_url}/health` and `/ready` inside a **3s total budget**
(`probe.ZONE_PROBE_TIMEOUT` — one wedged zone must not hang the page that warns about wedged
zones), re-runs `tools.ValidateExternalURL` at probe time (DNS can be re-pointed under a
stored value), refuses redirects, and stores one of `structs.ZoneReachability` via
`query.RecordZoneProbe`. ⚠️ **A `200`-only check would be worthless here**: every zone runs
the same binary and answers `200`, so a `query_url` aimed at another zone would pass and
mislabel every read. The probe therefore compares the far end's reported **`zone` and
`role`** (both on `GET /health`) against what the registry expected — `role` FIRST, because
a control plane carries the same `MON_ZONE_SLUG` default and serves no events, and
`MON_PUBLIC_URL` makes it the likeliest wrong URL to be typed. `unverified` (answered but
would not say who it is) is deliberately not `healthy`. **An unreachable zone is a `200`
from this route** — the probe succeeded, the zone is what is broken, and a `500` here would
leave an operator unable to tell the two apart.

**API keys are project-scoped on both verbs.** `apikeys.List` returns only the keys of the
request's project and refuses rather than falling back if none resolved — the page rendering
it carries a project selector, and a list ignoring that selector shows keys the user cannot
mint. `apikeys.Create` binds a new key with the precedence *explicit `project_slug` → the
project the session has SELECTED → the install default*: without the middle rung, a user who
switched the console to `atlas` and minted an ingest key would get one bound to `default`, so
the service wired to it reports into a project they were not looking at — a mistake that
surfaces days later as an empty dashboard with events piling up elsewhere.

⚠️ **`query.ListAllAPIKeys` is the one unscoped read, and it has exactly one caller:** the
`apikeys` cache refresh. Validation must resolve a presented key whatever project it belongs
to, and the refresher has no request and therefore no project. Scoping it would make every
other tenant's keys stop authenticating, with a 401 as the only symptom. A second caller
appearing there is the signal that the scoped `ListAPIKeys` was wanted.

#### Scoping the read path

Ingest decides whose data a row *is*; this decides who may *read* it. **Every read of
`monitor.events` carries a mandatory project predicate**, and the mechanism is
`scope/scope.go`:

- `QueryAuthMiddleware` injects a project on **every** accepted branch — an admin key reads
  its own `ProjectSlug` (admin is a scope over *verbs*, never over tenants), the env master
  key reads `MON_DEFAULT_PROJECT` because that is where its own writes land, and a session
  reads whatever it **selected**, defaulting to `MON_DEFAULT_PROJECT`. The session branch is
  the odd one out and the next subsection is entirely about why.
- `scope.ProjectPredicate(ctx)` returns **an error**, not an empty predicate, when the
  context carries no project, and every builder propagates it. An unscoped read is therefore
  not something a caller reaches by omitting a parameter — it is a query that cannot be
  constructed. A route registered outside `QueryAuthMiddleware` 500s on its first request
  instead of quietly serving every project.
- **One chokepoint per SQL builder**, never a predicate per call site:

  | Chokepoint | Covers |
  |---|---|
  | `services.selectEvents` (`query.go`) | `QueryEvents` (count **and** page), `GetLabelValues`, `GetDataKeys`, `GetDataValues` |
  | `services.buildEventWhere` (`analytics.go`) | `QueryAnalytics`, `QueryTimeSeries`, `QueryTopN`, `QueryGauge`; `QueryCompare` inherits it through `QueryGauge` for both periods |
  | `routes.selectIssueEvents` (`issues.go`) | the indexed issue-event lookup **and** both arms of the legacy pre-004 scan |
  | `routes.subscriptionFilters` → `scope.Matches` (`stream.go`, `services/hub.go`) | the SSE live tail |

- **The SSE project filter is server-derived and mandatory.** `project` is absent from
  `clientStreamFilters` *and* is applied after the client's filters, so it cannot be named or
  overridden from the query string; a request with no project is refused rather than
  subscribed. `services.matchesFilters` delegates to `scope.Matches` so the live tail and the
  stored query cannot answer the same question differently.
- **`structs/columns.go` gained `project` in all four sets.** Membership grants the ability to
  *name* the column, never to widen what a request sees — the mandatory predicate is ANDed, so
  `?project=someone-else` yields `project = 'mine' AND project = 'someone-else'` and returns
  nothing.
- **`scope/chokepoint_test.go` is the guard against the *next* leak.** Every other test can
  only prove that today's read paths are scoped; this one walks the AST of `main`, `services`,
  `routes`, `issues` and `alerts` and fails when a function names the events table without
  calling a scoping helper. Exemptions live in one map, each with its reason, so an
  unscopeable read is a visible decision rather than an omission. **An exemption is a claim
  about a function's *callers*, and the AST cannot check one** — `queryAggForRange` sat in that
  map on the grounds that alert evaluation is timer-driven, which was true of one of its two
  callers. When adding an entry, name the callers you checked.

**The empty-string transition — dated, with a removal trigger.** Rows written before 006 read
back as `''`, and the backfill is a manual mutation, so **the default project — and only the
default project — also matches the empty string** (`(project = ? OR project = '')`). Any other
project matches exactly, so this can never widen a real tenant's view. Without it the
dashboard goes blank the moment scoping deploys. **Remove both arms** (`ProjectPredicate` and
`Matches`) once the backfill reports `is_done = 1` **and** 30 days have passed since 006
deployed — **2026-10-06 at the earliest** — after which the TTL has recycled every pre-column
row and an empty project can only mean a bug.

**What the tenancy work added to the wire, for `monitor-web` and `monitor-mcp`.** All of it
is additive — no field was renamed, removed or changed shape, so an existing client keeps
working without knowing any of this exists:

| Response | New field | Note |
|---|---|---|
| any `Event` (query, `/v1/issues/{id}/events`, SSE frames) | `project` | `omitempty`, so a pre-006 row with an empty project omits the key rather than sending `""` |
| `Issue` | `project` | not `omitempty` — an issue always has one, and its absence would mean a bug rather than a legacy row |
| `APIKey` (`GET`/`POST /v1/api-keys`) | `project_id`, `project_slug` | `POST` also *accepts* an optional `project_slug`, defaulting to `MON_DEFAULT_PROJECT` |

Two routes were added for the switcher, both reads, both session-available (the write
surface is `/admin/zones*` + `/admin/projects*`, admin-only and control-plane-only):

| Route | Returns |
|---|---|
| `GET /v1/zones` | the install's **active** zones — exactly one row in Phase 1, which is correct, not a bug |
| `GET /v1/zones/{zone}/projects` | the **active** projects in that zone; a retired *zone* still resolves so a bookmark into one can still render a switcher |

Both accept `limit`/`offset` and default to `db.MAX_LIMIT` rather than the house 50 — a
switcher that silently truncated would hide a tenant. Neither may be called with `?project=`.

Both also accept **`?include_deleted=true`**, which puts retired rows back in the list.
Opt-in, so no switcher starts offering a retired zone as a destination — and the **admin
registry page turns it on**, because that is where retirement is managed and a spent slug
that is simply absent looks free. A non-boolean value is a `400` rather than a silent
`false`: a list quietly missing the rows you asked for is how an operator concludes a retired
zone is gone.

`GET /v1/*` also now accepts **`?project=<slug>`** from a **session**, which selects the
project being read (see §6 *The project asymmetry*): unknown or retired → `400` naming the
slug, absent or blank → `MON_DEFAULT_PROJECT`, two values → `400`. API-key callers are
unaffected — for them `project` remains an ordinary filter column that can only narrow.

`project` is also now a legal filter, group-by and label column (`?project=`,
`group_by: project`, `GET /v1/labels/project/values`) — which today returns a single value.
Naming it never widens a result: the mandatory predicate is ANDed on top.

**Issues are scoped in MariaDB, by a separate mechanism.** `monitor.issues` is reached through
`db.SQL`, never through a ClickHouse builder, so no chokepoint above touches it and the AST
audit does not look at it either. `query.scopeIssues` is its counterpart: `ListIssues`,
`CountIssues`, `GetIssue` and `UpdateIssue`'s own `WHERE` all carry the caller's project, and
an empty project returns `query.ErrNoIssueProject` rather than a query without the predicate.
Handlers resolve it through `routes.requireProject`, which 500s exactly as
`scope.ProjectPredicate` does. Two properties are worth knowing:

- **An issue row carries event data.** `message` is the error text lifted off the event by
  `issues.extractMessage`, alongside the service, the path, the occurrence count and the
  seen-window. Unscoped, `GET /v1/issues` answered "what is failing, where and how often" for
  every tenant — while the raw events behind those failures were correctly scoped.
- **Every id-addressed read is scoped, not just the list.** The id is a UUIDv5 over a
  fingerprint that already contains the project, so a foreign id cannot be *derived* — but it
  can be read off a list, a monitor-web URL, an alert payload or a comment. A foreign id
  returns 404, indistinguishable from one that never existed. `routes.requireIssue` is the
  single gate for the timeline, the occurrence history, all three comment verbs and all three
  link verbs, so scoping it covers eight endpoints at once.

**One known gap and one deliberate divergence, both dated 2026-09-06:**

- **TIMER-DRIVEN alert evaluation is zone-wide.** `Evaluator.Run` has a background context,
  no credential and therefore no project, so `alerts.queryAggForRange` aggregates across every
  project and a rule's threshold counts the whole instance. A rule *can* opt in via a `project`
  filter (that is why `project` joined `FilterColumns`); the alert tables still have no project
  column. **The HTTP half is NOT a gap and is now scoped:** `EvaluateRuleNow`, reached from
  `POST /v1/alert-rules/{id}/test`, passes the request's context, so `scope.ProjectPredicate`
  applies and the aggregate returned to the caller is their own project's. Unscoped it was an
  oracle rather than a gap — a rule carries a caller-chosen aggregation, field and filter set,
  so an admin key bound to one project could read back a count or a `max()` over every
  project's events, one number per request, with no row crossing the boundary to notice. The
  old safety argument ("no rule can read a project its author cannot already read, because rule
  authorship is admin-scoped") was true when admin meant global and became false when
  `QueryAuthMiddleware` made admin a scope over *verbs*. **Consequence, on purpose:** a rule's
  test result and the value that fires it can disagree once a second project exists. See the
  `KNOWN GAP` header on `alerts/evaluator.go`.
- **`monitor.issue_occurrences_daily` has no project column** (005 keys it on
  `(issue_id, day)`), so it cannot be aggregated per project without joining `monitor.issues`.
  Since migration 118 this is a schema nicety, not a boundary: issue ids are derived from a
  fingerprint that includes the project, so **one rollup key can no longer accumulate two
  tenants' occurrences**. Adding the column needs a table rebuild plus a `DROP`/`CREATE` of the
  materialized view, which the boot runner cannot perform. A predicate on those reads would
  still be wrong — every caller already holds an issue read from `monitor.issues`, so it would
  narrow a histogram under an issue the caller can read in full. **That argument depends on the
  issue read path being scoped**, which it now is (`query.scopeIssues`) and was not when this
  was written: an unscoped list handed out any tenant's issue id, which these reads turn into
  that tenant's per-day counts. See the `KNOWN GAP` header on `issues/history.go`.

### Stores — configuration vs facts

There are two datastores and one rule for deciding which a table belongs in. It is worth
stating explicitly because the repo got it wrong at first and paid for it twice.

**A table belongs in ClickHouse if it holds FACTS: rows written by machines, appended at
volume, read as a series, and expired by a TTL.** Events, issue occurrences, alert states,
alert history. Nothing edits them.

**A table belongs in MariaDB if it holds CONFIGURATION: rows a human edits, that reference
each other, and whose correctness depends on constraints.** Users, api keys, the tenancy
registry, issues (the triage state on them is edited), service→repo mappings, and — since
migrations 119-124 — every alerting and dashboard setting.

| Table | Store | Why |
|---|---|---|
| `events`, `issue_occurrences_daily` | ClickHouse | facts, columnar, TTL'd |
| `alert_states` | ClickHouse | one row per rule, rewritten by a 15s timer |
| `alert_history` | ClickHouse | append-only transitions, **90-day TTL** (no MariaDB equivalent) |
| `monitor.issues`, `issue_timeline`, `issue_links` | MariaDB | triage state, uniqueness, FKs (111-114) |
| `monitor.service_repos` | MariaDB | explicit mapping (115) |
| `monitor.alert_rules`, `notification_channels`, `notification_policies`, `service_groups` | MariaDB | alerting configuration (119-122) |
| `monitor.dashboards`, `monitor.saved_views` | MariaDB | saved UI state (123-124) |
| `monitor_auth.*` | MariaDB | identity + tenancy (100-109, 116-117) |

**What getting it wrong cost, both times.** Eight tables were originally created by ad-hoc
`CREATE TABLE IF NOT EXISTS` inside package `Init()` functions at boot, with no migration
file, all `MergeTree`/`ReplacingMergeTree ORDER BY (id)`. That engine cannot enforce
uniqueness and deduplicates only when a background merge happens to run:

- **Issues (fixed in 324f612, migrations 111-114):** four rows for one fingerprint in
  production, and `occurrence_count` drift under concurrent workers that was only ever
  mitigated process-locally by a 64-way mutex shard.
- **Notification policies (fixed by migration 121):** `position` is an evaluation order and
  had no uniqueness at all. `getNextPosition` was a racy `max(position) + 1`, and
  `ReorderPolicies` rewrote rows one at a time with no transaction — ClickHouse has none —
  so a mid-loop failure left the list half-renumbered with duplicates by construction. Two
  policies at one position give `alerts/router.go` a non-deterministic first match, which
  silently changes **where alerts are sent**, per evaluation, with nothing logged.

The full argument, table by table, is in the header of `db/migrations/119_create_alert_rules.sql`.

**How reordering works under the new UNIQUE key**, because the obvious implementation does
not: InnoDB checks a unique index **per row**, and MariaDB has no deferrable constraints, so
assigning `1..N` in a loop collides the moment a policy moves onto a slot another has not
vacated. `query.ReorderNotificationPolicies` runs one transaction that (1) locks the whole
list `FOR UPDATE`, (2) `UPDATE … SET position = -position` in a single statement — negation
is injective and its image is disjoint from its domain, so it can never violate the key —
and (3) assigns `1..N` into the now-empty positive range. That is why
`structs.NotificationPolicy.Position` is a **signed** int and why the column carries no
`CHECK (position > 0)`. A caller may name a subset: those lead, the rest keep their relative
order and follow. `query/notification_policies_query_test.go` fails if the vacate step is
dropped, reordered, or taken out of its transaction.

**Two known inconsistencies, recorded rather than left to be found:**

- `alert_states` and `alert_history` are still created by an ad-hoc `CREATE TABLE` in
  `alerts.Init`, which is exactly the pattern 119 condemns. They are the last two in the
  repo created that way; giving them files means a ClickHouse migration (`migrations/007…`),
  a different runner with its own re-runnability story, and it is a separate change.
- **None of the six moved tables has a `project` column.** Adding one would partition six
  live configuration sets between tenants that share them today — a behaviour change, not a
  move — and it is the same question as the evaluator's zone-wide gap. What the move does is
  make it cheap: one migration per table, against a store that can enforce the result.

**Legacy ClickHouse tables are left in place, unread.** `monitor-core backfill-config` can
only be re-run while they exist. Drop them by hand once a release has passed, along with
`cutover/` and the `dashboards/`/`views/` tombstones.

**Booting before that copy has run DISABLES ALERTING — loudly — rather than crash-looping.**
`cutover.RequireConfigBackfill` runs first in main()'s data-plane block, ahead of
`alerts.Init` so the default-policy seed cannot fire on an un-migrated instance. When a
legacy table holds rows the MariaDB table does not, the process **starts anyway**, logs a
banner, does **not start the alert evaluator**, sets `routes.AlertingDisabledReason`, and
reports `alerting_ok: false` on `/health` plus a **503 naming `alerting`** on `/ready`.

That is a deliberate trade and the reasoning must survive: a fatal guard crash-loops the
container, and CI redeploys this image on every push to `main`, so fatal would take
**ingestion** down on the DEFAULT path — swapping a rare silent failure for a common loud
outage in the one system whose job is to still be recording when everything else breaks.
Alerting is the thing that would be silently wrong, so alerting is the thing that stops;
ingest, queries and issues are untouched. `routes/health_test.go` pins the visibility,
because degrading is only defensible while the degraded state is impossible to miss.

The guard is gated on a `settings` marker the backfill stamps, so it can fire only once,
never on a fresh install, and never again after a real cutover. §8 carries the runbook.

### Request flow — ingestion (unchanged by the auth overhaul)

```
go-monitor / monitor-js SDK
  → POST /v1/events  (NDJSON body, X-Api-Key)         [IngestAuthMiddleware]
      resolves the credential's project and injects it (middleware.WithProject)
  → routes.IngestEventsHandler parses+validates the WHOLE body into a slice first
      (any bad line → 400, nothing enqueued — retry-safe)
  → per event: stamps event.Project (from the request context) + event.IssueID,
      then services.Queue.Enqueue (only Enqueue==true counts toward `accepted`)
  → services.Batcher (goroutine) flushes on BATCH_SIZE or FLUSH_INTERVAL
  → db.Writer batch-inserts into ClickHouse <CLICKHOUSE_DATABASE>.events
      (5 retries, then the batch is abandoned — and counted into `dropped`)
  → (in parallel) publishes to the SSE hub + issues.TrackError for error/fatal
```

An event can therefore die in two places, and **both feed the one `dropped` counter**
`/health` reports: refused at `Enqueue` when the queue is full, or destroyed by the
Batcher when the writer never succeeds. Before that second call existed a zone with a dead
ClickHouse answered `{"status":"ok","dropped":0}` while losing everything.

**A `project` in the NDJSON body is parsed and then thrown away.** `Event.Project` is
unmarshalled like any other field — it has to be, or the discard could not be tested — and
then overwritten from the request context for *every* event, exactly as `Event.IssueID` is.
The overwrite is unconditional: there is no "respect a non-empty client value" branch, and
adding one (it reads as a harmless nicety) hands any holder of any ingest key the ability to
file events under any other tenant's project, indistinguishably from that tenant's real
traffic. `routes/events_test.go` pins both halves. The handler also applies **no** fallback
when the context carries no project — `IngestAuthMiddleware` is the only route to it and
resolves one for every credential it accepts, so a second default here would be a second
answer to "whose data is this".

### Request flow — query/analytics/UI

```
monitor-web → Next.js proxy (mon-* cookies + X-CSRF-Token) → GET/POST /v1/*  [QueryAuthMiddleware]
      resolves the credential's project and injects it (scope.WithProject)
  → routes/* handler → services.Query / services.Analytics builds squirrel SQL
      every builder starts at its chokepoint, which ANDs scope.ProjectPredicate(ctx);
      no project on the context ⇒ an error, never an unscoped query
  → ClickHouse → responder envelope
```

### Background goroutines (started in `main.go`)

The first two are **data plane only** — they do not start under `MON_ROLE=app`. The
API-key refresher starts in every role, because both planes authenticate API keys.

- **Batcher** — drains the queue into ClickHouse.
- **Alert evaluator** — 15s ticker; evaluates every enabled rule against ClickHouse,
  tracks firing/resolved state, records history, publishes to the alert SSE hub, routes
  notifications through policies. Alert types (`threshold`/`absence`/`rate_change`) and the
  pointer-field `UpdateRuleRequest` contract are documented in `alerts/AGENTS.md`.
- **API-key cache refresher** — `CACHE_REFRESH_INTERVAL` (30s) ticker started by
  `apikeys.Init`; re-reads `api_keys` from MariaDB so revocation propagates (§5).

All three are wrapped in `recover()` and cancelled via the shutdown context. Note the
`recover()` sits **outside** the loop in each of them, so a panic ends that goroutine for
the life of the process rather than restarting it — a shared weakness, not one of them
deviating.

### External systems

- **ClickHouse** — events/analytics. DB `monitor` unless `CLICKHOUSE_DATABASE` says
  otherwise (the DDL is written against the literal `monitor.` and rewritten at run time —
  §4), table `events` (30-day TTL; carries the `project` tenant column since 006 — see
  §6 Tenancy for why it is neither a key nor indexed, and why pre-006 rows read empty),
  plus
  `issue_occurrences_daily` — an `AggregatingMergeTree` rollup with **no TTL**, fed by a
  materialized view on `events` keyed by `issue_id`. It is what lets an issue's history
  outlive the events that produced it: after 30 days the totals and the seen-window
  survive even though every raw event is gone. Read it with the `-Merge` combinators
  (`countMerge`, `minMerge`, `maxMerge`) — the stored columns are partial aggregation
  states, not numbers.
- **MariaDB** — two schemas on one host, one `db.SQL` pool (`mariadb:11.4`, host port 3336
  locally). `monitor_auth` holds the auth layer; `monitor` holds issue-tracking state
  (`issues`, `issue_timeline`, `issue_links`). Cross-schema joins between them are native.
  **⚠️ The DSN user needs privileges on both**, and `migrations_applied` lives in `monitor_auth`.
  **⚠️ ClickHouse also has a database named `monitor`** — same name, different engine. A
  qualified `monitor.<table>` in a squirrel query means MariaDB; ClickHouse is reached via
  `db.Conn`.
- **SSO IdPs** — any OIDC or OAuth2 provider configured in `sso_providers` (none by default).
- **Keyring** — optional secret injection at boot (`main.go`); skipped if `KEYRING_*` is
  absent, falling back to plain env vars. Sources `MON_DB_DSN`, `MON_JWT_SIGNING_KEY`,
  `MON_CRYPTO_KEY`, `MON_ADMIN_*`, and each provider's `client_secret_ref` in production.

---

## 7. Ecosystem & related repos

| Repo | Relationship |
|---|---|
| `monitor-web` | The Next.js dashboard. Calls `/v1/*` and `/auth/*` via a server-side proxy that forwards `mon-*` cookies + `X-CSRF-Token`. Its auth model mirrors this repo's — see `monitor-web/AGENTS.md`. |
| `go-monitor` | Go SDK. POSTs NDJSON to `POST /v1/events` with `X-Api-Key`. **Diff against `go-monitor/AGENTS.md` when touching ingestion** — the wire contract is unchanged by the auth overhaul, but the key it sends must now be the env master key or an `ingest`-scope key; an `admin` key no longer ingests. |
| `monitor-js` | TypeScript SDK (GitHub only). Same ingestion contract. |
| `monitor-mcp` | MCP server exposing this API to Claude (`mcp__monitor__*`). **House rule: any new `/v1/*` route should add or consciously skip a matching MCP tool in the same change.** The `/auth/*` + `/admin/sso-providers` surface is browser/cookie-oriented and not part of the MCP tool set. The registry **write** surface (`/admin/zones*`, `/admin/projects*`) is **consciously skipped** for now: writing a zone row is an act with hand-provisioned infrastructure behind it and an unreusable slug in front of it, so it wants an operator looking at a form, not an agent inferring one. `monitor_list_zones` already covers the read. |

---

## 8. Operations

- **Deploy:** on a push to `main`, `build-and-deploy.yml` builds the image to
  `registry.appleby.cloud/monitor-core` and then **triggers the redeploy itself** — a
  `POST` to `LATTICE_DEPLOY_URL?container=monitor-core&commit=<sha>`. This is the same
  step every other service in the ecosystem uses; keep it identical.
  Two prerequisites, both outside this repo: the `LATTICE_DEPLOY_URL` repo secret, and an
  active deploy token on the Lattice stack. `monitor-core` and `monitor-web` are
  containers in the **same stack** ("Trailblaze Monitor"), so they share one token and one
  URL — `?container=` is what separates them. A green CI run with no visible change means
  checking the token's `last_used_at`: if it's `null`, CI never reached Lattice.
- Do **not** deploy by hand from here (repo guardrails).

- **⚠️ CUTOVER DEPLOY — migrations 119-124 (one-time, THE ORDER MATTERS).**

  The six alerting/dashboard configuration tables moved from ClickHouse to MariaDB (§6
  *Stores*). The new binary reads them from MariaDB; the legacy ClickHouse tables are left
  in place and unread. **`monitor-core backfill-config` must run before the new binary
  serves.**

  **`cutover.RequireConfigBackfill` catches that at boot** — first thing in main()'s
  data-plane block, before `alerts.Init`. If a legacy ClickHouse table holds rows, the
  MariaDB table that replaced it is empty, and the backfill has never recorded a completed
  run, the process **starts but refuses to evaluate alerts**, naming every stranded table
  and the command that fixes it, and failing `/ready` until it is run. Without it both
  failure modes below are entirely silent, and CI makes the bad ordering the default rather
  than the exception:

  - **Alerting stops, invisibly.** `alert_rules` is empty, so `listEnabledRules` returns
    nothing and the evaluator has nothing to evaluate. No error, no log line, no alert —
    there is no code path that distinguishes "no rules configured" from "six rules gone".
  - **The default notification policies double.** `alerts.Init` seeds four defaults into an
    empty policies table; the backfill then appends the four ClickHouse originals after them
    (nothing collides — positions are renumbered from `MAX+1`), leaving eight policies of
    which four match every alert by priority and route it nowhere. Running the guard *before*
    `alerts.Init` means this state is never created rather than created and then explained.

  The guard is a **one-shot, not a permanent tripwire**. It short-circuits on a
  `cutover:config_backfill_completed_at` row in `settings`, stamped by `BackfillConfig` only
  after all six tables have copied — so once the cutover has happened, row counts stop
  mattering and deleting your last alert rule through the UI can never bring the refusal
  back. It also never fires on a fresh install or after the legacy tables are dropped
  (existence is probed through `system.tables`, so an absent table reads as zero rather than
  as an error). A ClickHouse probe that errors **fails open with a warning** — in the state
  being guarded, ClickHouse is healthy by construction, so refusing over a probe error would
  invent a new way for a deploy to fail; the MariaDB count fails closed.

  The safe sequence:

  0. **Take the census first.** Before anything is merged, record what the old build is
     serving — `GET /v1/alert-rules`, `GET /v1/notification-policies`,
     `/v1/notification-channels`, `/v1/service-groups`, `/v1/dashboards`, `/v1/views` — and
     keep the rule *names*, not just the counts. Step 5 is a comparison, and a comparison
     with nothing to compare against is a vibe. This is cheap insurance rather than a hard
     prerequisite: the cutover never writes to ClickHouse, so the same numbers are
     recoverable from the untouched legacy tables at any later point (see the queries in
     step 5) — but recovering them under time pressure, from a store you are in the middle
     of migrating off, is not where you want to be first learning that `count()` and
     `count() FINAL` disagree.
  1. **Pause CI's auto-redeploy of the `monitor-core` container, or be ready to run step 3
     immediately.** `build-and-deploy.yml` builds *and* redeploys on a push to `main`, and
     the subcommand only exists in the new image — so by default the new binary starts
     before anyone can run it. With the guard in place that is a **visibly degraded start,
     not a silent outage and not an ingest outage**: the container serves, ingestion and
     queries continue, `/ready` returns 503 naming `alerting`, and only alert evaluation is
     off until step 3 runs. Pausing the redeploy is still the tidier path, but the default
     ordering is now survivable rather than an incident.
  2. Merge. CI builds `registry.appleby.cloud/monitor-core:latest`.
  1a. **If the new binary already booted before the cutover, CLEAR THE SEEDED POLICIES
     FIRST.** A build older than this fix seeded four default notification policies into
     the empty MariaDB table on boot, and the cutover then appends the real ones behind
     them at positions 5-8. Matching is first-past-the-post by position and every default
     carries `continue_matching = false` with `channel_ids = "[]"`, so the defaults would
     govern every route and send it nowhere, while the real policies are never reached.
     Nothing errors.

     The tell is in the dry run: `renumbered from position 1 to 5`. A clean cutover
     inserts at positions 1-4 with no renumbering at all.

     ⚠️ **THIS IS THE MariaDB `monitor` SCHEMA, NOT THE ClickHouse DATABASE OF THE SAME
     NAME.** Migration 110's header records that collision and it bites hardest right here:
     the seeded rows to delete are in MariaDB, while the ClickHouse table of the same name
     still holds the four REAL policies the cutover is about to read. Running the delete
     against ClickHouse destroys them permanently. ClickHouse rejects a bare `DELETE FROM
     <table>` with a syntax error (it wants `DELETE FROM … WHERE …`), which is the only
     thing standing between a mis-targeted paste and data loss — do not "fix" it by adding
     `WHERE 1=1`.

     Confirm what is there, then clear it. The real policies are still in ClickHouse, so
     the MariaDB table should hold nothing but the four seeded defaults:

     ```sh
     # from the monitor-mariadb container
     mariadb -u"$MARIADB_USER" -p"$MARIADB_PASSWORD" monitor \
       -e "SELECT id, name, position, channel_ids FROM notification_policies ORDER BY position;"
     # expect exactly: Critical (P0) — All Channels / High (P1) — PagerDuty + Email /
     #                 Medium (P2) — Email Only / Low (P3) — Web Only, all channel_ids '[]'

     mariadb -u"$MARIADB_USER" -p"$MARIADB_PASSWORD" monitor \
       -e "DELETE FROM notification_policies;"
     ```

     Then re-run the dry run and check it reports positions 1-4 with no renumbering.
     `alerts.Init` no longer seeds while the cutover guard is unsatisfied, so this cannot
     recur — but an install that booted the intermediate build needs the one-time clear.

  2a. **Rehearse it first: `monitor-core backfill-config --dry-run`.** Reads, validates and
     reports exactly what would be copied and what would be REFUSED, writing nothing and
     stamping no marker. Worth running every time — the refusals are the failure that
     actually bites (a duplicate `name` against the new `UNIQUE` keys, an empty enum), and
     finding them in a rehearsal beats finding them halfway through a hand-run cutover on a
     deploy that currently has alerting switched off. A clean dry run exercises every read,
     every enum check and every duplicate-key decision — only the write is skipped.
     The dry run is enforced at ONE chokepoint (`cutover.insertRow`), so a seventh table
     added later is covered by default; `cutover/guard_test.go` pins both that it writes
     nothing AND, via a negative control, that the guard is not simply always on.
  3. Run the cutover from the **new image**, with the same env (`MON_DB_DSN`, the
     `CLICKHOUSE_*` vars, `MONITOR_API_KEY`) and a role that includes the data plane:
     `monitor-core backfill-config`. It applies migrations 119-124 itself (the runner runs
     before the subcommand), then copies. Each table is reported twice — once by
     `cutover.BackfillConfig` as it finishes
     (`cutover: alert_rules — copied 6, skipped 0 (already present)`) and again in `main.go`'s
     end-of-run summary (`alert_rules   copied 6, skipped 0`), the second of which prints
     **even when the run then fails**, so a partial cutover still says how far it got.
     **Read those lines before moving on** — they are the only place the per-table outcome
     is stated, and a table that copied 0 rows when the step-0 census said otherwise is the
     failure this whole runbook exists to catch, visible here a full step before it becomes
     an alerting outage.
  4. Redeploy the serving container. On boot `RequireConfigBackfill` finds the marker and
     passes, and the policies table is non-empty so `alerts.Init` seeds nothing.
  5. **Verify — the alert rules specifically, and by count on both sides.** Everything else
     that moved is visible in the UI the moment someone looks at it; a missing alert rule is
     visible only when an outage fails to page anyone, which is exactly when nobody is
     reading a runbook. So check it directly and check it numerically:

     ```
     GET /v1/alert-rules                                     # what the new build serves
     SELECT COUNT(*) FROM monitor.alert_rules;               # MariaDB — the new source of truth
     SELECT count() FROM monitor.alert_rules FINAL;          # ClickHouse — what should have moved
     ```

     The three must agree (**6 at the time of writing** — re-derive it, do not trust that
     figure), and the rule names must match the step-0 census, not merely the totals: six
     rows of which one is the wrong rule is still a page that never fires.

     **`FINAL` is not optional in that third query.** The legacy `alert_rules` is a
     `ReplacingMergeTree(updated_at)`, so every edit ever made to a rule may still be sitting
     there as an un-merged row until a background merge collapses it. Without `FINAL` the
     ClickHouse side over-counts — sometimes by a lot on a heavily-edited rule — and you will
     spend the deploy window hunting rows that were never missing. `cutover/config.go` reads
     the legacy `alert_rules`, `service_groups`, `notification_policies` and `dashboards`
     `FINAL` for the same reason, and reads `notification_channels` and `saved_views`
     *without* it because those two are plain `MergeTree`s that never deduplicated at all.
     Match the copy when you check the copy.

     **If the counts do not match — the ladder, cheapest first.** All four rungs are
     non-destructive to ClickHouse, which still holds the originals:

     1. **MariaDB has fewer rows than ClickHouse `FINAL`.** Re-run
        `monitor-core backfill-config`. This is the ordinary repair and usually the whole
        answer — already-present ids are skipped, so a re-run copies exactly the rows that
        did not make it and touches nothing else. Re-running is safe an unlimited number of
        times.
     2. **The re-run copies nothing and the shortfall persists.** Read its output: a row it
        refuses names itself. The designed refusal is the `UNIQUE (name)` collision (errno
        1062) that migrations 119/120/122 introduced and ClickHouse never had — two rules
        genuinely sharing a name. Rename one **in ClickHouse** and re-run; nothing already
        copied is redone.
     3. **MariaDB is empty across the board and the guard let the process boot anyway.** The
        marker was stamped by an earlier run — check
        ``SELECT * FROM settings WHERE `key` = 'cutover:config_backfill_completed_at';``.
        Note the table is **unqualified**: `settings` is migration 105 and lives in the
        DSN's own default schema (`monitor_auth`), unlike the six tables that just moved,
        which are explicitly `monitor.*`. Running that query against `monitor` returns a
        "table doesn't exist" that reads exactly like "the marker was never written".
        Deleting the row restores the boot guard's protection; the backfill re-stamps it on
        the next complete run. Do not delete it as routine hygiene: without it, every future
        boot re-counts six tables against ClickHouse for no benefit.
     4. **`GET /v1/notification-policies` returns eight rows.** Step 3 ran after the new
        binary had already served: `alerts.Init` seeded four defaults into the empty table
        and the backfill appended the four originals behind them (positions renumber from
        `MAX+1`, so nothing collided and nothing errored). Four policies now match every
        alert by priority and route it nowhere. Delete the **seeded** four — they are the
        rows whose ids are *not* in `monitor.notification_policies`' ClickHouse counterpart
        — and the list is correct again. This is the one failure mode that leaves no error
        anywhere, which is why the boot guard exists to prevent it rather than to report it.

     **Nothing here is a reason to roll back on its own.** ClickHouse is untouched, so the
     old image still serves the complete configuration — but see *Rollback is clean but
     one-way* below before reaching for it, because any edit made through the new build is
     invisible to the old one.

  Properties worth knowing:

  - **Re-running is safe and is the repair.** A row whose id is already in MariaDB is
    SKIPPED, never overwritten — deliberately *unlike* `backfill-issues`, whose conflict
    rule is monotonic and can therefore merge. These are rows a human edits, so an upsert on
    a re-run would silently restore an old threshold from a store nobody writes to any more.
    A config edit made between step 3 and step 4 lands in ClickHouse and is missed; re-run
    step 3 after step 4 to pick it up.
  - **It can abort, on purpose.** Migrations 119, 120 and 122 add `UNIQUE (name)` keys the
    ClickHouse tables never had. A duplicate rule/channel/group name stops the copy with
    errno 1062, wrapped with the table, the row and the remedy. Rename one and re-run;
    everything already copied is skipped.
  - **Rollback is clean but one-way.** The old binary reads ClickHouse, which is untouched,
    so reverting the image works — but any configuration edited through the new build lives
    only in MariaDB and the old one will not see it.
  - **A partial run leaves no marker.** `BackfillConfig` stamps
    `cutover:config_backfill_completed_at` only after all six tables have copied, so an
    abort part-way keeps the boot guard refusing until the rest is copied — which is the
    correct reading of "half the configuration is still in ClickHouse".
  - **Drop the legacy ClickHouse tables by hand once a release has passed**, together with
    `cutover/` (guard included) and the `dashboards/`/`views/` tombstone files. Dropping the
    tables ends the ability to re-run the backfill, and also clears the guard permanently —
    which is the escape hatch if the legacy rows are genuinely being abandoned. Delete the
    `settings` marker row in the same pass.
- **Config (env vars, defaults from `env/env.go`):**

  | Var | Default | Notes |
  |---|---|---|
  | `HTTP_PORT` | `8080` | |
  | `MON_ROLE` | `both` | which plane this process runs: `app` (control), `zone` (data) or `both`. **Unset must stay `both`** — that is the deployed configuration and the default exists so a deploy of this feature changes nothing. Case and surrounding whitespace are normalised; anything else **refuses to boot** rather than falling back, because a typo that degraded to the default would run the control plane on a host meant to be a zone. Gates the ClickHouse connect, the bootstrap seeders, `sso.Install`, the ingest/alerting goroutines **and route registration** — see §6 *Roles* |
  | `CLICKHOUSE_ADDR` / `_DATABASE` / `_USERNAME` / `_PASSWORD` | `localhost:9000` / `monitor` / `default` / `` | events store. `_DATABASE` must match `^[a-z][a-z0-9_]{2,62}$` (`db.ValidateDatabaseName`) — it is interpolated into SQL, never bound, so a name outside that shape refuses to boot |
  | `CLICKHOUSE_MAX_MEMORY_USAGE` | `2147483648` (2 GiB) | per-**query** memory ceiling in bytes, sent as the `max_memory_usage` setting. Sized for the ~8 GB host; ClickHouse's own default (10 GiB) exceeds the machine, so unset it is no limit and one high-cardinality `GROUP BY` OOM-kills the server and ingest with it. Over the limit, that single query fails with `MEMORY_LIMIT_EXCEEDED` and nothing else is affected |
  | `MONITOR_API_KEY` | *(required)* | env master ingest/query key (`X-Api-Key`); refuses to boot without it |
  | `MON_DB_DSN` | `monitor:monitor@tcp(127.0.0.1:3336)/monitor_auth` | MariaDB DSN; from Keyring in prod |
  | `MON_JWT_SIGNING_KEY` | *(dev default)* | HS512 session key; **prod must override** (else fail-fast) |
  | `MON_CRYPTO_KEY` | *(dev default)* | **exactly 32 bytes**, AES-256-GCM key for SSO secrets/tokens; **prod must override** |
  | `MON_COOKIE_DOMAIN` | `` | domain on the `mon-*` cookies |
  | `MON_COOKIE_INSECURE` | `false` | `true` = local dev (drops `Secure`, allows dev-default secrets) |
  | `MON_PUBLIC_URL` | `https://monitor.appleby.cloud` | this process's own origin. Builds each SSO `redirect_uri` (`{base}/auth/sso/{slug}/callback`) — must match the IdP registration byte-for-byte — **and** seeds a fresh zone's `ingest_url`/`query_url` in `bootstrap.EnsureZoneAndProject` |
  | `MON_ADMIN_EMAIL` / `MON_ADMIN_PASSWORD` | `` | seed the first admin on a fresh DB; empty = no seed |
  | `MON_ALLOW_REGISTRATION` | `false` | gates `POST /auth/register` |
  | `MON_ZONE_SLUG` | `trailblaze` | slug of the single zone seeded at boot. The default is what the existing data actually is — all 15 services currently reporting are Trailblaze services. Slugs are immutable, so changing it after the row exists seeds a *second* zone rather than renaming the first. Also read by `apikeys.resolveProject` on `POST /v1/api-keys` |
  | `MON_DEFAULT_PROJECT` | `default` | slug of the project seeded inside that zone. Every `api_keys` row binds to it, and the env master key (which has no `api_keys` row, so no binding) stamps events with it — the Mimir `anonymous` / Loki `fake` precedent that the tenant dimension is never null. **NOT boot-only — do not change it on a running install.** It is read per request (both auth middlewares), per query (`scope.ProjectPredicate`), per SSE event (`scope.Matches`) and per error (`issues.fingerprintProject`). Only the reader whose project equals it also matches the empty-project pre-006 rows, so repointing it hides every unbackfilled event; existing API keys hold a project *id* and do not follow it |
  | `MON_GITHUB_TOKEN_TRAILBLAZE` | `` | default fine-grained PAT for fetching linked PR state. **Optional** — unset means links store fine but carry no live state |
  | `MON_GITHUB_TOKEN_<OWNER>` | `` | per-org token, name derived from the GitHub owner (`TeamTrailblaze` → `MON_GITHUB_TOKEN_TEAMTRAILBLAZE`). Takes precedence over the default; adding an org needs no code change |
  | `MON_GITHUB_WEBHOOK_SECRET_TRAILBLAZE` | `` | shared secret GitHub signs deliveries with. **Optional, but unset REJECTS every delivery** — it never fails open. Generate with `openssl rand -hex 32` and paste the same value into the repo's webhook config |
  | `BATCH_SIZE` / `FLUSH_INTERVAL` / `QUEUE_SIZE` / `MAX_SSE_SUBSCRIBERS` | `1000` / `5s` / `100000` / `100` | ingestion/SSE tuning |

- **Manual reconciliation scripts (`migrations/manual/`) — the operator runbook.**

  These are the only pieces of SQL in the repo that a human runs. Nothing embeds them,
  nothing applies them at boot, and nothing verifies afterwards that they were run — which
  is exactly why they are written down here rather than left to whoever remembers.

  | Script | What it does | When to run it |
  |---|---|---|
  | `backfill_events_project.sql` | Stamps `MON_DEFAULT_PROJECT` onto every `events` row written before `006_events_project.sql`, which reads back with an empty `project`. | Once, any time after 006 has deployed. Until it has run *and* 30 days have passed, the empty-string arm in `scope.ProjectPredicate`/`scope.Matches` is what keeps that history visible — the two are a pair (§6). |
  | `delete_orphaned_issue_rollups.sql` | Deletes `issue_occurrences_daily` rows whose `issue_id` is no longer present in `monitor.issues`. | Only after the pre-118 issues have been dealt with **and their rows DELETEd**. It keys on absence, not on age or status, so it is a deliberate no-op while those rows still exist. |
  | `dedupe_issues_by_fingerprint.sql` | Folds the duplicate rows one fingerprint acquired in the **legacy ClickHouse** `issues` table, before ids were derived from the fingerprint. | **Historical — do not run it now.** Issues moved to MariaDB (`111_create_issues.sql`); the ClickHouse table it edits is unread. Kept only until that table is dropped. |

  Three properties are shared by all three files, and each is the reason the file is where
  it is rather than a style preference:

  - **They live in `migrations/manual/` and must never move up a directory.** The ClickHouse
    runner has no applied-tracking table: it replays every top-level `migrations/*.sql` on
    **every** boot and requires idempotent DDL. `ALTER TABLE … UPDATE`/`DELETE` is an
    asynchronous mutation, so one of these in `migrations/` would queue a fresh table-wide
    rewrite on every restart, forever — one more per crash-loop iteration. `manual/` is
    outside the `*.sql` embed pattern precisely so that cannot happen by accident. Their
    in-comment semicolons would also crash startup on their own, since the runner splits
    naively on `;`.
  - **They are NOT rewritten for `CLICKHOUSE_DATABASE`.** The `migrations.rewriter` only sees
    embedded files, so the literal `monitor.` prefix in these is the database that gets hit.
    On a deployment with a non-default database, edit the prefix before running.
  - **Mutations are asynchronous.** The statement returns when it is *accepted*, not when it
    is applied, and a failure lands in `latest_fail_reason` rather than at the client. Verify
    with `SELECT mutation_id, parts_to_do, is_done, latest_fail_reason FROM system.mutations
    WHERE database = 'monitor' AND table = '<table>' AND is_done = 0`, and cancel a bad one
    with `KILL MUTATION WHERE … mutation_id = '…'`. Each script carries the exact
    before/watch/after queries in its own header; run them, they are the verification.

  ⚠️ **`delete_orphaned_issue_rollups.sql` has a trap its header spells out in full, and
  step 0a exists solely to defuse it.** The live issue ids are in MariaDB and the rollup is in
  ClickHouse, so the predicate crosses stores through the `mysql()` table function — and
  `issue_id NOT IN (empty set)` is true for **every** row. Bad credentials, a wrong database
  argument or an unreachable host therefore delete the entire no-TTL rollup, which cannot be
  reconstructed from `events` once the 30-day window has passed. Prove the subquery returns a
  non-zero count first, and take the backup the header describes.

- **Monitoring:** Monitor monitors itself — `mcp__monitor__monitor_service_overview` on
  `monitor-core`; `lattice_get_container_logs` for the container.
- **Common failure modes:**
  - *Refuses to start: "MON_ROLE=… is not a recognised role"* → a typo in `MON_ROLE`. Unset
    it (that means `both`) or set exactly `app`, `zone` or `both`. It refuses on purpose;
    see §6 *Roles*.
  - *A route that used to work now 404s, everything else is fine* → check the `role` on
    `GET /health` and the role line at the top of the boot log. A single-plane process does
    not register the other plane's routes, and a 404 is what that is designed to look like.
  - *Refuses to start: "MON_JWT_SIGNING_KEY … dev default" / "MON_CRYPTO_KEY must be
    exactly 32 bytes"* → set real secrets (or `MON_COOKIE_INSECURE=true` for dev).
  - *Startup aborts on ClickHouse/MariaDB migrations* → the store is unreachable or a bad
    `.sql`; both runners are fail-fast.
  - *Login works but SSO fails at the IdP* → `MON_PUBLIC_URL` / the provider's redirect_uri
    don't match, or `client_secret_ref` can't resolve via Keyring/env.
  - *Everyone logged out at once* → the SSO checkpoint fails **open** on IdP blips, so this
    points at the JWT signing key changing or MariaDB being down, not the checkpoint.
  - *One analytics query 500s with `MEMORY_LIMIT_EXCEEDED` (code 241)* → it hit
    `CLICKHOUSE_MAX_MEMORY_USAGE`. That is the limit working: before it, that query would
    have climbed until the node OOM-killed ClickHouse. Narrow the group-by/time range first;
    only raise the ceiling if the host actually has the headroom.
  - *Boot logs `⚠️  ALERTING IS DISABLED`, `/ready` is 503 naming `alerting`, `/health`
    reports `alerting_ok: false`* → exactly what it says, and the guard doing its job.
    Ingest and queries are FINE; only evaluation is off. Run `monitor-core backfill-config`
    from the same image and env (see the cutover section above), then restart the service.
    If those legacy rows are being abandoned on purpose, drop the ClickHouse tables it names
    instead.
  - *No alerts firing at all, nothing in `alert_history`, `GET /v1/alert-rules` returns `[]`*
    → the config cutover has not run **and the guard did not catch it** — which means either
    the ClickHouse existence probe failed open (look for the `WARNING: could not check
    whether …` line at boot) or the legacy tables are already gone. Run
    `monitor-core backfill-config`. An empty rule set is indistinguishable from a quiet
    system from the outside, which is why it is listed here rather than left to be noticed.
  - *Eight notification policies where there should be four, four of them routing nowhere*
    → `backfill-config` ran after the new binary had already seeded a fresh install. The
    boot guard now prevents this (it runs before `alerts.Init`), so reaching it means the
    seed happened on a build without the guard. Delete the four seeded rows (the ids not
    present in the ClickHouse table).
  - *`ERROR 1062 … uq_alert_rules_name` during `backfill-config`* → two rules share a name.
    Migrations 119/120/122 add uniqueness the ClickHouse tables never had. Rename one and
    re-run; copied rows are skipped.
  - *Events silently missing under load* → queue overflow (`QUEUE_SIZE`); `/health` reports
    `dropped`. That counter now also covers batches the Batcher abandoned (5 failed writes,
    or shutdown mid-retry), so a rising `dropped` with a stale/absent `last_flush_at` means
    the writer is failing, not that ingestion is outrunning the queue — check `/ready`,
    which names the store that is down.

---

## 9. Rules & guardrails

- Never add an ORM; keep squirrel + native ClickHouse driver + `db.Queryable` for MariaDB.
- **Always validate `data.*` field names** with `structs.SafeIdentifierRegex` before
  putting them into ClickHouse SQL. That is the injection boundary.
- **Never re-declare the identifier regex or a column allowlist outside
  `structs/columns.go`.** A local copy is how the dot divergence happened; extend the
  shared set instead.
- **Identity is `(provider, provider_user_id)`, never email.** Do not weaken the
  verified-both-sides link gate in `sso/resolve.go` — it is the pre-account-takeover defense.
- Never persist a raw refresh token (store the SHA-256 hash) or a raw SSO client
  secret/IdP token (encrypt with AES-256-GCM). Keep the HS512 pin in `jwt/`.
- Keep handlers thin; auth data goes through `query.*`, SSO orchestration through `sso/`.
- **An SSE filter key lives in two files — add it to both.** `routes/stream.go` builds the
  subscriber's filter map from a hardcoded allowlist (`service`, `env`, `level`, `name`),
  and `services.matchesFilters` switches on that key and **fails closed** on anything it
  does not recognise. Add a key to the route without adding a case to the switch and that
  filter silently matches nothing; the old fall-through did the opposite and delivered
  *every* event to a subscriber who asked for a subset — no error, no log line. Matching
  nothing is the safe side of that drift, but neither side is correct: keep the two in sync
  and extend `services/hub_test.go`, which asserts the full contract.
- **Keep `/health` returning 200.** It is liveness, the container HEALTHCHECK points at it,
  and a dependency verdict belongs in `/ready` (§5).
- **Never create a table from an `Init()`.** Eight tables in this repo were created by
  ad-hoc `CREATE TABLE IF NOT EXISTS` inside package `Init()` functions with no migration
  file, and six of them had to be moved out of ClickHouse afterwards for exactly the reasons
  §6 *Stores* lists. A schema with no migration file has no history, no review and — as the
  discarded `_ = db.Conn.Exec(ctx, "ALTER TABLE …alert_rules ADD COLUMN … priority")` in the
  old `alerts.Init` showed — no way to report that it failed. MariaDB DDL goes in
  `db/migrations/`, ClickHouse DDL in `migrations/`. The two survivors (`alert_states`,
  `alert_history`) are a recorded exception, not a precedent.
- **Configuration goes in MariaDB, facts go in ClickHouse** (§6 *Stores* has the rule and
  the table). If a human edits the row, or two rows must not collide, it is not a ClickHouse
  table — that engine cannot enforce uniqueness and deduplicates only when a background
  merge happens to run.
- **`condition` is a MariaDB reserved word.** `monitor.alert_rules.condition` is legal bare
  in a SELECT (a word after a period in a qualified name is always an identifier), but any
  UNQUALIFIED use — an INSERT column list, an UPDATE SET clause — must be backticked or the
  statement is a syntax error that only shows up on the write path.
  `TestAlertRuleSQLQuotesReservedWords` pins it.
- **New ClickHouse migration files are written against the literal `monitor.` prefix**, not
  an unqualified table name and not `db.Database` — the runner rewrites that prefix to the
  configured database (§4), and an unqualified name would resolve against the connection
  default instead and defeat the rewrite.
- **`POST /v1/events` takes the env master key or an `ingest`-scope DB key, nothing else.**
  A caller that needs to write events gets an `ingest` key minted for it; do not widen the
  middleware back to any-scope, and never compare a key with `==` (use
  `matchesEnvMasterKey` / `apikeys.ValidateWithScope`).
- Don't touch `Dockerfile`/`docker-compose*.yml`/`.github/workflows/` unless asked. The
  Dockerfile builds the single `./main.go` — do not add a second `package main` file, and
  keep any operator CLI flags inside `main.go` for that reason.
- **A project is derived, never accepted.** Ingest overwrites `Event.Project` from the
  request context for every event, unconditionally, exactly as it does `Event.IssueID`.
  Do not add a "respect a non-empty client value" branch: it reads as a nicety and hands
  any holder of any ingest key the ability to file events under another tenant's project,
  indistinguishable from that tenant's own traffic.
- **Every new read of `monitor.events` goes through a chokepoint** —
  `services.selectEvents`, `services.buildEventWhere`, `routes.selectIssueEvents`, or
  `scope.Matches` for the in-memory path (§6). Never `.Where(project…)` at a call site;
  that is the pattern the chokepoints replaced, and its omissions are invisible.
  `scope/chokepoint_test.go` fails the build on a new unscoped read, and an exemption added
  to it is a claim about a function's *callers* that the AST cannot check — name the callers
  you checked.
- **Every read of `monitor.issues` carries the caller's project too, and nothing automated
  checks that one.** `query.scopeIssues` is the builder-side gate and `routes.requireProject`
  / `routes.requireIssue` are the handler-side ones; the AST audit only walks the ClickHouse
  reads, so a new issue query that forgets the predicate compiles, passes CI and leaks. An
  issue row is not metadata — it carries the error message lifted off the event.
- **`project` must never join `clientStreamFilters`.** It is the tenancy boundary for the
  live tail, taken from the credential and applied after the client's filters. An unscoped
  stream leaves no statement to read back and no audit row — it just delivers other
  projects' errors for as long as the connection is held.
- **Never make `uq_api_keys_key_hash` composite**, however consistent it would look beside
  everything else this phase made per-project (§6 has the full argument).
- **Never `DELETE` a `zones` or `projects` row, and never add a `Slug` field to an update
  request.** Retire with `status='deleted'`; the `UNIQUE` key is what keeps the name spent.
  A recycled slug silently reattaches a month of one owner's events — and a permanent
  rollup — to another, with every reference still syntactically valid.
- **New one-off reconciliation SQL goes in `migrations/manual/`, never in `migrations/`.**
  The runner replays every embedded file on every boot; a mutation there re-queues a
  table-wide rewrite per restart, and an in-comment semicolon is fatal at startup (§8).
- Any new `/v1/*` route → update this file, the `monitor-web` docs if the UI will call it,
  and add/skip a `monitor-mcp` tool. Any change to the auth model, cookies, or schema →
  update §6 here and `monitor-web/AGENTS.md` in the same change.

---

## 10. Verification

```bash
gofmt -w -s .
go build ./...
go vet ./...
go test ./...
```

CI (`.github/workflows/ci.yml`) gates PRs; `build-and-deploy.yml` deploys only on `main`.
If your change touched structure, stack, commands, conventions, endpoints, schema, or a
service contract, update this `AGENTS.md` (and `README.md`) **in the same change**.

---

## 11. Keeping this file updated

Any change that alters structure, stack, commands, conventions, endpoints, schema, or a
service contract MUST update this file in the same change — not as a follow-up. In
particular, any new auth endpoint, cookie, `sso_providers` column, or role must be
reflected in §6, and any new env var in §8. Stale docs mislead every future agent, which
is worse than no docs.
