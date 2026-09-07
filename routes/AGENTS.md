# AGENTS.md — routes/ (monitor-core)

HTTP handlers. Handlers are **thin**: parse the request, call a query/service/subsystem
function, hand the result to `responder`. Business logic lives in `services/` and the
subsystem packages, not here. Read the root `../AGENTS.md` first.

**A handler calls `query.*` directly unless the operation genuinely needs orchestration.**
Since migrations 119-124 that is the shape for dashboards, saved views and most of the
alerting surface; the surviving `alerts.*` calls are the three that touch MariaDB and
ClickHouse together. A package that only forwards a handler to one SQL statement is the
service layer the repo's rules say not to build.

## Files → routes

| File | Endpoints |
|---|---|
| `events.go` | `POST /v1/events` (ingest), `GET /health` (liveness) |
| `health.go` | `GET /ready` (readiness) + the shared ClickHouse/MariaDB pings |
| `query.go` | `GET /v1/events`, `/v1/labels/{label}/values`, `/v1/data/keys`, `/v1/data/values` |
| `analytics.go` | `POST/GET /v1/analytics`, `POST/GET /v1/timeseries`, `POST /v1/topn`, `/v1/gauge`, `/v1/compare` |
| `stream.go` | `GET /v1/events/stream` (SSE) — subscriber filters come from a hardcoded `service`/`env`/`level`/`name` allowlist |
| `api_keys.go` | `GET/POST /v1/api-keys`, `DELETE /v1/api-keys/{id}` — `POST` takes an optional `project_slug`, defaulting to `MON_DEFAULT_PROJECT`; the response carries `project_id`/`project_slug` |
| `dashboards.go` | `GET/POST /v1/dashboards`, `GET/PUT/DELETE /v1/dashboards/{id}` — straight to `query.*` since migration 123 moved the table to MariaDB; the `dashboards` package is now empty |
| `views.go` | `GET/POST /v1/views`, `DELETE /v1/views/{id}` — straight to `query.*` since migration 124; the `views` package is now empty |
| `alerts.go` | alert-rules, alert-history, notification-channels, service-groups, notification-policies CRUD + `/v1/alerts/stream` (SSE). **Two layers on purpose:** `query.*` for the pure-MariaDB CRUD (migrations 119-122), `alerts.*` only for the operations that span both stores — `ListRules`/`GetRule`/`DeleteRule`, which join or remove a MariaDB rule alongside its ClickHouse `alert_states` row, plus `ListHistory`, which is ClickHouse-only |
| `issues.go` | `GET /v1/issues`, `GET/PUT /v1/issues/{id}`, `GET /v1/issues/{id}/events` + `requireProject` |
| `issue_timeline.go` | `GET /v1/issues/{id}/timeline`, `/history`, the three `comments` verbs and the three `links` verbs — all behind `requireIssue` |
| `service_repos.go` | `GET /v1/service-repos`, `GET/PUT/DELETE /v1/service-repos/{service}` |
| `HandleListZones.router.go` | `GET /v1/zones` (active zones) + `registryListPage`, the limit/offset parser both registry routes share |
| `HandleListProjects.router.go` | `GET /v1/zones/{zone}/projects` (active projects in one zone) — what the project switcher populates from |
| `HandleLogin/Register/Refresh/Logout.router.go` | `POST /auth/{login,register,refresh,logout}` (native session auth) |
| `HandleGetSelf.router.go` | `GET/PUT /auth/self` (current user + set/change password) |
| `HandleIdentities.router.go` | `GET /auth/self/identities`, `POST/DELETE /auth/self/identities/{slug}` (link/unlink) |
| `HandleSSOConfig/Login/Callback.router.go` | `GET /auth/sso/config`, `/auth/sso/{slug}/login`, `/auth/sso/{slug}/callback` |
| `HandleAdminSSOProviders.router.go` (mounted by `RegisterSSORoutes.go`) | `GET/POST /admin/sso-providers`, `PUT/DELETE /admin/sso-providers/{slug}` |
| `cookies.go` / `session.go` | not routes — `mon-*` cookie writing + `issueSession` (mint tokens, persist the refresh-token family) |

> There is no bare `GET /self` — `GET /auth/self` is the current-user route.

The authoritative route table (with methods, auth, and handler names) is `../main.go`
(plus `RegisterSSORoutes.go` for the SSO/admin subrouter) — those are the single source
of registration. Auth details (cookies, JWT, SSO flow, roles) live in `../AGENTS.md` §6.

## Conventions

- **Response envelope:** use `responder.*` (success/error/withcount). Do **not**
  hand-roll JSON except the three intentional exceptions: `/health`, `/ready` and
  `POST /v1/events` return bare JSON, and ingest errors use `http.Error` (plain text).
  See root §5.
- **Liveness vs readiness:** `/health` (`events.go`) always returns **200 / `status:"ok"`**
  — the container HEALTHCHECK points at it, and killing the process does not fix a dead
  ClickHouse. It reports `clickhouse_ok`, `mariadb_ok`, `last_flush_at` and `role` as
  diagnostics only, additively after the frozen `status`/`enqueued`/`dropped`/`pending` keys
  that monitor-web reads. `/ready` (`health.go`) is the one that returns **503**, naming the
  down store in `failing`. Do not make `/health` fail on a dependency.
- **Both probes are role-aware, in opposite directions.** `/health` nil-checks `Queue`
  before `Stats()` and reports zeroes when it is nil — an app process runs no queue, and a
  panic here would restart a container that is working. `/ready` requires ClickHouse for
  every role **except exactly `app`** (`env.MonRole != env.RoleApp`, not
  `!RunsDataPlane()`), so it fails closed: an unset or future role keeps the event store a
  hard dependency, because being wrongly un-ready costs a routing decision while being
  wrongly ready hands traffic to a replica that drops what it accepts. Both report `role`,
  since the boot log line that also states it scrolls away and these can be curled.
- **Never call `pingDependencies` from a handler — use `cachedPingDependencies`.** Both
  routes are unauthenticated and unthrottled, and a raw ping costs one ClickHouse
  connection (from the pool the batcher writes through) plus one MariaDB connection per
  request. The 5s memo in `health.go` is the only thing stopping a poll loop from starving
  ingest; `health_test.go` asserts it, including under `-race`.
- **Auth is applied by middleware, not handlers** — `IngestAuthMiddleware` on ingest,
  `QueryAuthMiddleware` on the `/v1` subrouter, and `SessionMiddleware` (`middleware.Protected`,
  plus `RequireAdmin`/`RequireEditor`) on `/auth/*` and `/admin/*`. The global
  `CSRFMiddleware` guards unsafe cookie-authenticated requests (Bearer/`X-Api-Key`/safe
  methods and the login/register/refresh/SSO-callback paths are exempt). Handlers read the
  authenticated user via `middleware.GetUserFromContext`.
- **Package-level dependencies are injected from `main.go`** via exported vars:
  `routes.Queue`, `routes.Batcher`, `routes.EventHub`, `routes.AlertNotifHub`. Handlers use these
  globals; they are set once at startup — **and only on the data plane**. All four are nil
  under `MON_ROLE=app`, which constructs none of them. That is safe only because
  `buildRouter` registers no route that reads them in that role; the exception is `/health`,
  which exists in every role and therefore checks. A new global of this kind must be
  paired with either a data-plane route gate or a nil-tolerant reader — "main.go sets it" is
  not true in every role, and a nil interface method call is answered by net/http closing
  the connection with no response at all, which debugs like a network fault rather than a
  bug.
- **Pagination:** events + issues return a `pagination` block. Events prebuild
  `next`/`previous` URLs; issues return empty next/prev (client pages via limit/offset).

## Which of these routes a process registers

**Registration is not in this package.** `buildRouter(role)` in the repo root owns the whole
HTTP surface and decides, per `MON_ROLE`, which handlers here are reachable: `/auth/*` and
the SSO set are control plane, ingest/query/analytics/dashboards/views/alerting/issues and
`POST /webhooks/github` are data plane, and `/health`, `/ready`, the zone/project reads,
`/v1/service-repos` and `/v1/api-keys` are registered in every role. Under the default
`MON_ROLE=both` every route in the tables above is registered, in the order it always was —
`router_test.go` pins that inventory, and pins that `both` is exactly the union of the two
planes, so a route cannot silently fall out of the gating and 404 only in a split deploy.
The full per-subsystem breakdown, including why the three MariaDB-only groups stay in both
roles, is root `../AGENTS.md` §6 *Roles*.

Two consequences for work in this package:

- **A gated route does not degrade, it disappears.** In a role that does not register it the
  request 404s. That is deliberate — the alternative for a data-plane handler in an app
  process is a nil `db.Conn` and a panic — but it means "this endpoint 404s" has a second
  possible cause now, and the first thing to check is the `role` on `/health`.
- **Adding a route means choosing its plane.** Ask what the handler *touches*, then what the
  data it serves *belongs to* — the second question is the one that decides. `POST
  /webhooks/github` writes only MariaDB and would run anywhere, but everything it writes
  belongs to the zone-owned issue tracker, so on an app process it would find no issues,
  write nothing, and still answer 200 — retiring a delivery GitHub would otherwise retry.

## SSE handler pattern (and the current breakage)

Both `stream.go` and the alert stream in `alerts.go` do:

```go
flusher, ok := w.(http.Flusher)
if !ok { http.Error(w, "streaming not supported", 500); return }
// set text/event-stream headers, then loop: write "data: …\n\n"; flusher.Flush()
```

This assertion now **succeeds**: `middleware/logging.go`'s `loggingResponseWriter`
forwards `Flush`/`Hijack`/`Unwrap`. After setting the SSE headers, each handler also
clears its write deadline (`http.NewResponseController(w).SetWriteDeadline(time.Time{})`)
so the global `WriteTimeout: 30s` (kept for all other routes) doesn't sever the stream.

⚠️ **`stream.go`'s filter allowlist and `services.matchesFilters` are one contract in two
files.** `clientStreamFilters` holds the four keys a subscriber may choose — `service`,
`env`, `level`, `name` — and the hub's switch understands those plus `project`, **returning
false for anything else**, so a key added here but not there silently matches nothing. (It
used to be the opposite — an unhandled key fell through to "match everything", which is why
the hub now fails closed.) Add a key to both, and to `services/hub_test.go`. See
`services/AGENTS.md`.

⚠️ **`project` is not a client filter and must never be added to `clientStreamFilters`.**
`subscriptionFilters` takes it from the authenticated credential and applies it *after* the
client's filters, and returns `false` — a 500, no subscription — when no project resolves.
An unscoped stream leaves no statement to read back and no audit row; it just delivers other
projects' errors for as long as the connection is held.

## Project-scoped event reads

`issues.go` hand-writes SQL against `monitor.events` in two places — the indexed
`queryEventsByIssueID` and the legacy pre-004 fallback scan — and those bypass every
allowlist and every squirrel builder in `services/`. Both now go through
**`selectIssueEvents(ctx, where, args…)`**, the only expression in the file that names the
table, which prepends `scope.ProjectPredicate(ctx)` and its argument. It **returns an error**
rather than an unscoped query when the context carries no project.

**An `issue_id` match is not a tenancy boundary on its own, even now that fingerprints
begin with the project.** Migration 118 made every *newly minted* id project-specific, but
ids minted before it were computed from `service + name + path` alone, and events stamped
with one of those are still inside the 30-day retention window — so a legacy id genuinely
can span several projects' rows. The predicate is what makes that safe, and it stays
regardless: an id read off a URL, an alert payload or a comment must not be a way to select
events, and "the key happens to be narrow enough" is not a boundary anyone can check at the
call site. In the legacy fallback scan the project is deliberately **absent** from the
`service`/`name`/`path` pre-filter for the same reason — `selectIssueEvents` has already
applied it, and restating it would be a second copy to keep in step, while the recomputed
fingerprint checks it exactly rather than by policy.

Nothing else in the file may build a query against `monitor.events` —
`scope/chokepoint_test.go` fails the build if something does.

## The tenancy registry routes

`GET /v1/zones` and `GET /v1/zones/{zone}/projects` are **reads only**, available to any
authenticated session. There is deliberately no create/update/delete counterpart in this
phase: the registry is seeded by `bootstrap.EnsureZoneAndProject` and managed out of band.
Slugs are immutable and never reusable, so a mistyped project minted through a REST call
could only ever be retired, never corrected — that belongs to an operator with a migration,
not to a form.

- **The zone is a path segment, the project is not.** A project slug is unique only *within*
  its zone, so listing projects by slug alone would return the right rows today (one zone)
  and another zone's rows later. This mirrors `query.GetProjectBySlug`, which takes a zone id
  for the same reason.
- **Phase 1 is single-zone, so `GET /v1/zones` returns exactly one row.** That is the correct
  output, not a bug — a switcher showing one entry is what a single-zone install looks like.
- **Retired zones still resolve, retired projects do not appear.** `query.ListZones` filters
  the *listing* to active rows because that is what an operator picks from, while
  `HandleListProjects` accepts a retired zone so a bookmark into one can still render its
  switcher and move the user somewhere live.
- **The projects listing carries `default_project_slug` alongside the rows.** It names which
  project an unset `?project` resolves to (`env.DefaultProjectSlug`), and the switcher needs
  it to avoid rendering that tenant TWICE — once as the "nothing selected" row and once under
  its own name, two URLs for one thing with two checked states that cannot both be right. It
  is a sibling field rather than a flag on the row because it is a property of the *install*,
  not of the project: the same row stops being the default when the env var changes.
- **An out-of-range `limit` is REFUSED, not clamped.** `query.ListZones` silently rewrites
  anything outside `(0, MAX_LIMIT]` to `DEFAULT_LIMIT`, so a caller asking for 1000 would get
  50 with no error — reintroducing the exact truncation cliff the `MAX_LIMIT` default below
  exists to avoid, against the caller who was most explicit about not wanting it.
- **An absent `limit` asks for `db.MAX_LIMIT`, not the house default of 50.** `registryListPage`
  does this on purpose: these routes populate the switcher, and a 51st project that silently
  failed to appear is a tenant the operator cannot reach, with nothing logged and no symptom
  pointing at pagination. An explicit `limit`/`offset` is still honoured.
- ⚠️ **Never send `?project=` to either route.** They sit on the `/v1` subrouter and therefore
  run through `QueryAuthMiddleware`, so a stale selection would refuse the very request a
  client needs in order to discover a valid one. Answering the registry without a selection is
  what keeps a bad selection recoverable.

## Project-scoped issue reads

The issue tables are MariaDB, so nothing above touches them: `scope.ProjectPredicate` builds
ClickHouse SQL and the AST audit does not look at `query/`. Two helpers in this package are
the HTTP half of the separate mechanism (`query.scopeIssues`) that covers them:

- **`requireProject` (`issues.go`)** — reads `scope.GetProject` and, finding none, answers
  **500** and reports false. It is a 500 rather than a 401/403 on purpose: every `/v1` route
  sits behind `QueryAuthMiddleware`, which injects a project on all three accepted branches,
  so reaching that line means a route was registered outside it — a wiring fault in
  `main.go`, not something the credential did. `HandleListIssues` stamps the resolved project
  onto the request struct **after** parsing the query string, so it overwrites rather than
  competes with anything a caller could have sent — the same ordering, for the same reason,
  as `subscriptionFilters` applying the project after the subscriber's own filters.
- **`requireIssue` (`issue_timeline.go`)** — resolves `{id}` to an issue **in the caller's
  project** and 404s otherwise, indistinguishably from an id that never existed. It gates the
  timeline, the occurrence history, all three comment verbs and all three link verbs, so it
  is one gate covering eight endpoints. Add a ninth `/v1/issues/{id}/…` route and it goes
  through `requireIssue` too — resolving the id any other way reintroduces the leak for that
  route alone.

## Known issues & gaps

**Resolved 2026-07-23:** SSE 500s (middleware Flush/Unwrap + write-deadline clear),
the NDJSON partial-commit + `accepted` miscount (`events.go` now parses the whole body
first via `parseEvents`, then enqueues, counting only `Enqueue==true`), and the
unbounded `go issues.TrackError` (now a bounded worker pool — `issues.TrackError(event)`
is a non-blocking enqueue). Query handlers return **400** on a bad filter column /
`data.*` key via `isFilterValidationError` (`query.go`).

## Verification

`gofmt -w -s . && go build ./... && go vet ./... && go test ./...` from repo root.
