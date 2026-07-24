# AGENTS.md — routes/ (monitor-core)

HTTP handlers. Handlers are **thin**: parse the request, call a service/subsystem
function, hand the result to `responder`. Business logic lives in `services/` and the
subsystem packages, not here. Read the root `../AGENTS.md` first.

## Files → routes

| File | Endpoints |
|---|---|
| `events.go` | `POST /v1/events` (ingest), `GET /health` |
| `query.go` | `GET /v1/events`, `/v1/labels/{label}/values`, `/v1/data/keys`, `/v1/data/values` |
| `analytics.go` | `POST/GET /v1/analytics`, `POST/GET /v1/timeseries`, `POST /v1/topn`, `/v1/gauge`, `/v1/compare` |
| `stream.go` | `GET /v1/events/stream` (SSE) |
| `api_keys.go` | `GET/POST /v1/api-keys`, `DELETE /v1/api-keys/{id}` |
| `dashboards.go` | `GET/POST /v1/dashboards`, `GET/PUT/DELETE /v1/dashboards/{id}` |
| `views.go` | `GET/POST /v1/views`, `DELETE /v1/views/{id}` |
| `alerts.go` | alert-rules, alert-history, notification-channels, service-groups, notification-policies CRUD + `/v1/alerts/stream` (SSE) |
| `issues.go` | `GET /v1/issues`, `GET/PUT /v1/issues/{id}`, `GET /v1/issues/{id}/events` |
| `HandleLogin/Register/Refresh/Logout.router.go` | `POST /auth/{login,register,refresh,logout}` (native session auth) |
| `HandleGetSelf.router.go` | `GET/PUT /auth/self` (current user + set/change password) |
| `HandleIdentities.router.go` | `GET /auth/self/identities`, `POST/DELETE /auth/self/identities/{slug}` (link/unlink) |
| `HandleSSOConfig/Login/Callback.router.go` | `GET /auth/sso/config`, `/auth/sso/{slug}/login`, `/auth/sso/{slug}/callback` |
| `HandleAdminSSOProviders.router.go` (mounted by `RegisterSSORoutes.go`) | `GET/POST /admin/sso-providers`, `PUT/DELETE /admin/sso-providers/{slug}` |
| `cookies.go` / `session.go` | not routes — `mon-*` cookie writing + `issueSession` (mint tokens, persist the refresh-token family) |

> `self.go` (the old Forta-protected `GET /self`) has been **deleted** — native
> `GET /auth/self` replaces it. There is no `/self` or `/forta/*` route anymore.

The authoritative route table (with methods, auth, and handler names) is `../main.go`
(plus `RegisterSSORoutes.go` for the SSO/admin subrouter) — those are the single source
of registration. Auth details (cookies, JWT, SSO flow, roles) live in `../AGENTS.md` §6.

## Conventions

- **Response envelope:** use `responder.*` (success/error/withcount). Do **not**
  hand-roll JSON except the two intentional exceptions: `/health` and `POST /v1/events`
  return bare JSON, and ingest errors use `http.Error` (plain text). See root §5.
- **Auth is applied by middleware, not handlers** — `IngestAuthMiddleware` on ingest,
  `QueryAuthMiddleware` on the `/v1` subrouter, and `SessionMiddleware` (`middleware.Protected`,
  plus `RequireAdmin`/`RequireEditor`) on `/auth/*` and `/admin/*`. The global
  `CSRFMiddleware` guards unsafe cookie-authenticated requests (Bearer/`X-Api-Key`/safe
  methods and the login/register/refresh/SSO-callback paths are exempt). Handlers read the
  authenticated user via `middleware.GetUserFromContext`.
- **Package-level dependencies are injected from `main.go`** via exported vars:
  `routes.Queue`, `routes.EventHub`, `routes.AlertNotifHub`. Handlers use these
  globals; they are set once at startup.
- **Pagination:** events + issues return a `pagination` block. Events prebuild
  `next`/`previous` URLs; issues return empty next/prev (client pages via limit/offset).

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

## Known issues & gaps

**Resolved 2026-07-23:** SSE 500s (middleware Flush/Unwrap + write-deadline clear),
the NDJSON partial-commit + `accepted` miscount (`events.go` now parses the whole body
first via `parseEvents`, then enqueues, counting only `Enqueue==true`), and the
unbounded `go issues.TrackError` (now a bounded worker pool — `issues.TrackError(event)`
is a non-blocking enqueue). Query handlers return **400** on a bad filter column /
`data.*` key via `isFilterValidationError` (`query.go`).

## Verification

`gofmt -w -s . && go build ./... && go vet ./... && go test ./...` from repo root.
