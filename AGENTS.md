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
- **Dashboards**, **saved views**, and **API-key management** persistence.
- **Identity & authentication** — native accounts (email+password), Monitor-owned
  session tokens, and a pluggable, config-driven SSO subsystem — all owned here
  (see §6 Auth).
- Two Server-Sent-Events (SSE) streams: a live event tail and a live alert feed.

It **does not** own: the UI (that's `monitor-web`) or the client SDKs (`go-monitor`,
`monitor-js`).

---

## 2. Stack & dependencies

- **Language:** Go 1.25 (`go.mod` — `go 1.25.5`).
- **Router:** `github.com/gorilla/mux`.
- **SQL builder:** `github.com/Masterminds/squirrel` (imported as `sq`) — **no ORM**.
- **Datastores — two of them:**
  - **ClickHouse** (`github.com/ClickHouse/clickhouse-go/v2`) — events + analytics.
    The columnar workload needs it. DB `monitor`, accessed via the ClickHouse connection
    in `db/clickhouse.go`.
  - **MariaDB** (`github.com/go-sql-driver/mysql`) — the **relational auth data layer**
    (users, identities, refresh_tokens, sso_providers, sso_sessions, settings, api_keys).
    DB `monitor_auth`, accessed via `db.SQL` (`db/sql.go`). This is the standard-shape
    `db.Queryable` + squirrel-against-`database/sql` stack.
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
  main.go                  # Entry: config, both DB connects + migrations, bootstrap, sso.Install(), router, goroutines, shutdown
  env/env.go               # Env config (getEnv/getEnvInt/getEnvDuration) + RequireProductionSecrets fail-fast guard
  db/
    clickhouse.go          # ClickHouse connection (db.Connect/db.Close) + batch Writer
    sql.go                 # MariaDB connection (db.SQL), db.Queryable, db.RunMigrations (embeds db/migrations/*.sql)
    migrations/            # MariaDB auth-schema DDL (100_users … 107_sso_trust_email_verified)
  jwt/jwt.go               # Monitor-owned HS512 access/refresh JWTs (mint + validate, alg-pinned)
  tools/                   # Password.tool.go (bcrypt 12), Crypto.go (AES-256-GCM), Validate.tool.go (SSRF guard)
  bootstrap/               # First-run seeding: admin.go (first admin user)
  sso/                     # Thin wiring onto go-forta/sso — see §6
  query/                   # MariaDB query layer (squirrel): users, identities, refresh_tokens, sso_providers, sso_sessions, settings, api_keys
  structs/                 # User/Identity/SSOProvider/SSOSession/RefreshToken/APIKey (.struct.go) + event.go + analytics.go
  middleware/
    session.go             # Monitor session auth (Bearer/mon-access-token JWT) + Protected/RequireAdmin/RequireEditor/RejectPending
    csrf.go                # Double-submit CSRF (mon-csrf ↔ X-CSRF-Token); Bearer/X-Api-Key/safe-method exempt
    ingest_auth.go         # X-Api-Key auth for POST /v1/events (admin OR ingest scope)
    query_auth.go          # X-Api-Key (admin) OR a Monitor session for /v1/* reads
    logging.go header.go   # RequestID + logging (SSE-safe); Server header
  responder/responder.go   # Standard JSON envelope helpers
  routes/                  # HTTP handlers (thin) — see routes/AGENTS.md
    HandleLogin/Register/Refresh/Logout/GetSelf/Identities.router.go   # native auth + self + identities
    HandleSSOConfig/Login/Callback.router.go, HandleAdminSSOProviders.router.go, RegisterSSORoutes.go
    cookies.go session.go  # mon-* cookie writing; issueSession (mint + persist refresh-token family)
    events.go query.go analytics.go stream.go api_keys.go dashboards.go views.go issues.go alerts.go
  apikeys/                 # API-key cache (now backed by MariaDB, was ClickHouse)
  alerts/ issues/ dashboards/ views/   # Subsystems (each Init()s from main.go)
  migrations/              # ClickHouse DDL (001_schema, 002_add_user_id, 003_api_keys) + embed.go (in-app runner)
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
```

- **No `.env` is created for you** — set the vars in §6/§8. The server **refuses to
  start** if `MONITOR_API_KEY` is unset, and (in a non-dev profile) if
  `MON_JWT_SIGNING_KEY` / `MON_CRYPTO_KEY` are unset or still the committed dev
  defaults, or `MON_CRYPTO_KEY` isn't exactly 32 bytes (`env.RequireProductionSecrets`,
  called from `main.go`). Set `MON_COOKIE_INSECURE=true` for local dev to permit the
  fallbacks.
- **Both schemas migrate automatically at startup, fail-fast:**
  - **ClickHouse** — `migrations.RunMigrations(ctx)` (`migrations/embed.go`) embeds
    `migrations/*.sql`, runs each in sorted order (all `IF NOT EXISTS`, no tracking table).
  - **MariaDB** — `db.RunMigrations()` (`db/sql.go`) embeds `db/migrations/*.sql`, runs
    each once, tracked in a `migrations_applied` table. The DSN needs `multiStatements`
    (added automatically by `ensureDSNParams`).
- **Bootstrap runs after MariaDB migrations** (`main.go`): `bootstrap.EnsureAdminUser`
  seeds the first admin from `MON_ADMIN_EMAIL`/`MON_ADMIN_PASSWORD` (no-op once any user
  exists). No SSO provider is seeded from env — providers are created through the admin
  API / `/admin/sso`, never from Go code.
- **Unit tests** cover pure logic and the auth layer with a mocked `Queryable`
  (`jwt/jwt_test.go`, `tools/Password_test.go`, `sso/resolve_test.go`,
  `routes/HandleIdentities_test.go`, `db/sql_test.go`, `alerts/…`, `issues/…`,
  `services/query_test.go`). No integration tests (no ClickHouse/MariaDB harness); `dev
  test` runs the unit tests.

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
  caller-controlled `data.*` field names MUST be validated** against `safeIdentifierRegex`
  before interpolation (`services/analytics.go`, `services/query.go`, `alerts/evaluator.go`)
  — `JSONExtractString` takes the key as a SQL string literal, so an unvalidated key is an
  injection vector.
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
  - Two endpoints bypass `responder`: `GET /health` (`{status,enqueued,dropped,pending}`)
    and `POST /v1/events` (`{accepted:<int>}`). Ingest errors are plain text.
- **Subsystem `Init(ctx)`** is the idempotent startup entry point, called once from
  `main.go`. `apikeys.Init` now loads its cache from **MariaDB** (table created by
  `db.RunMigrations`, not by the package).

---

## 6. Domain & architecture

### Auth model (Monitor-owned)

Monitor owns identity end-to-end. Every external identity provider is a row in
`sso_providers` — Google, Okta, Entra, Forta and anything else are indistinguishable
as far as the code is concerned. There is **no provider-specific Go code**; adding an
IdP means filling in the form at `/admin/sso`, not shipping a build.

**Data model (MariaDB `monitor_auth`) — true account linking, one user ↔ many identities:**

| Table | Purpose |
|---|---|
| `users` | The account: `email` (`UNIQUE`), `email_verified`, `name`, `display_name`, `role` (`admin`\|`editor`\|`viewer`\|`pending`), `active`. |
| `identities` | One sign-in method per row, `UNIQUE(provider, provider_user_id)`. `provider="password"` stores a bcrypt hash in `password_hash`; SSO rows store the claim envelope in `identity_data`. **`(provider, provider_user_id)` is the ONLY identity key — never email.** `FK → users ON DELETE CASCADE`. |
| `refresh_tokens` | Rotating-refresh store. Only the SHA-256 `token_hash` (`BINARY(32)`) is persisted; tokens minted from one login share a `family_id`; rotation stamps `replaced_by`. |
| `sso_providers` | One configured IdP per row (see below). |
| `sso_sessions` | One row per SSO-backed user (`user_id` PK) caching the AES-256-GCM-encrypted IdP tokens so the checkpoint can re-introspect the upstream grant. |
| `settings` | Key/value app settings. |
| `api_keys` | Ingest/admin API keys (`scope` = `admin`\|`ingest`). Moved here from ClickHouse. |

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
| `IngestAuthMiddleware` | `POST /v1/events` | env master key (`MONITOR_API_KEY`) **or** any DB key (admin or ingest scope) |
| `QueryAuthMiddleware` | all other `/v1/*` | env master key **or** DB **admin-scope** key **or** a valid Monitor session (`mon-access-token`/Bearer, incl. SSO checkpoint). Ingest keys → `403`. |
| `SessionMiddleware` (`Protected`) | `/auth/*`, `/admin/*` | a valid access JWT (Bearer or `mon-access-token`) → loads the active user into context. `RequireAdmin`/`RequireEditor`/`RejectPending` gate on `role`. |
| `CSRFMiddleware` (global) | all unsafe methods | `mon-csrf` cookie == `X-CSRF-Token` header (constant-time). Safe methods, Bearer clients, `X-Api-Key` clients, and `/auth/{login,register,refresh}` + SSO callbacks are exempt. |

**Endpoint surface** (registered in `main.go` + `RegisterSSORoutes.go`):

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
GET    /admin/sso-providers                 list providers (secrets never returned)  [Protected + RequireAdmin]
POST   /admin/sso-providers                 create a provider                     [Protected + RequireAdmin]
PUT    /admin/sso-providers/{slug}          update a provider                     [Protected + RequireAdmin]
DELETE /admin/sso-providers/{slug}          delete a provider                     [Protected + RequireAdmin]
```

Native flows: `HandleLogin` returns a neutral 401 on every failure (no email
enumeration; missing accounts still pay a dummy bcrypt cost). `HandleRefresh` implements
rotating refresh with **reuse detection** — presenting a spent (rotated) or revoked
token revokes the entire `family_id` and clears cookies (OAuth 2.0 Security BCP §4.14).
`HandleLogout` revokes the family (or all of the user's tokens when the path-scoped
refresh cookie isn't sent). `HandleUpdateSelf` can create a `password` identity for an
SSO-only account.

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
| `sso/sessionstore.go` | `ssolib.SessionStore` over `sso_sessions`, with AES-256-GCM at rest. Returns `(nil, nil)` for a native login, which is what stops the checkpoint denying every password user. |
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

### Request flow — ingestion (unchanged by the auth overhaul)

```
go-monitor / monitor-js SDK
  → POST /v1/events  (NDJSON body, X-Api-Key)         [IngestAuthMiddleware]
  → routes.IngestEventsHandler parses+validates the WHOLE body into a slice first
      (any bad line → 400, nothing enqueued — retry-safe)
  → per event: services.Queue.Enqueue (only Enqueue==true counts toward `accepted`)
  → services.Batcher (goroutine) flushes on BATCH_SIZE or FLUSH_INTERVAL
  → db.Writer batch-inserts into ClickHouse monitor.events
  → (in parallel) publishes to the SSE hub + issues.TrackError for error/fatal
```

### Request flow — query/analytics/UI

```
monitor-web → Next.js proxy (mon-* cookies + X-CSRF-Token) → GET/POST /v1/*  [QueryAuthMiddleware]
  → routes/* handler → services.Query / services.Analytics builds squirrel SQL
  → ClickHouse → responder envelope
```

### Background goroutines (started in `main.go`)

- **Batcher** — drains the queue into ClickHouse.
- **Alert evaluator** — 15s ticker; evaluates every enabled rule against ClickHouse,
  tracks firing/resolved state, records history, publishes to the alert SSE hub, routes
  notifications through policies. Alert types (`threshold`/`absence`/`rate_change`) and the
  pointer-field `UpdateRuleRequest` contract are documented in `alerts/AGENTS.md`.

Both are wrapped in `recover()` and cancelled via the shutdown context.

### External systems

- **ClickHouse** — events/analytics. DB `monitor`, table `events` (30-day TTL).
- **MariaDB** — auth data. DB `monitor_auth` (`mariadb:11.4`, host port 3336 locally).
- **SSO IdPs** — any OIDC or OAuth2 provider configured in `sso_providers` (none by default).
- **Keyring** — optional secret injection at boot (`main.go`); skipped if `KEYRING_*` is
  absent, falling back to plain env vars. Sources `MON_DB_DSN`, `MON_JWT_SIGNING_KEY`,
  `MON_CRYPTO_KEY`, `MON_ADMIN_*`, and each provider's `client_secret_ref` in production.

---

## 7. Ecosystem & related repos

| Repo | Relationship |
|---|---|
| `monitor-web` | The Next.js dashboard. Calls `/v1/*` and `/auth/*` via a server-side proxy that forwards `mon-*` cookies + `X-CSRF-Token`. Its auth model mirrors this repo's — see `monitor-web/AGENTS.md`. |
| `go-monitor` | Go SDK. POSTs NDJSON to `POST /v1/events` with `X-Api-Key`. **Diff against `go-monitor/AGENTS.md` when touching ingestion** — the ingest contract is unchanged by the auth overhaul. |
| `monitor-js` | TypeScript SDK (GitHub only). Same ingestion contract. |
| `monitor-mcp` | MCP server exposing this API to Claude (`mcp__monitor__*`). **House rule: any new `/v1/*` route should add or consciously skip a matching MCP tool in the same change.** The `/auth/*` + `/admin/sso-providers` surface is browser/cookie-oriented and not part of the MCP tool set. |

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
- **Config (env vars, defaults from `env/env.go`):**

  | Var | Default | Notes |
  |---|---|---|
  | `HTTP_PORT` | `8080` | |
  | `CLICKHOUSE_ADDR` / `_DATABASE` / `_USERNAME` / `_PASSWORD` | `localhost:9000` / `monitor` / `default` / `` | events store |
  | `MONITOR_API_KEY` | *(required)* | env master ingest/query key (`X-Api-Key`); refuses to boot without it |
  | `MON_DB_DSN` | `monitor:monitor@tcp(127.0.0.1:3336)/monitor_auth` | MariaDB DSN; from Keyring in prod |
  | `MON_JWT_SIGNING_KEY` | *(dev default)* | HS512 session key; **prod must override** (else fail-fast) |
  | `MON_CRYPTO_KEY` | *(dev default)* | **exactly 32 bytes**, AES-256-GCM key for SSO secrets/tokens; **prod must override** |
  | `MON_COOKIE_DOMAIN` | `` | domain on the `mon-*` cookies |
  | `MON_COOKIE_INSECURE` | `false` | `true` = local dev (drops `Secure`, allows dev-default secrets) |
  | `MON_PUBLIC_URL` | `https://monitor.appleby.cloud` | origin used to build each SSO `redirect_uri` (`{base}/auth/sso/{slug}/callback`) — must match the IdP registration byte-for-byte |
  | `MON_ADMIN_EMAIL` / `MON_ADMIN_PASSWORD` | `` | seed the first admin on a fresh DB; empty = no seed |
  | `MON_ALLOW_REGISTRATION` | `false` | gates `POST /auth/register` |
  | `BATCH_SIZE` / `FLUSH_INTERVAL` / `QUEUE_SIZE` / `MAX_SSE_SUBSCRIBERS` | `1000` / `5s` / `100000` / `100` | ingestion/SSE tuning |

- **Monitoring:** Monitor monitors itself — `mcp__monitor__monitor_service_overview` on
  `monitor-core`; `lattice_get_container_logs` for the container.
- **Common failure modes:**
  - *Refuses to start: "MON_JWT_SIGNING_KEY … dev default" / "MON_CRYPTO_KEY must be
    exactly 32 bytes"* → set real secrets (or `MON_COOKIE_INSECURE=true` for dev).
  - *Startup aborts on ClickHouse/MariaDB migrations* → the store is unreachable or a bad
    `.sql`; both runners are fail-fast.
  - *Login works but SSO fails at the IdP* → `MON_PUBLIC_URL` / the provider's redirect_uri
    don't match, or `client_secret_ref` can't resolve via Keyring/env.
  - *Everyone logged out at once* → the SSO checkpoint fails **open** on IdP blips, so this
    points at the JWT signing key changing or MariaDB being down, not the checkpoint.
  - *Events silently missing under load* → queue overflow (`QUEUE_SIZE`); `/health` reports
    `dropped`.

---

## 9. Rules & guardrails

- Never add an ORM; keep squirrel + native ClickHouse driver + `db.Queryable` for MariaDB.
- **Always validate `data.*` field names** with `safeIdentifierRegex` before putting them
  into ClickHouse SQL. That is the injection boundary.
- **Identity is `(provider, provider_user_id)`, never email.** Do not weaken the
  verified-both-sides link gate in `sso/resolve.go` — it is the pre-account-takeover defense.
- Never persist a raw refresh token (store the SHA-256 hash) or a raw SSO client
  secret/IdP token (encrypt with AES-256-GCM). Keep the HS512 pin in `jwt/`.
- Keep handlers thin; auth data goes through `query.*`, SSO orchestration through `sso/`.
- Don't touch `Dockerfile`/`docker-compose*.yml`/`.github/workflows/` unless asked. The
  Dockerfile builds the single `./main.go` — do not add a second `package main` file, and
  keep any operator CLI flags inside `main.go` for that reason.
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
