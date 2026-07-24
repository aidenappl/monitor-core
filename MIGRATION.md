# Forta → Native-Accounts Migration (zero lockout)

Monitor moved from "100% Forta identity" to **native accounts with linked SSO**.
Forta is now just one SSO provider row. Without a migration, an existing Forta
user's first post-cutover "Continue with Forta" would auto-provision them as a
brand-new `pending` account and lock them out of their real access.

This migration removes that risk in two additive, idempotent steps:

1. **Seed the Forta provider row** so the "Continue with Forta" button exists.
2. **Backfill shadow shells** — pre-create an ACTIVE `users` row + a `forta`
   `identities` row (keyed on the Forta subject) for every existing Forta user,
   so the SSO callback resolves each returning user to their real account with
   their real role instead of provisioning a `pending` one.

Everything is reversible and re-runnable. Keep Forta enabled indefinitely.

---

## Operator steps

### 1. Configure Forta as an SSO provider

Set these env vars (Keyring in production) before deploy. If they are unset the
Forta row is simply not seeded (no Forta button) and startup is unaffected.

| Variable | Meaning |
|----------|---------|
| `MON_SSO_FORTA_AUTHORIZE_URL` | Forta OAuth2 authorize endpoint |
| `MON_SSO_FORTA_TOKEN_URL`     | Forta token endpoint |
| `MON_SSO_FORTA_USERINFO_URL`  | Forta userinfo endpoint |
| `MON_SSO_FORTA_INTROSPECT_URL`| (optional) Forta introspection endpoint — used by the SSO revocation checkpoint |
| `MON_SSO_FORTA_CLIENT_ID`     | Monitor's Forta OAuth client id |
| `MON_SSO_FORTA_SCOPES`        | (optional) defaults to `openid email profile` |

Create the **Keyring secret** `MON_SSO_FORTA_CLIENT_SECRET` (the Forta client
secret). The seeded provider row stores only the *reference* to this name
(`client_secret_ref`), never the raw value — it is resolved at login time.

### 2. Deploy

On startup `bootstrap.EnsureFortaProvider` seeds the `forta` row into
`sso_providers` (kind=`oauth2`, `trust_email_verified=1`, `allow_auto_link=1`,
`auto_provision=1`, `enabled=1`). It is a **no-op** if a `forta` row already
exists (operator edits via the admin API are never clobbered).

### 3. Export existing Forta users → JSON

Export the users who have a grant on the Monitor platform in Forta and write a
JSON array. Map each user's Forta grant/role to a Monitor role
(`admin` | `editor` | `viewer`):

```json
[
  { "subject": "forta-user-id-1", "email": "ada@appleby.cloud",  "email_verified": true, "name": "Ada Lovelace",  "role": "admin"  },
  { "subject": "forta-user-id-2", "email": "grace@appleby.cloud", "email_verified": true, "name": "Grace Hopper", "role": "editor" },
  { "subject": "forta-user-id-3", "email": "alan@appleby.cloud",  "email_verified": true, "name": null,          "role": "viewer" }
]
```

Field notes:

- **`subject`** — the Forta subject/user id. This is the identity key
  (`(forta, subject)`), **never** the email. It must match the `sub`/`id` Forta
  returns from its userinfo endpoint so the callback resolves cleanly.
- **`email`** — the user's Forta email (lower-cased on ingest; a linking hint).
- **`email_verified`** — normally `true` (Forta verifies emails upstream).
- **`name`** — optional display name (`null` allowed).
- **`role`** — `admin` | `editor` | `viewer`. `pending` and unknown values are
  rejected (the whole point is to restore ACTIVE access with a real role).

### 4. Run the backfill once

The backfill runs as a flag on the same binary (no separate image — keeps the
single-file `./main.go` build intact). It connects to MariaDB, applies
migrations, seeds the Forta row, runs the backfill, prints counts, and exits
**without** starting the HTTP server:

```bash
# in the monitor-core container / environment (same env as normal startup)
./monitor-core -backfill-forta /path/to/forta-users.json
```

Output:

```
forta backfill: processing N user(s) from /path/to/forta-users.json
forta backfill: created=N linked=0 skipped=0
forta backfill: done
```

Per user, in its own transaction:

- **created** — no account existed for the email → new ACTIVE `users` row (with
  the given role + `email_verified`) plus its `forta` identity.
- **linked** — a `users` row already existed for the email → a `forta` identity
  was attached to it.
- **skipped** — a `(forta, subject)` identity already existed → nothing to do.

It is **idempotent** — re-running the same file reports every user as `skipped`.
A per-user error aborts only that user's transaction and returns the counts so
far; fix the offending row and re-run.

### 5. Verify

- Have an existing Forta user click **"Continue with Forta"**. They should land
  on their real role (e.g. `admin`/`editor`/`viewer`), **not** `pending`, and
  get a `mon-*` session — no re-registration.
- Confirm in MariaDB that their `users.role` and a `forta` `identities` row keyed
  on their subject exist.
- API-key ingestion (`X-Api-Key`) is unaffected by any of this.
