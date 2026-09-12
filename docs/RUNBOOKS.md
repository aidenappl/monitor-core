# Operator runbooks

Procedures that touch live data or the registry. Each one states its
preconditions, what to expect, and how to tell it worked — because every one of
them is either irreversible or hard to unpick.

Written after standing up the second zone (`appleby`) took hours, almost
entirely on knowledge that existed only in one person's head.

---

## 1. Register a zone's project in the control-plane registry

**Precondition:** the zone-scoped API-key fix must be **deployed** first.

Until it is, `query.DeleteAPIKey` scopes by `projects.slug` alone. A slug is
unique only per zone (`uq_projects_zone_slug`, migration 116), so the moment a
second zone owns a project called `default`, its uncorrelated scalar subquery
returns two rows and MariaDB raises `ER_SUBSELECT_NO_1_ROW` (1242) — surfaced as
a bare 500 on **every** API-key deletion in the install. Registering a second
`default` is exactly what triggers it.

Confirm the fix is live before proceeding:

```bash
curl -s https://api.monitor.appleby.cloud/version | jq '{commit, schema}'
```

Then create the project through the control plane's admin API (or Admin →
Registry in the dashboard). Verify:

```bash
# Should list the project you just created.
curl -s -H "Cookie: $MON_SESSION" \
  https://api.monitor.appleby.cloud/v1/zones/appleby/projects | jq
```

⚠️ **Creating it centrally does not create it in the zone.** The two planes keep
independent `projects` tables and nothing reconciles them, so the zone's own
`registry.HasActiveProject` will still answer false and `apikeys.resolveProject`
will still refuse to mint a key for it. Until server-side federation lands, a
project that a zone must actually honour has to be created **on that zone**.
This is the single largest remaining gap.

---

## 2. Backfill `events.project`

**What is wrong:** 1,694,424 rows carry `project = ''` against ~14k stamped.
Live ingest is correct — the crossover was mid-day 2026-09-07 — but the
historical rows are reachable only through the transition arm in
`scope/scope.go`, which ORs `project = ''` in when the selection equals the
install default. **The first non-default project therefore sees none of the last
month's history.**

**Precondition:** ship the ingest-side refusal of an empty project first. Without
it a future bug can recreate `''` rows and the arm silently re-absorbs them, so
the arm could never be removed and this backfill would need doing again.

**Before you start**, record the counts so you can prove rows were relabelled
rather than lost:

```sql
SELECT project, count() FROM monitor.events GROUP BY project;
SELECT partition, sum(rows) FROM system.parts
 WHERE database='monitor' AND table='events' AND active
 GROUP BY partition ORDER BY partition DESC;
```

**Run it one partition at a time, newest first**, stopping at the TTL horizon
(30 days — older partitions expire anyway):

```sql
ALTER TABLE monitor.events UPDATE project = 'default'
  IN PARTITION '20260907' WHERE project = '';
```

Wait for each to finish before the next:

```sql
SELECT mutation_id, parts_to_do, is_done, latest_fail_reason
  FROM system.mutations
 WHERE database='monitor' AND table='events' AND is_done = 0;
```

Per-partition is for **observability and abortability**, not load — 1.7M rows is
small for ClickHouse. It is resumable, `parts_to_do` reads as progress per day
rather than one opaque number, `KILL MUTATION` stops it cleanly, and no single
long mutation queues in front of every later merge. The `WHERE project = ''`
guard makes each statement idempotent and interruption-safe.

⚠️ **`'default'` is the only correct value.** `issues.fingerprintProject` already
folds `''` → the default project, so every issue derived from these rows is
permanently keyed under it. Any other slug orphans them.

**Verification — the count is not the proof.** Run the same `/v1/analytics` and
`/v1/timeseries` queries as the default project before and after and assert the
numbers are *unchanged*. The transition arm already made them equal, so equality
afterwards is what proves rows were relabelled rather than moved out from under
the default project. Then:

1. `SELECT count() FROM monitor.events WHERE project = ''` → 0
2. The per-project sum equals the recorded before-sum plus arrivals
3. No mutation with `is_done = 0` or a non-empty `latest_fail_reason`
4. Repeat (1) on three consecutive days, to catch a writer still emitting `''`

**Only then** remove the `OR project = ''` arm — in **one commit** across
`scope.ProjectPredicate` **and** `scope.Matches`, which are two encodings of one
rule and must not drift. Note the date in that comment is wrong twice over: it
says 2026-10-06 on a "30 days after 006" rule, but the ingest crossover was
2026-09-07, and it is predicated on this backfill having run, which it had not.

---

## 3. Trailblaze's empty registry row

**Blocked — do not attempt yet.**

Zone `trailblaze` has `ingest_url = ''` and `query_url = ''`, so the control
plane has no address for its own zone, and `bootstrap.ensureZone` returns early
for an existing row so it can never self-heal.

It **cannot** simply be typed in: `tools.ValidateExternalURL` refuses any host
resolving to a private address, and `api.monitor.appleby.cloud` is
`10.15.10.71`. `appleby` validates only because it happens to sit behind
Cloudflare. **A zone on an internal address is unregisterable by construction** —
which is a normal deployment, and is the shape of the control plane's own zone.

This needs the trusted-internal opt-in from the federation phase. Until then
`MON_LOCAL_ZONE` in monitor-web is what keeps trailblaze working, and its
`query_url` being blank is *expected* rather than broken.

---

## 4. Standing up a new zone

Every step below is required. Ones marked ⚠️ were undocumented and are what made
the last one take hours.

1. **Choose the slug.** Immutable and never reusable — retirement spends the name
   forever. It must not collide with a frontend route.
2. **Provision** the stack: `monitor-core` + MariaDB + ClickHouse. ⚠️ The
   committed `docker-compose.yml` hardcodes container names and host ports and
   cannot host a second zone as written.
3. **DNS + a real public TLS certificate.** ⚠️ Forced by
   `tools.ValidateExternalURL`: https only, and refused if the host resolves to a
   loopback/RFC1918/link-local address (see runbook 3).
4. **Mint Keyring secrets scoped to this zone only.** ⚠️ Keyring's `InjectEnv`
   runs *before* `env.Load()`, so its values **override** the container's rather
   than filling gaps. A broadly-granted token will silently point this process at
   another zone's stores. Preflight now names every variable Keyring overrode.
5. **Set the environment** — see `.env.example`, which is now complete and
   CI-pinned. Required: `MON_ROLE=zone`, `MON_ZONE_SLUG`, `MON_PUBLIC_URL`,
   `MON_DB_DSN`, `MONITOR_API_KEY`, `MON_JWT_SIGNING_KEY`, `MON_CRYPTO_KEY`.
   ⚠️ `MON_DB_DSN` must be a **fully resolved literal** — Lattice substitutes
   `${VAR}` only as a whole value, never mid-string. Preflight now fails the boot
   on a leftover `${`.
   ⚠️ `MON_JWT_SIGNING_KEY` must be **byte-identical** to the control plane's, or
   every cross-zone dashboard read 401-loops.
6. ⚠️ **Grant the schema**, as root, before the first boot — migration 110 does
   `CREATE DATABASE monitor` but the image grants the app user rights only on
   `MARIADB_DATABASE`:
   ```sql
   GRANT ALL PRIVILEGES ON monitor.* TO 'monitor'@'%';
   FLUSH PRIVILEGES;
   ```
7. **Boot and read the log.** Expect the role line, the zone line *without* the
   "this is the fallback, not a choice" warning, `✅ N migration(s) applied` from
   both runners, an install-id line on a fresh database, and **no preflight
   findings**.
8. **Check identity:**
   ```bash
   curl -s https://<zone>/health  | jq '{zone, role, clickhouse_ok, mariadb_ok}'
   curl -s https://<zone>/version | jq '{commit, install_id, schema}'
   ```
   The `install_id` must differ from every other zone's. Two zones reporting the
   same one means two registry rows resolve to the same database.
9. **Register it** in the control plane (Admin → Registry), then **Verify** —
   expect `healthy` with `reported_zone` matching the slug.
10. ⚠️ **Mint the zone's first ingest key on the zone itself.** Never copy
    `MONITOR_API_KEY` between zones: it grants full ingest *and* query on the
    default project of whichever zone it is presented to, so one copied value is
    one credential over two tenants. A key minted on the control plane binds to a
    control-plane project.
11. **Point one service at it** and confirm the event lands in the right project
    and the issue is keyed under it.

**Retirement:** `POST /admin/zones/{id}/retire`, which requires zero active
projects. The slug is spent permanently.
