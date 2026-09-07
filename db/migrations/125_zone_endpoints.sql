-- Records WHERE a zone is, and what the last look at it found.
--
-- 116 created zones as slug + display_name + status, which is enough to NAME a
-- tenant and not enough to reach one. That was correct for Phase 1 — a
-- single-zone install has exactly one place its data can be, and it is the
-- process reading this table — and it stops being correct the moment a second
-- zone exists, because at that point the control plane has to answer "which box
-- do I ask?" and the registry is the only thing that could know.
--
--
-- A ZONE ROW DOES NOT CREATE A ZONE. Read this before adding anything here.
--
-- Everything below is a RECORD of infrastructure that already exists: a Lattice
-- stack, its own ClickHouse, its own MariaDB, a DNS record and a TLS
-- certificate, all provisioned by hand. Writing a row provisions nothing, and
-- nothing downstream reconciles a row against reality. So the failure this
-- migration is shaped around is not "the URL is missing" — that one is loud, the
-- probe reports it and nothing works. It is "the URL is syntactically perfect
-- and points at the WRONG BOX": the dashboard then renders one zone's data under
-- another zone's name, every reference involved stays valid, nothing errors and
-- nothing logs. That is the same class of silent mis-attribution the slug
-- non-reuse rule in 116 exists to prevent, arriving from the other direction —
-- 116 stops a name being reattached to the wrong data, this stops a URL being.
--
-- The columns are therefore in two groups, and the second group exists only
-- because of the paragraph above: reported_zone is what the box at the other end
-- said it was, kept beside the slug this registry expected, so "reachable" and
-- "reachable AND the right zone" can never collapse into one another.
--
--
-- WHY TWO URLs AND NOT ONE.
--
-- They are two different network paths with two different audiences, and a
-- single column would force one of them to be wrong.
--
--   ingest_url is PUBLIC and CLIENT-FACING. Every service running go-monitor
--   POSTs to it with an X-Api-Key, from wherever that service happens to run —
--   another provider, a laptop, CI. It must be reachable from the open internet,
--   it must terminate TLS with a certificate the SDKs' trust stores accept, and
--   its value is copied into other repos' configuration, which makes it
--   effectively permanent: changing it means chasing down every producer.
--
--   query_url is the CONTROL-PLANE HOP. Only monitor-core itself calls it —
--   the app process reading a zone's events on behalf of a dashboard session,
--   and the reachability probe below. It is server-to-server, so it may
--   legitimately be a different hostname on a private DNS zone, behind a
--   different proxy, or on a port the SDKs are never told about.
--
-- Collapsing them means either exposing the control-plane hop publicly or
-- routing every SDK through the internal name — and the second failure is the
-- quiet one, because it works from inside the network and fails only for the
-- producers nobody tested from.
--
-- ⚠️ query_url IS AN OUTBOUND FETCH TARGET SUPPLIED BY AN ADMINISTRATOR, which
-- makes it textbook SSRF input. tools.ValidateExternalURL is the guard, applied
-- at write time in the query layer AND AGAIN at probe time (DNS can change under
-- a stored value; the write-time check is not a lasting property of the row).
-- Do not add a code path that fetches either of these without it.
--
--
-- WHY THEY ARE `NOT NULL DEFAULT ''` RATHER THAN REQUIRED IN THE DDL.
--
-- The single zone this install already has predates these columns, and there is
-- no value SQL could put there: the URL is a property of the deployment, and SQL
-- cannot read the environment. 117 solved its equivalent problem with literals
-- because the slugs it needed were compiled-in defaults with one right answer;
-- there is no equivalent here — MON_PUBLIC_URL's default is this install's
-- CONTROL plane, which is not necessarily any zone's ingest endpoint, and
-- writing it in would be inventing the very fact this migration exists to record.
--
-- So the empty string is a real, first-class state and it means "not recorded".
-- It is NOT the same as "unreachable", and the reachability enum keeps them
-- apart deliberately ('unconfigured' vs 'unreachable'), because they need
-- different actions from an operator: one is a registry row to finish filling
-- in, the other is a box to go and look at. The API refuses to CREATE a zone
-- without both URLs — a new row records infrastructure, and infrastructure with
-- no address is not something to record — so the empty state can only ever be
-- the pre-existing row, never a new one.
--
-- The CHECK below is coarse on purpose. It pins the ONE property SQL can hold
-- cheaply and unambiguously (https, no embedded whitespace) and leaves the real
-- rule — parseability, no credentials, no query string, no fragment, a public
-- host that does not resolve into RFC1918 — to tools.ValidateExternalURL in Go,
-- for the same reason 116 keeps the format rule in the DDL and the reserved-slug
-- rule out of it: the half that changes with the deployment does not belong in a
-- schema migration.
--
--
-- WHY THE PROBE RESULT IS PERSISTED AT ALL.
--
-- Because the alternative is an admin page that probes every zone on every page
-- load. That turns one wedged zone into a hung listing — N sequential timeouts
-- before anything renders — which is exactly the outcome the probe exists to
-- warn about, delivered by the warning itself. Storing the last verdict lets the
-- list render immediately from MariaDB and lets the operator re-check one zone
-- deliberately.
--
-- The cost of storing it is that the value AGES, and a stale 'healthy' is a lie
-- with a timestamp on it. last_probe_at is therefore NOT NULL-able decoration:
-- it is the half of the answer that makes the other half readable, and any UI
-- rendering `reachability` without it is asserting a freshness the column does
-- not have. NULL means never probed, and 'unknown' is its enum twin so the two
-- can never disagree.
--
-- reported_zone is VARCHAR(64), not VARCHAR(30) like slug, because it holds what
-- a REMOTE box claimed rather than a value this registry minted. It is untrusted
-- text: a misconfigured or unrelated service can put anything there, including
-- something that is not a valid slug at all, and truncating it to the slug width
-- would risk two different wrong answers reading as the same wrong answer. Go
-- truncates to fit rather than failing the write — losing the tail of a bogus
-- identity is fine; losing the probe result because the identity was bogus is
-- not.
--
--
-- RE-RUNNABILITY, the same rule 107 and 117 wrote down: db.RunMigrations records
-- a file as applied only after a clean Exec and DDL commits implicitly, so a run
-- that dies partway is retried IN FULL on the next boot with the successful half
-- already durable. Every statement below is guarded.

ALTER TABLE zones
    ADD COLUMN IF NOT EXISTS ingest_url VARCHAR(255) NOT NULL DEFAULT '' AFTER display_name;

ALTER TABLE zones
    ADD COLUMN IF NOT EXISTS query_url VARCHAR(255) NOT NULL DEFAULT '' AFTER ingest_url;

-- The reachability ladder, worst to best. Seven values rather than a boolean,
-- because "can I reach it" and "is it the zone I think it is" are different
-- questions and a boolean can only answer one of them:
--
--   unknown       never probed. The default for every existing row, and the only
--                 value that carries no claim at all.
--   unconfigured  no query_url recorded — there is nothing to probe. Distinct
--                 from unreachable: nobody has been unable to reach anything.
--   unreachable   the probe ran and got no usable answer (connection refused,
--                 TLS failure, timeout, a redirect it refused to follow, or a
--                 target the SSRF guard declined to fetch).
--   unverified    something answered 200 but would not say which zone it is.
--                 NOT healthy, and this is the value that costs the most to
--                 argue for: a 200 from /health proves a monitor-core is
--                 listening, not that it is THIS zone's monitor-core. Folding
--                 this into 'healthy' is how the mislabelling failure ships.
--   mismatched    it answered, and identified itself as a DIFFERENT zone (or as
--                 a control plane, which serves no events). The registry row
--                 points at the wrong box; every read through it is another
--                 tenant's data under this zone's name.
--   degraded      the right zone, but its own /ready says it is not serving.
--   healthy       the right zone, ready.
--
-- Ordered worst-to-best in the ENUM so that MIN()/ORDER BY over the column sorts
-- by severity for free — MariaDB orders an ENUM by declaration position, not
-- alphabetically, and 'degraded' < 'healthy' < 'mismatched' alphabetically would
-- put the worst outcome in the middle.
ALTER TABLE zones
    ADD COLUMN IF NOT EXISTS reachability
        ENUM('unknown','unconfigured','unreachable','unverified','mismatched','degraded','healthy')
        NOT NULL DEFAULT 'unknown' AFTER status;

ALTER TABLE zones
    ADD COLUMN IF NOT EXISTS reachability_detail VARCHAR(500) NOT NULL DEFAULT '' AFTER reachability;

ALTER TABLE zones
    ADD COLUMN IF NOT EXISTS reported_zone VARCHAR(64) NOT NULL DEFAULT '' AFTER reachability_detail;

ALTER TABLE zones
    ADD COLUMN IF NOT EXISTS last_probe_at DATETIME NULL DEFAULT NULL AFTER reported_zone;

-- Dropped-then-added for re-runnability, exactly as 117 does with its foreign
-- key. `LIKE 'https://%'` rather than a REGEXP because the real format rule lives
-- in Go and a second, subtly-different regex here would be two rules pretending
-- to be one — the mistake 116's header warns about for slugs. The whitespace
-- clause catches the copy-paste with a trailing tab or an embedded newline, which
-- is the one malformed value that survives a human eyeballing the config.
ALTER TABLE zones DROP CONSTRAINT IF EXISTS ck_zones_ingest_url;
ALTER TABLE zones
    ADD CONSTRAINT ck_zones_ingest_url CHECK (
        ingest_url = '' OR (ingest_url LIKE 'https://%' AND ingest_url NOT LIKE '%\t%' AND ingest_url NOT LIKE '% %')
    );

ALTER TABLE zones DROP CONSTRAINT IF EXISTS ck_zones_query_url;
ALTER TABLE zones
    ADD CONSTRAINT ck_zones_query_url CHECK (
        query_url = '' OR (query_url LIKE 'https://%' AND query_url NOT LIKE '%\t%' AND query_url NOT LIKE '% %')
    );

-- NO INDEX on reachability, and no index on either URL.
--
-- Same reasoning 116 gives for not adding a separate index on projects.zone_id:
-- the table holds one row today and will hold a handful — a zone is a whole
-- ClickHouse instance, not a tenant — so every query over it is a full scan of a
-- single page, and an index would be pure write cost against a plan the
-- optimiser would never choose. If this table ever grows past a few dozen rows,
-- something has gone wrong with what a zone means, and an index is not the fix.
