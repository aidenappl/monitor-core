package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aidenappl/go-keyring"
	"github.com/aidenappl/monitor-core/alerts"
	"github.com/aidenappl/monitor-core/apikeys"
	"github.com/aidenappl/monitor-core/bootstrap"
	"github.com/aidenappl/monitor-core/buildinfo"
	"github.com/aidenappl/monitor-core/cutover"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/issues"
	"github.com/aidenappl/monitor-core/migrations"
	"github.com/aidenappl/monitor-core/preflight"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/registry"
	"github.com/aidenappl/monitor-core/routes"
	"github.com/aidenappl/monitor-core/services"
	"github.com/aidenappl/monitor-core/sso"
	"github.com/aidenappl/monitor-core/telemetry"
	"github.com/rs/cors"
)

// subcommands are the one-off operator actions this binary accepts instead of
// serving. Listed in one place so the unknown-argument error can name them all,
// rather than an operator guessing which spelling this build understands.
var subcommands = []string{"backfill-issues", "backfill-config [--dry-run]", "check-env", "version"}

// requireKnownCommand rejects an argument this build does not implement.
//
// Takes argv rather than reading os.Args so it is testable, which matters more
// than it looks: the behaviour being pinned is "does NOT boot a server", and
// that is not something a test can assert about a function that calls
// log.Fatalf on the real process.
func requireKnownCommand(argv []string) error {
	if len(argv) < 2 {
		return nil
	}
	for _, known := range subcommands {
		if argv[1] == strings.Fields(known)[0] {
			return nil
		}
	}
	return fmt.Errorf("unknown command %q — run with no arguments to serve, or one of: %s",
		argv[1], strings.Join(subcommands, ", "))
}

func main() {
	// VALIDATE THE ARGUMENT FIRST, before Keyring, config, or a single connection.
	//
	// It depends on nothing, and putting it anywhere later means a typo is
	// reported as whatever the boot happens to trip over first — an operator who
	// typed `backfill-confg` gets "MONITOR_API_KEY must be set", which is true,
	// useless, and about a different problem.
	//
	// It also closes a real foot-gun: without this the subcommand checks further
	// down simply fall through and the process boots a FULL SERVER. A typo, or a
	// correct command run against an image too old to have it, would start a
	// second evaluator and batcher inside a container that already has one,
	// re-run both migration runners, and then die failing to bind the port —
	// presenting as a wall of normal startup logs ending in "address in use",
	// which says nothing about the word that was actually wrong.
	if err := requireKnownCommand(os.Args); err != nil {
		log.Fatalf("❌ %v", err)
	}

	// TELEMETRY COMES UP FIRST — before Keyring, config, or any connection — so
	// that every one of those can be reported when it fails. Its configuration is
	// read from the plain environment for the same reason (env.LoadTelemetry):
	// Keyring cannot be the source of the thing that reports Keyring failing.
	//
	// Serving only. The subcommands are operator tools whose output belongs to the
	// terminal they were run in; with telemetry never initialised, every event
	// they trigger prints there exactly as the log line it replaced.
	//
	// Nothing here can fail or delay the boot, and that includes an ingest URL
	// pointing back at THIS process while it is still starting (appleby-core
	// reports into itself): events queue, and the spool — or, without one, the
	// SDK's short retry window — carries them until the listener is up.
	serving := len(os.Args) < 2
	env.LoadTelemetry()
	if serving {
		telemetry.Init(telemetry.Settings{
			IngestURL: env.TelemetryIngestURL,
			APIKey:    env.TelemetryAPIKey,
			Env:       env.TelemetryEnv,
			Zone:      env.TelemetryZone,
			SpoolDir:  env.TelemetrySpoolDir,
			Debug:     env.TelemetryDebug,
			Stdout:    env.TelemetryStdout,
		})
		preflight.Sink = reportPreflight
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Optionally load secrets from Keyring before reading env vars.
	// Requires KEYRING_URL, KEYRING_ACCESS_KEY_ID, and KEYRING_SECRET_ACCESS_KEY
	// to be set in the environment. Silently skipped if they are absent.
	//
	// The environment is SNAPSHOTTED around the injection so preflight can say
	// which variables Keyring REPLACED rather than filled in. That distinction
	// is the whole diagnosis of an incident that cost hours: Keyring injects
	// before env.Load() and getEnv reads os.Getenv first, so Keyring silently
	// wins over anything the container set — and a broadly-granted token
	// overrode a zone's CLICKHOUSE_* with another zone's, with nothing logged.
	envBeforeKeyring := preflight.Snapshot()
	if client, err := keyring.New(); err == nil {
		if err := client.InjectEnv(ctx); err != nil {
			telemetry.Error(ctx, "keyring.inject.failed", err, map[string]any{
				"reason":  "keyring_unavailable",
				"outcome": "booting on the container's plain environment; any secret only Keyring holds is missing",
			})
		}
	}

	// Load configuration (picks up any values injected by Keyring above).
	env.Load()

	// Diagnose the configuration before anything tries to USE it, so a mistake is
	// reported as itself rather than as whatever fails first because of it. A
	// literal "${…}" in a DSN is the motivating case: unresolved, it surfaces as
	// "Access denied ... (using password: YES)", which is indistinguishable from
	// a wrong password, a stale volume, or the Keyring override above.
	preflightChecks := preflight.Env()
	preflightChecks = append(preflightChecks, preflight.KeyringOverrides(envBeforeKeyring, preflight.Snapshot())...)
	preflightFatal := preflight.Report(preflightChecks)

	// `monitor-core check-env` is the SAME checks, run standalone and exiting on
	// the result. One code path, deliberately: a doctor that could disagree with
	// what the boot enforces is a doctor nobody can trust, and the disagreement
	// would surface as "it passes the check and still will not start".
	//
	// It runs here — after Keyring and env.Load, before any connection — so it
	// works against a stack that does not exist yet, which is exactly when a zone
	// is being stood up and the answer is most wanted.
	if len(os.Args) > 1 && os.Args[1] == "check-env" {
		if preflightFatal {
			log.Fatal("check-env: configuration is not usable — see above")
		}
		log.Printf("check-env: %d finding(s), none fatal", len(preflightChecks))
		return
	}

	// `monitor-core version` prints the build identity without connecting to
	// anything, so it answers "which image is this?" on a box whose stores are
	// down — which is when that question tends to get asked.
	if len(os.Args) > 1 && os.Args[1] == "version" {
		info := buildinfo.Get()
		fmt.Printf("%s %s\ncommit:  %s\nbuilt:   %s\ngo:      %s\nrole:    %s\nzone:    %s\n",
			info.Service, info.Version, info.Commit, info.BuildTime, info.Go, env.MonRole, env.ZoneSlug)
		return
	}

	if preflightFatal {
		telemetry.Fatal("config.preflight.failed", "preflight found a configuration error that would misdiagnose itself later — see above", nil, map[string]any{
			"reason":        "preflight_failed",
			"failed_checks": fatalCheckNames(preflightChecks),
		})
	}

	// Settle which plane this process is before anything reads the answer. An
	// unrecognised MON_ROLE stops the boot rather than degrading to the default —
	// see env.RequireValidRole for why a silent fallback here would quietly stand
	// up a second control plane on a machine meant to be a zone.
	if err := env.RequireValidRole(); err != nil {
		telemetry.Fatal("config.role.invalid", "MON_ROLE is not a recognised role", err, map[string]any{"reason": "invalid_role"})
	}

	// Announce the role once, at the very top of the boot log, for the same
	// reason migrations.RunMigrations names the database it migrated: an operator
	// holding nothing but a container's logs must be able to answer "which plane
	// does this process think it is?". Printed BEFORE the first fatal below, so a
	// process that dies during startup still says what it was trying to be — and
	// printed unconditionally, including for `both`, so running as `both` is a
	// state someone can see rather than the absence of a signal.
	log.Printf("monitor-core role: %s (control plane: %t, data plane: %t)",
		env.MonRole, env.MonRole.RunsControlPlane(), env.MonRole.RunsDataPlane())

	// Every event from here on says which plane and zone produced it: the control
	// plane and appleby-core are the same binary under one service name.
	telemetry.SetIdentity(string(env.MonRole), env.ZoneSlug)

	// Say which zone this process believes it is, and — the load-bearing half —
	// whether anyone chose. A defaulted slug is the one configuration mistake
	// that produces a working process serving the wrong tenant's name, so it is
	// announced next to the role rather than left to be inferred from /health.
	build := buildinfo.Get()
	telemetry.Info(ctx, "service.startup", map[string]any{
		"role":            string(env.MonRole),
		"zone":            env.ZoneSlug,
		"version":         build.Version,
		"commit":          build.Commit,
		"port":            env.Port,
		"telemetry_spool": env.TelemetrySpoolDir != "",
	})
	if !env.ZoneSlugExplicit {
		telemetry.Warn(ctx, "config.zone.defaulted", map[string]any{
			"zone":    env.ZoneSlug,
			"reason":  "mon_zone_slug_unset",
			"outcome": "serving under the fallback zone name — this is the default, not a choice",
		})
	}
	if err := env.RequireZoneIdentity(); err != nil {
		telemetry.Fatal("config.zone.unset", "a data plane must name its own zone", err, map[string]any{"reason": "zone_identity_required"})
	}

	// MON_PUBLIC_URL lost its default because that default was the control
	// plane's own URL (see env.go). Unset is now honest rather than misleading,
	// but it is not harmless, and the two ways it bites are different enough to
	// name separately: a fresh zone cannot seed its registry endpoints, and a
	// control plane builds SSO redirect_uris that are relative paths rather than
	// origins — which the IdP rejects with an error naming neither.
	if strings.TrimSpace(env.PublicBaseURL) == "" {
		telemetry.Warn(ctx, "config.public_url.unset", map[string]any{
			"reason":  "mon_public_url_unset",
			"outcome": "a new zone cannot record its ingest/query endpoints, and SSO redirect_uris will be malformed",
		})
	}

	if env.IngestKey == "" {
		telemetry.Fatal("config.ingest_key.missing", "MONITOR_API_KEY must be set — refusing to start without ingest authentication", nil, map[string]any{"reason": "ingest_key_missing"})
	}

	// Refuse to start in production on the committed dev-default JWT/crypto keys
	// (would allow forged admin sessions / decryptable SSO secrets). Set
	// MON_COOKIE_INSECURE=true for local dev to permit the fallbacks.
	if err := env.RequireProductionSecrets(); err != nil {
		telemetry.Fatal("config.secrets.insecure", "refusing to start on development secrets", err, map[string]any{"reason": "production_secrets_required"})
	}

	// Handle shutdown signals
	// Non-nil when the Phase 2 configuration cutover has not been run. Set below,
	// read by the evaluator gate and reported by /health and /ready.
	var configCutoverPending error

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// ---- ClickHouse: DATA PLANE ONLY -----------------------------------------
	//
	// An app process holds NO ClickHouse connection. db.Conn stays nil, and that
	// is the enforcement rather than a side effect: db.Conn is an interface, so a
	// nil one panics on the first method call instead of returning a zero value,
	// and there is no arrangement of this code in which a control plane quietly
	// reads or writes the event store. Nothing in an app process should reach it
	// — buildRouter registers no route that touches it, and none of the
	// goroutines below start — so the panic is a backstop for a mistake, not a
	// path anything is expected to take. router_test.go pins both halves.
	//
	// Do not "harden" this by returning an error from the nil case. Degrading to
	// an error is how a plane ends up serving empty results that look like an
	// empty database; the panic is the loud failure that is wanted here.
	if env.MonRole.RunsDataPlane() {
		// Connect to ClickHouse
		if err := db.Connect(ctx, env.ClickHouseAddr, env.ClickHouseDatabase, env.ClickHouseUsername, env.ClickHousePassword, env.ClickHouseMaxMemoryUsage); err != nil {
			telemetry.Fatal("clickhouse.connect.failed", "failed to connect to ClickHouse", err, map[string]any{
				"reason":   "clickhouse_unreachable",
				"addr":     env.ClickHouseAddr,
				"database": env.ClickHouseDatabase,
			})
		}
		defer db.Close()

		// Apply the ClickHouse schema migrations (events + api_keys) at startup.
		// Ingestion and queries depend on the events table existing, so this is
		// fail-fast — a fresh deploy no longer needs a manual `dev migrate` step.
		// The runner rewrites the DDL to env.ClickHouseDatabase, so it migrates the
		// same database the serving path reads and writes.
		if err := migrations.RunMigrations(ctx); err != nil {
			telemetry.Fatal("clickhouse.migrate.failed", "failed to run ClickHouse migrations", err, map[string]any{
				"reason":   "clickhouse_migration_failed",
				"database": db.Database,
			})
		}

		// Then prove it. Every failure mode of the above — a half-applied file, a
		// grant that covers CREATE but not SELECT, a rewrite that lands somewhere
		// unexpected — otherwise produces a process that boots green, accepts events
		// and loses all of them, because nothing on the serving path reads the
		// schema until the first write. One cheap read against the table ingestion
		// depends on turns that into a crash-loop, which is at least visible.
		//
		// Retried, unlike the assertion it makes: db.Connect and db.InitSQL both
		// retry, and a probe that crash-loops the container on one dropped
		// connection would be a worse failure than the one it guards against. Three
		// quick attempts separate "the schema is wrong" — which never recovers and
		// should stop the boot — from "the connection blipped", which does.
		probeQuery := fmt.Sprintf("SELECT count() FROM %s.events WHERE 1 = 0", db.Database)
		var probeErr error
		for attempt := 1; attempt <= 3; attempt++ {
			if probeErr = db.Conn.Exec(ctx, probeQuery); probeErr == nil {
				break
			}
			telemetry.WarnErr(ctx, "clickhouse.schema.probe.retrying", probeErr, map[string]any{
				"database":     db.Database,
				"attempt":      attempt,
				"max_attempts": 3,
				"outcome":      fmt.Sprintf("retrying in %ds", attempt),
			})
			time.Sleep(time.Duration(attempt) * time.Second)
		}
		if probeErr != nil {
			telemetry.Fatal("clickhouse.schema.unreadable", "the events table is not readable after migrations — refusing to start and silently drop events", probeErr, map[string]any{
				"reason":   "events_table_unreadable",
				"database": db.Database,
				"attempts": 3,
			})
		}
	}

	// Connect to MariaDB (relational auth data layer: users, identities,
	// refresh_tokens, sso_providers, sso_sessions, settings, api_keys).
	if err := db.InitSQL(); err != nil {
		telemetry.Fatal("mariadb.connect.failed", "failed to connect to MariaDB", err, map[string]any{"reason": "mariadb_unreachable"})
	}
	// Closed on the way out; an error closing a pool at exit changes nothing.
	defer func() { _ = db.CloseSQL() }()

	// Diagnose the `monitor` schema grant BEFORE the migration runner needs it,
	// so the missing GRANT is reported as a missing GRANT rather than as an
	// Error 1044 from inside migration 110 that names neither the grant nor the
	// fix. That misdiagnosis cost hours on the second zone.
	if schemaChecks := preflight.MonitorSchema(db.SQL); preflight.Report(schemaChecks) {
		telemetry.Fatal("mariadb.schema.unusable", "the database is not usable by this service — see the remedy above", nil, map[string]any{
			"reason":        "monitor_schema_unusable",
			"failed_checks": fatalCheckNames(schemaChecks),
		})
	}

	// Apply the MariaDB auth-schema migrations at startup.
	if err := db.RunMigrations(); err != nil {
		telemetry.Fatal("mariadb.migrate.failed", "failed to run MariaDB migrations", err, map[string]any{"reason": "mariadb_migration_failed"})
	}

	// Establish this install's identity and record what schema it is on, both
	// for GET /version.
	//
	// AFTER the migrations, because the settings table has to exist and the
	// counts have to be final; BEFORE the server listens, because neither can
	// change while the process runs and /version exists to be polled.
	//
	// Neither is fatal. A process that cannot mint an install id is still a
	// perfectly good Monitor — it just cannot be told apart from another install
	// by a probe — and refusing to boot over a diagnostic would trade a real
	// outage for a missing label.
	if installID, err := bootstrap.EnsureInstallID(db.SQL); err != nil {
		telemetry.WarnErr(ctx, "install_id.ensure.failed", err, map[string]any{
			"reason":  "mariadb_write_failed",
			"outcome": "GET /version omits the install id",
		})
	} else {
		routes.InstallID = installID
	}

	if applied, latest, err := query.AppliedMigrations(db.SQL); err != nil {
		telemetry.WarnErr(ctx, "migrations.ledger.read.failed", err, map[string]any{
			"reason":  "mariadb_read_failed",
			"outcome": "GET /version omits the MariaDB schema counts",
		})
	} else {
		routes.Schema.MariaDBApplied = applied
		routes.Schema.MariaDBLatest = latest
	}
	// Reported in every role: the files are compiled into the binary whether or
	// not this process runs a ClickHouse, and it is the binary a drift check is
	// comparing. See migrations.Inventory for why this is a file count rather
	// than an applied count — that runner keeps no ledger.
	routes.Schema.ClickHouseFiles, routes.Schema.ClickHouseLatest = migrations.Inventory()

	// `monitor-core backfill-issues` copies the legacy ClickHouse issues table
	// into MariaDB and exits without serving. Placed after both migration runners
	// so the destination schema is guaranteed to exist, and kept as an explicit
	// subcommand rather than a boot step because it is a one-time cutover, not
	// part of normal startup. Safe to re-run — see issues.BackfillFromClickHouse.
	if len(os.Args) > 1 && os.Args[1] == "backfill-issues" {
		// It reads the legacy ClickHouse table, so it needs the connection the
		// block above only opens on the data plane. Refused with the reason rather
		// than run into a nil-Conn panic, because the operator invoking this by
		// hand deserves to be told which knob is wrong.
		if !env.MonRole.RunsDataPlane() {
			log.Fatalf("❌ backfill-issues reads ClickHouse, which %s=%s does not connect to — re-run it with %s=%s or %s=%s",
				env.ROLE_ENV_VAR, env.MonRole, env.ROLE_ENV_VAR, env.RoleZone, env.ROLE_ENV_VAR, env.RoleBoth)
		}
		copied, err := issues.BackfillFromClickHouse(ctx)
		if err != nil {
			log.Fatalf("❌ backfill failed after %d issue(s): %v", copied, err)
		}
		log.Printf("✅ backfilled %d issue(s)", copied)
		return
	}

	// `monitor-core backfill-config` copies the six configuration tables that
	// moved to MariaDB in migrations 119-124 — alert_rules,
	// notification_channels, notification_policies, service_groups, dashboards
	// and saved_views — out of the legacy ClickHouse tables, then exits without
	// serving. Same shape as backfill-issues above and for the same reasons:
	// after both migration runners so the destination exists, and a subcommand
	// rather than a boot step because it is a cutover.
	//
	// ORDER IS LOAD-BEARING ON THE CUTOVER DEPLOY. The new binary reads these
	// tables from MariaDB, so this must run BEFORE it serves. Started first, the
	// new binary finds an empty alert_rules and the evaluator quietly has nothing
	// to evaluate — no error, no alert, just silence — and alerts.Init seeds four
	// default notification policies that this then appends four more behind,
	// leaving a routing list with duplicates in it. Neither failure raises
	// anything. AGENTS.md §8 carries the full sequence and the recovery.
	//
	// Safe to re-run: a row whose id is already in MariaDB is skipped, never
	// overwritten. See cutover.BackfillConfig.
	if len(os.Args) > 1 && os.Args[1] == "backfill-config" {
		// Reads the legacy ClickHouse tables, so it needs the connection only the
		// data plane opens — refused with the reason rather than run into a
		// nil-Conn panic, exactly as backfill-issues is.
		if !env.MonRole.RunsDataPlane() {
			log.Fatalf("❌ backfill-config reads ClickHouse, which %s=%s does not connect to — re-run it with %s=%s or %s=%s",
				env.ROLE_ENV_VAR, env.MonRole, env.ROLE_ENV_VAR, env.RoleZone, env.ROLE_ENV_VAR, env.RoleBoth)
		}
		// `backfill-config --dry-run` reads, validates and reports without writing
		// a row or stamping the marker. Worth running first every time: it is the
		// only way to see which rows would be REFUSED — a duplicate name against
		// the new UNIQUE keys, an empty enum — before the real run stops halfway
		// through a hand-run cutover on a deploy that has alerting switched off.
		cutover.DryRun = len(os.Args) > 2 && os.Args[2] == "--dry-run"
		if cutover.DryRun {
			log.Printf("🔍 DRY RUN — reading and validating only, nothing will be written")
		}

		reports, err := cutover.BackfillConfig(ctx)
		for _, report := range reports {
			log.Printf("   %-32s copied %d, skipped %d", report.Table, report.Copied, report.Skipped)
		}
		if err != nil {
			log.Fatalf("❌ config backfill failed: %v", err)
		}
		if cutover.DryRun {
			log.Printf("✅ dry run clean — re-run without --dry-run to apply")
		} else {
			log.Printf("✅ configuration backfill complete")
		}
		return
	}

	// ---- Tenancy rows: EVERY ROLE, because they mean different things ---------
	//
	// An earlier version of this gated the registry seed behind the control
	// plane, on the reasoning that "a zone minting its own zone row is a second
	// copy of something there should be one of". That conflated two different
	// tables that happen to share a name.
	//
	// The CONTROL PLANE's registry answers "which zones exist" — it is what the
	// switcher lists and what a fan-out would route over. There is exactly one of
	// those, and a zone must never write to it.
	//
	// A ZONE's OWN registry answers a local question: "which project does this
	// api_keys row belong to". apikeys.Create resolves a key's project against
	// the zone's own MariaDB (see resolveProject), and scope.ProjectPredicate
	// refuses to build a WHERE clause without one. A zone with no rows here can
	// migrate, boot and report healthy — and then cannot mint a single ingest
	// key, because the project it would bind to does not exist locally.
	//
	// So a zone seeds ITSELF: the row named by its own MON_ZONE_SLUG, and that
	// zone's default project. That is not divergence, it is the local resolution
	// data without which the zone cannot do its one job. It is idempotent, and it
	// can only ever create the zone this process already believes it is.
	//
	// Seed the zone and its default project (no-op once both rows exist).
	// Fail-fast rather than degraded: the default project is what api_keys bind
	// to and what the env master key stamps events with, so a Monitor without it
	// would ingest happily with a null tenant on every row — the state that is
	// hardest to notice and impossible to reattribute afterwards.
	if err := bootstrap.EnsureZoneAndProject(db.SQL); err != nil {
		telemetry.Fatal("tenancy.bootstrap.failed", "failed to bootstrap the tenancy registry", err, map[string]any{
			"reason":  "tenancy_seed_failed",
			"zone":    env.ZoneSlug,
			"project": env.DefaultProjectSlug,
		})
	}

	// Report rows filed under a project this zone does not have. A diagnostic,
	// not a gate: the migrations backfill `project` with the literal 'default'
	// because SQL cannot read the environment, so an install that overrode
	// MON_DEFAULT_PROJECT ends up with dashboards and rules under a project
	// nobody selects — present, uncorrupted, and invisible.
	bootstrap.VerifyConfigTenancy(db.SQL)

	// ---- Identity: CONTROL PLANE ONLY ----------------------------------------
	//
	// This one genuinely is an act of ownership. A zone that seeded an admin user
	// would stand up a second identity store — its own accounts, its own
	// passwords, its own sessions — none of which the control plane knows about
	// and none of which anything reconciles. Sessions are the control plane's
	// alone; a zone authenticates api_keys and nothing else.
	if env.MonRole.RunsControlPlane() {
		// Seed the first admin user on a fresh database (no-op once any user exists).
		if err := bootstrap.EnsureAdminUser(db.SQL); err != nil {
			telemetry.Fatal("admin.bootstrap.failed", "failed to bootstrap the admin user", err, map[string]any{"reason": "admin_seed_failed"})
		}

		// Wire the SSO revocation checkpoint into SessionMiddleware. Until this runs
		// the hook is nil and the checkpoint is skipped.
		//
		// Skipped on a zone because a zone serves no SSO surface at all — no
		// provider CRUD, no callback, no back-channel logout. The checkpoint is an
		// outbound introspection against a provider row this plane does not own.
		sso.Install()
	}

	// Initialize API key management (loads the key cache from MariaDB).
	//
	// EVERY role. A zone authenticates ingest against this cache, and the control
	// plane serves the management surface from it, so neither can go without.
	if err := apikeys.Init(ctx); err != nil {
		telemetry.Error(ctx, "apikeys.cache.load.failed", err, map[string]any{
			"reason":  "mariadb_read_failed",
			"outcome": "database-stored API keys do not authenticate until the refresher's next successful load (every 30s); the env master key still works",
		})
	}

	// Load the tenancy registry cache (the zone + its projects). This is what
	// QueryAuthMiddleware validates a session's ?project selector against on
	// every request, so it must not be a per-request MariaDB round trip.
	//
	// A warning rather than a fatal, like every other cache above it: the
	// refresher keeps trying, and a failed load degrades to "no project may be
	// explicitly selected" while the default project — which every session gets
	// when it names none — keeps working. Failing the boot would take the whole
	// dashboard down to protect a selector.
	if err := registry.Init(ctx); err != nil {
		telemetry.WarnErr(ctx, "registry.cache.load.failed", err, map[string]any{
			"reason":  "mariadb_read_failed",
			"outcome": "an explicit ?project selector is refused until the next successful refresh; the default project keeps working",
		})
	}

	// ---- Event machinery: DATA PLANE ONLY ------------------------------------
	//
	// The three Init calls below create ClickHouse tables and would panic on a
	// nil db.Conn; the hubs, queue, batcher and evaluator are the ingest and
	// alerting pipeline, which a control plane has no business running. Their
	// globals (routes.EventHub, routes.Queue, routes.Batcher,
	// routes.AlertNotifHub) stay nil in an app process — buildRouter registers no
	// route that reads them, and HealthHandler is written to tolerate a nil Queue
	// for exactly this reason.
	//
	// issues.Init is here rather than beside the MariaDB work it actually writes
	// to: it starts the error-tracking worker pool, and the only thing that ever
	// feeds that pool is ingestion. Workers with no producer are not harmful,
	// just dishonest about what the process does.
	var queue *services.Queue
	if env.MonRole.RunsDataPlane() {
		// Refuse to serve with the alerting configuration still stranded in
		// ClickHouse. Migrations 119-124 create the six MariaDB tables EMPTY, and
		// every read below now goes to them, so a process that starts before
		// `backfill-config` has run finds no rules and no policies and reports
		// nothing at all about it: listEnabledRules returns an empty slice with a
		// nil error, and the evaluator's loop simply has no work.
		//
		// FIRST IN THIS BLOCK, before alerts.Init, and that ordering is the point.
		// alerts.Init seeds four default notification policies into an empty
		// table; the backfill would then append the four ClickHouse originals
		// behind them, leaving eight policies of which four match every alert by
		// priority and route it nowhere. Refusing here means that state is never
		// created, rather than created and then explained in a runbook.
		//
		// See cutover.RequireConfigBackfill for why this is a crash-loop rather
		// than a warning, and why it can only fire once — it is gated on a marker
		// the backfill stamps, so deleting your last alert rule after the cutover
		// can never bring it back.
		//
		// DEGRADED, NOT FATAL — and the difference matters more than it looks.
		// A log.Fatalf here crash-loops the container, and CI deploys this image
		// automatically on every push to main, so the DEFAULT path would take
		// ingestion down until an operator noticed and ran the cutover by hand.
		// That trades a rare silent failure for a common loud outage, in the one
		// system whose job is to still be recording when everything else breaks.
		//
		// So: refuse to EVALUATE (the thing that would be silently wrong), keep
		// ingesting (the thing that must never stop), and make the state
		// unmissable — a startup banner, /health, and a 503 on /ready. This is
		// exactly the liveness/readiness split those two endpoints exist for:
		// Docker keeps the container up because the process is fine, while a load
		// balancer and an operator both see that it is not fully functional.
		if err := cutover.RequireConfigBackfill(ctx); err != nil {
			configCutoverPending = err
			telemetry.WarnErr(ctx, "alerting.cutover.pending", err, map[string]any{
				"reason":  "config_backfill_pending",
				"outcome": "ALERTING IS DISABLED — ingest, queries and issues are unaffected. Run `monitor-core backfill-config`, then restart this process",
			})
			routes.AlertingDisabledReason = err.Error()
		}

		// dashboards.Init and views.Init are GONE, not omitted: migrations 123
		// and 124 create those tables, so there is nothing left for a boot-time
		// CREATE TABLE to do. Both packages are now empty and say so.
		//
		// alerts.Init survives because two of its eight tables did not move —
		// alert_states and alert_history are still ClickHouse, still created
		// here, and alert_history's 90-day TTL is the reason (migration 119). It
		// also seeds the default notification policies into MariaDB, which stays
		// on this plane rather than moving to bootstrap/ so that the set of
		// processes creating those four rows is unchanged.
		// The default-policy seed is SUPPRESSED while the cutover is outstanding.
		// An empty notification_policies table then means "still in ClickHouse",
		// not "fresh install", and seeding it would put four defaults ahead of the
		// operator's real policies — which match first and would silently govern
		// every route. The ClickHouse table creation still runs; only the seed is
		// held back.
		if err := alerts.Init(ctx, configCutoverPending == nil); err != nil {
			telemetry.Error(ctx, "alerts.init.failed", err, map[string]any{
				"reason":  "policy_seed_failed",
				"project": env.DefaultProjectSlug,
				"outcome": "the default notification policies were not seeded; evaluation still starts",
			})
		}

		// Initialize issues
		if err := issues.Init(ctx); err != nil {
			telemetry.Error(ctx, "issues.init.failed", err, map[string]any{
				"reason":  "tracker_init_failed",
				"outcome": "error events are ingested but not grouped into issues",
			})
		}

		// Create SSE hub
		hub := services.NewHub(env.MaxSSESubscribers)
		routes.EventHub = hub

		// Create event queue
		queue = services.NewQueue(env.QueueSize)
		routes.Queue = queue

		// Create and start batcher
		writer := &db.Writer{}
		batcher := services.NewBatcher(queue, writer, env.BatchSize, env.FlushInterval)
		routes.Batcher = batcher
		go func() {
			defer telemetry.Recover(ctx, "batcher", "batcher stopped: ingest keeps accepting until QUEUE_SIZE, then refuses every event, until the process restarts")
			batcher.Run(ctx)
		}()

		// The periodic ingest summary — the ONLY way healthy ingest shows up in
		// Monitor, since a successful POST /v1/events emits no request event.
		telemetry.Go(ctx, "ingest-summary", routes.RunIngestSummary)

		// Create alert notification hub for SSE streaming
		alertHub := alerts.NewAlertHub(env.MaxSSESubscribers)
		routes.AlertNotifHub = alertHub

		// Start alert evaluator — unless the configuration cutover is outstanding,
		// in which case its rule table is empty and every evaluation would decide
		// "nothing is wrong" from an empty slice, with no error and no log line.
		// Not starting it at all is the honest state: alerting is off, and the
		// banner above plus /health and /ready all say so.
		if configCutoverPending == nil {
			evaluator := alerts.NewEvaluator(alertHub)
			go func() {
				defer telemetry.Recover(ctx, "alert-evaluator", "alert evaluation stopped: no rule fires or resolves until the process restarts")
				evaluator.Run(ctx)
			}()
		}
	}

	// Setup router. The role decides which routes exist at all — see
	// buildRouter in router.go.
	r := buildRouter(env.MonRole)

	// CORS Middleware
	corsMiddleware := cors.New(cors.Options{
		AllowedOrigins: []string{
			"https://monitor.local.appleby.cloud:3020",
			"https://monitor.appleby.cloud",
			"https://trailblaze.to",
			"https://*.trailblaze.to",
			"https://*.appleby.cloud",
			"http://localhost:*",
		},
		AllowCredentials: true,
		AllowedHeaders:   []string{"X-Requested-With", "Content-Type", "Origin", "Authorization", "Accept", "Referer", "Dnt", "User-Agent", "X-Api-Key", "X-CSRF-Token"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		// Let browsers cache a preflight for 2h (Chromium's cap) instead of the
		// 5s default. Without it monitor-js pays a preflight before nearly every
		// ingest flush. The one cost: a change to AllowedHeaders/AllowedMethods
		// can take up to 2h to reach an already-open browser.
		MaxAge: 7200,
	})

	server := &http.Server{
		Addr:         ":" + env.Port,
		Handler:      corsMiddleware.Handler(r),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Called as Shutdown begins. An SSE stream ends only when its client goes
	// away, so without this one open live-tail tab holds every deploy for the
	// full shutdown timeout — long enough, with the drain and flush below, to be
	// SIGKILLed past Docker's stop grace. See routes/draining.go.
	server.RegisterOnShutdown(routes.StartDraining)

	go func() {
		defer telemetry.Recover(ctx, "http-server", "the HTTP server stopped serving")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			telemetry.Fatal("http.listen.failed", "the HTTP server could not serve", err, map[string]any{
				"reason": "listen_failed",
				"port":   env.Port,
			})
		}
	}()

	// The last line of the boot phase. From here on info events are shipped but
	// no longer printed — a request event per dashboard call would otherwise be a
	// stdout line per dashboard call.
	telemetry.Info(ctx, "http.listen.started", map[string]any{"port": env.Port, "role": string(env.MonRole)})
	telemetry.MarkServing()

	// Wait for shutdown signal
	sig := <-sigChan
	telemetry.MarkStopping()
	telemetry.Info(ctx, "service.stopping", map[string]any{"signal": sig.String()})
	// Before the listener closes — appleby-core ships to itself. See telemetry.Flush.
	telemetry.Flush(2 * time.Second)

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		telemetry.WarnErr(ctx, "http.shutdown.failed", err, map[string]any{
			"reason":  "shutdown_timeout",
			"outcome": "open connections were cut rather than drained",
		})
	}

	cancel()
	// Nil in an app process, which creates no queue. The sleep that follows is
	// the batcher's window to drain what the close released, and there is no
	// batcher either — but it costs two seconds on a shutdown path and removing
	// it for one role would be a second thing to keep in step for no gain.
	if queue != nil {
		queue.Close()
	}
	time.Sleep(2 * time.Second)

	// Last: the batcher has drained, so anything it reported is in the buffer.
	telemetry.Shutdown(sig.String())
}

// reportPreflight turns a preflight finding into an event while serving. A Fatal
// finding is a warning here because the boot's own fatal event (which names it)
// is the one failure; `check-env` never sets the sink and prints for a terminal.
func reportPreflight(check preflight.Check) {
	data := map[string]any{
		"check":  check.Name,
		"detail": check.Detail,
		"remedy": check.Remedy,
	}
	switch check.Severity {
	case preflight.Fatal:
		telemetry.Warn(context.Background(), "preflight.check.failed", data)
	case preflight.Warn:
		telemetry.Warn(context.Background(), "preflight.check.warned", data)
	default:
		telemetry.Info(context.Background(), "preflight.check.noted", data)
	}
}

// fatalCheckNames names the findings that stopped the boot.
func fatalCheckNames(checks []preflight.Check) []string {
	var names []string
	for _, check := range checks {
		if check.Severity == preflight.Fatal {
			names = append(names, check.Name)
		}
	}
	return names
}
