package preflight

import (
	"fmt"
	"strings"

	"github.com/aidenappl/monitor-core/db"
)

// MonitorSchema checks that the DSN user can actually create and use the
// `monitor` schema, BEFORE the migration runner tries to.
//
// THE INCIDENT THIS DIAGNOSES. db/migrations/110 does
// `CREATE DATABASE IF NOT EXISTS monitor`, because the issue and alerting tables
// live in their own schema alongside the auth tables in monitor_auth. But the
// official mariadb image grants the app user privileges on MARIADB_DATABASE
// ONLY — literally `monitor_auth`, with the underscore escaped, not a wildcard.
// So on a fresh install the boot dies inside migration 110 with:
//
//	Error 1044 (42000): Access denied for user 'monitor'@'%' to database 'monitor'
//
// naming neither the grant nor the fix. `grep -rn GRANT` across the whole repo
// returned nothing executable — the requirement existed only as prose in 110's
// own header and one clause in AGENTS.md, neither of which is in the setup path.
// It cost hours on the second zone.
//
// It CANNOT be fixed inside a migration: db/sql.go opens one pool as that same
// user, and a user cannot grant privileges to itself. So the options are an
// initdb script (which only helps a database being created for the first time)
// and this — a diagnosis that names the exact statement to run.
//
// Deliberately runs the real `CREATE DATABASE IF NOT EXISTS`, not a permissions
// lookup. It is idempotent, it is what migration 110 will do moments later, and
// testing the actual operation beats inferring it from information_schema —
// which is how you end up with a check that passes while the thing it stands for
// fails.
func MonitorSchema(engine db.Queryable) []Check {
	if _, err := engine.Exec("CREATE DATABASE IF NOT EXISTS monitor DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"); err != nil {
		if isAccessDenied(err) {
			return []Check{{
				Name:     "mariadb.monitor_schema",
				Severity: Fatal,
				Detail: "the DSN user cannot create or use the `monitor` schema. Migration 110 creates it, " +
					"but the mariadb image grants the app user rights on MARIADB_DATABASE (monitor_auth) only — " +
					"never on a second schema. Without this the boot dies inside the migration with a bare " +
					"Error 1044 that names neither the grant nor the fix.",
				Remedy: "As root on this zone's MariaDB:\n" +
					"    GRANT ALL PRIVILEGES ON monitor.* TO 'monitor'@'%';\n" +
					"    FLUSH PRIVILEGES;\n" +
					"then restart. (Substitute the user from MON_DB_DSN if it is not `monitor`.)",
			}}
		}
		return []Check{{
			Name:     "mariadb.monitor_schema",
			Severity: Fatal,
			Detail:   fmt.Sprintf("could not create or reach the `monitor` schema: %v", err),
			Remedy:   "Check MON_DB_DSN points at a reachable MariaDB and that the schema is not locked by another operation.",
		}}
	}

	// A grant can cover CREATE and not SELECT, which would pass the statement
	// above and then fail at the first read — the same misdiagnosis one step
	// later. main.go already probes the ClickHouse events table for exactly this
	// reason; this is its MariaDB counterpart.
	if _, err := engine.Exec("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'monitor'"); err != nil {
		return []Check{{
			Name:     "mariadb.monitor_schema_read",
			Severity: Fatal,
			Detail:   fmt.Sprintf("the `monitor` schema exists but cannot be read by the DSN user: %v", err),
			Remedy:   "GRANT ALL PRIVILEGES ON monitor.* TO the DSN user — a CREATE-only grant is not enough.",
		}}
	}

	return nil
}

// isAccessDenied matches MariaDB's 1044 (no access to database) and 1045 (bad
// credentials) by message rather than by driver error code, so this package does
// not take a dependency on go-sql-driver's error type for a diagnostic.
func isAccessDenied(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "access denied")
}
