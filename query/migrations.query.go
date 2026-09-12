package query

import (
	"database/sql"
	"fmt"

	"github.com/aidenappl/monitor-core/db"
)

// AppliedMigrations reports how many MariaDB migrations this DATABASE has
// applied, and the name of the newest.
//
// Read from migrations_applied — the ledger db.RunMigrations writes after each
// file succeeds — so unlike the ClickHouse side this is a fact about the
// database rather than about the binary. A zone whose image is current but whose
// migrations failed part-way is exactly the state this distinguishes, and it is
// invisible to a version string alone.
//
// A missing table is not an error: on a brand-new database this is called before
// anything has run, and "zero applied" is the correct answer rather than a
// failure worth aborting a boot for.
func AppliedMigrations(engine db.Queryable) (count int, latest string, err error) {
	if err := engine.QueryRow("SELECT COUNT(*) FROM migrations_applied").Scan(&count); err != nil {
		return 0, "", nil
	}

	var name sql.NullString
	if err := engine.QueryRow("SELECT name FROM migrations_applied ORDER BY name DESC LIMIT 1").Scan(&name); err != nil {
		if err == sql.ErrNoRows {
			return count, "", nil
		}
		return count, "", fmt.Errorf("read latest applied migration: %w", err)
	}
	return count, name.String, nil
}
