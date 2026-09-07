package migrations

import (
	"context"
	"embed"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/aidenappl/monitor-core/db"
)

//go:embed *.sql
var migrationsFS embed.FS

// rewriter builds the substitution RunMigrations applies to each file before it
// splits on ';'. The literal name takes exactly two forms across migrations/*.sql
// — as a table qualifier (`monitor.`), and as the CREATE DATABASE target in 001 —
// and nothing else in those files contains the token, so replacing both is
// exhaustive without needing to parse SQL. The Replacer never rescans what it
// wrote, so a database name that itself contains "monitor" cannot compound.
//
// Split out from RunMigrations so it can be exercised against the real embedded
// files with no ClickHouse connection — embed_test.go pins the property that
// matters most, that this is byte-for-byte identity for the default `monitor`.
func rewriter(database string) *strings.Replacer {
	return strings.NewReplacer(
		"CREATE DATABASE IF NOT EXISTS monitor", "CREATE DATABASE IF NOT EXISTS "+database,
		"monitor.", database+".",
	)
}

// RunMigrations applies every embedded ClickHouse migration (migrations/*.sql) in
// sorted filename order. Each file's text is first rewritten from the literal
// database name it is written against (`monitor`) to db.Database, then split into
// individual statements on ';' and executed via the ClickHouse connection. All
// statements are idempotent (IF NOT EXISTS), so re-running is safe — there is no
// applied-tracking table.
//
// The rewrite exists because the runtime does the same interpolation: ingestion
// and every query build their SQL against db.Database. Without it, a process
// started with CLICKHOUSE_DATABASE set to anything but `monitor` migrates one
// database and reads and writes another — and does so while looking perfectly
// healthy, since nothing on the serving path ever touches the migrated one.
//
// Two consequences bind anything added here, because there is no tracking table
// and the runner is fail-fast:
//   - Every file runs on EVERY boot. A statement that is not idempotent (an
//     INSERT ... SELECT, an UPDATE, a DELETE) will be re-applied on each restart
//     and compound. Schema-only, IF NOT EXISTS-guarded statements only.
//   - Files are split naively on ';'. A semicolon inside a comment produces a
//     comment-only fragment, which ClickHouse rejects and which therefore takes
//     the service down at startup.
//
// Both survive the rewrite: it is a plain substitution of two fixed forms, it
// runs before the split, and db.ValidateDatabaseName has already constrained the
// name to [a-z][a-z0-9_]{2,62} — so it can add neither a statement nor a
// semicolon. Keep writing new files against the literal `monitor.` prefix like
// the existing ones; an unqualified table name would quietly resolve against the
// connection's default database and defeat the whole mechanism.
//
// One-off data reconciliation belongs in migrations/manual/ — a subdirectory,
// so the `*.sql` pattern above does not embed it — and is run by hand. Those
// files are NOT rewritten, so qualify them yourself when you run them.
//
// This is the ClickHouse schema (events + api_keys). The relational (MariaDB)
// auth schema has its own runner in db.RunMigrations. Ingestion and queries
// depend on the events table existing, so this is fail-fast: the caller should
// treat an error as fatal.
func RunMigrations(ctx context.Context) error {
	// db.Database is a package global set by db.Connect. Re-check it here rather
	// than trust the boot order, because an unset one would rewrite `monitor.`
	// to a bare `.` and fail as an opaque ClickHouse syntax error instead.
	if err := db.ValidateDatabaseName(db.Database); err != nil {
		return fmt.Errorf("refusing to run migrations: %w", err)
	}

	rewrite := rewriter(db.Database)

	entries, err := migrationsFS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("failed to read embedded migrations: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)

	total := 0
	for _, name := range names {
		content, err := migrationsFS.ReadFile(name)
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", name, err)
		}

		for _, stmt := range strings.Split(rewrite.Replace(string(content)), ";") {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if err := db.Conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("failed to execute statement in %s: %w", name, err)
			}
			total++
		}
	}

	// Naming the database is the point: it is the one line that tells an operator
	// which database was migrated, and so whether it is the one being served.
	log.Printf("clickhouse migrations: applied %d statement(s) across %d file(s) in database %q", total, len(names), db.Database)
	return nil
}
