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

// RunMigrations applies every embedded ClickHouse migration (migrations/*.sql) in
// sorted filename order. Each file is split into individual statements on ';' and
// executed via the ClickHouse connection. All statements are idempotent
// (IF NOT EXISTS), so re-running is safe — there is no applied-tracking table.
//
// This is the ClickHouse schema (events + api_keys). The relational (MariaDB)
// auth schema has its own runner in db.RunMigrations. Ingestion and queries
// depend on monitor.events existing, so this is fail-fast: the caller should
// treat an error as fatal.
func RunMigrations(ctx context.Context) error {
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

		for _, stmt := range strings.Split(string(content), ";") {
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

	log.Printf("clickhouse migrations: applied %d statement(s) across %d file(s)", total, len(names))
	return nil
}
