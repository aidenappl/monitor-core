package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/telemetry"

	_ "github.com/go-sql-driver/mysql"
)

// Pagination defaults for the relational (MariaDB) auth data layer.
const (
	DEFAULT_LIMIT = 50
	MAX_LIMIT     = 500
)

// SQL is the global MariaDB connection for the relational auth data layer
// (users, identities, refresh_tokens, sso_providers, sso_sessions, settings,
// api_keys). It lives alongside the ClickHouse connection (Conn), which
// remains the store for events/analytics.
var SQL *sql.DB

// Queryable is satisfied by both *sql.DB and *sql.Tx, so query functions can
// run inside or outside a transaction. Identical across every repo.
type Queryable interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

//go:embed migrations/*.sql
var migrationsFS embed.FS

// InitSQL opens the MariaDB connection pool. DSN comes from env.MonDBDSN
// (sourced from Keyring MON_DB_DSN in production). Panics on a malformed DSN;
// connectivity is verified separately via a bounded ping-with-retry.
func InitSQL() error {
	dsn := ensureDSNParams(env.MonDBDSN)
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open mariadb connection: %w", err)
	}

	conn.SetMaxOpenConns(25)
	conn.SetMaxIdleConns(10)
	conn.SetConnMaxLifetime(5 * time.Minute)

	// Ping with a short retry loop so a still-booting MariaDB doesn't fail startup.
	attempts := 0
	for attempt := 1; attempt <= 10; attempt++ {
		attempts = attempt
		if err = conn.Ping(); err == nil {
			break
		}
		telemetry.WarnErr(context.Background(), "mariadb.connect.retrying", err, map[string]any{
			"attempt": attempt, "max_attempts": 10,
			"outcome": fmt.Sprintf("retrying in %ds", attempt),
		})
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	if err != nil {
		return fmt.Errorf("failed to connect to mariadb after 10 attempts: %w", err)
	}

	SQL = conn
	telemetry.Info(context.Background(), "mariadb.connect.succeeded", map[string]any{"attempts": attempts})
	return nil
}

// CloseSQL closes the MariaDB connection.
func CloseSQL() error {
	if SQL != nil {
		return SQL.Close()
	}
	return nil
}

// ensureDSNParams guarantees the driver params the migration runner and query
// layer require (utf8mb4, parseTime, multiStatements) are present on the DSN.
func ensureDSNParams(dsn string) string {
	required := "charset=utf8mb4&parseTime=True&multiStatements=true"
	if strings.Contains(dsn, "?") {
		return dsn + "&" + required
	}
	return dsn + "?" + required
}

// RunMigrations applies every embedded MariaDB migration in filename order,
// tracking applied files in a migrations_applied table so each runs once.
// These are the RELATIONAL (MariaDB) migrations only — the ClickHouse schema
// lives in the repo-root migrations/ dir and is applied by ClickHouse itself.
func RunMigrations() error {
	_, err := SQL.Exec(`CREATE TABLE IF NOT EXISTS migrations_applied (
		name VARCHAR(255) NOT NULL PRIMARY KEY,
		applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`)
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("failed to read migrations directory: %w", err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	applied := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		var count int
		if err := SQL.QueryRow("SELECT COUNT(*) FROM migrations_applied WHERE name = ?", entry.Name()).Scan(&count); err != nil {
			return fmt.Errorf("failed to check migration status for %s: %w", entry.Name(), err)
		}
		if count > 0 {
			continue
		}

		content, err := migrationsFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", entry.Name(), err)
		}

		if _, err := SQL.Exec(string(content)); err != nil {
			return fmt.Errorf("failed to execute migration %s: %w", entry.Name(), err)
		}

		if _, err := SQL.Exec("INSERT INTO migrations_applied (name) VALUES (?)", entry.Name()); err != nil {
			return fmt.Errorf("failed to record migration %s: %w", entry.Name(), err)
		}

		applied++
	}

	telemetry.Info(context.Background(), "mariadb.migrations.applied", map[string]any{"applied": applied})
	return nil
}
