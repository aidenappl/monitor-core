package db

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/aidenappl/monitor-core/structs"
)

// Conn is the global ClickHouse connection
var Conn driver.Conn

// Database is the current database name
var Database string

// databaseNamePattern is deliberately narrower than what ClickHouse itself
// accepts. An identifier can never be a bound parameter, so this name is
// interpolated as text into every statement the service issues — the batch
// insert below, ~50 query sites, and the migration DDL. The boundary is
// therefore closed by rejecting anything that is not a plain lowercase
// identifier, not by trying to escape it. As a side effect it also guarantees
// the name carries no ';', which is what lets the migration runner rewrite file
// contents before splitting on that character.
var databaseNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{2,62}$`)

// ValidateDatabaseName rejects a CLICKHOUSE_DATABASE value that must not reach a
// statement. Connect calls it before Database is assigned, so anything that
// later reads the global is working from a checked identifier.
func ValidateDatabaseName(name string) error {
	if !databaseNamePattern.MatchString(name) {
		return fmt.Errorf("CLICKHOUSE_DATABASE %q is not an accepted database name: it must match %s. The name is interpolated directly into SQL, so it is rejected rather than escaped", name, databaseNamePattern)
	}
	return nil
}

// Connect establishes a connection to ClickHouse with retry logic.
// maxMemoryUsage is the per-query memory ceiling in bytes (env.ClickHouseMaxMemoryUsage).
func Connect(ctx context.Context, addr, database, username, password string, maxMemoryUsage int) error {
	var conn driver.Conn
	var err error

	// Checked before the retry loop: a malformed name is a config error, and
	// there is nothing for ten backed-off attempts to fix.
	if err := ValidateDatabaseName(database); err != nil {
		return err
	}

	// Retry connection up to 10 times with exponential backoff
	for attempt := 1; attempt <= 10; attempt++ {
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{addr},
			Auth: clickhouse.Auth{
				Database: database,
				Username: username,
				Password: password,
			},
			Debug: false,
			Settings: clickhouse.Settings{
				"max_execution_time": 60,

				// A ceiling on how much memory ONE query may allocate, in bytes.
				// Without it the effective limit is max_server_memory_usage —
				// ~90% of host RAM — so a single high-cardinality GROUP BY from
				// the analytics endpoints walks the server up to that line and
				// the node (swapless) OOM-kills the whole ClickHouse process,
				// taking ingest down with it. With it, the same query fails
				// alone: the caller gets MEMORY_LIMIT_EXCEEDED and every other
				// query, and the write path, keep running. Failing one dashboard
				// request is the cheaper outcome by a wide margin.
				//
				// Note this is per query, not per server: the pool caps us at
				// MaxOpenConns concurrent queries, so it bounds the blast radius
				// of a runaway query rather than guaranteeing a total footprint.
				// A true server-wide cap is max_server_memory_usage, which is
				// the server's own config and not ours to set from a client.
				"max_memory_usage": maxMemoryUsage,
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: time.Hour,
		})
		if err != nil {
			log.Printf("attempt %d: failed to open clickhouse connection: %v", attempt, err)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		if err = conn.Ping(ctx); err != nil {
			log.Printf("attempt %d: failed to ping clickhouse: %v", attempt, err)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		// Success
		log.Printf("connected to ClickHouse at %s", addr)
		Conn = conn
		Database = database
		return nil
	}

	return fmt.Errorf("failed to connect to clickhouse after 10 attempts: %w", err)
}

// eventInsertColumns names the columns of the events INSERT, in the physical
// order the table declares them: 001 created the table, 002 added user_id AFTER
// trace_id, 004 added issue_id AFTER data, and 006 added project AFTER service.
// _inserted_at is absent on purpose — it carries a DEFAULT now64(3) and is the
// server's record of when the row landed, not the caller's.
//
// Matching the physical order is a readability convention, not a requirement;
// the driver builds the insert block from this list, so what actually binds is
// that eventInsertRow returns its values in the SAME order as this slice. That
// correspondence is positional and completely unchecked at compile time: every
// column here is a String or a String-like LowCardinality, so transposing two of
// them produces no type error, no driver error and no failed insert. It produces
// rows in which service names are stored in env — valid-looking data that
// nothing downstream can identify as wrong. The two are kept adjacent, and
// pinned together by TestEventInsertColumnsMatchRowOrder, for that reason.
var eventInsertColumns = []string{
	"timestamp",
	"service",
	"project",
	"env",
	"job_id",
	"request_id",
	"trace_id",
	"user_id",
	"name",
	"level",
	"data",
	"issue_id",
}

// eventInsertRow flattens one event into the values for eventInsertColumns, in
// that exact order. Edit it and eventInsertColumns in the same keystroke.
func eventInsertRow(event *structs.Event) []interface{} {
	return []interface{}{
		event.Timestamp,
		event.Service,
		event.Project,
		event.Env,
		event.JobID,
		event.RequestID,
		event.TraceID,
		event.UserID,
		event.Name,
		event.Level,
		event.DataJSON(),
		event.IssueID,
	}
}

// WriteBatch inserts a batch of events into ClickHouse
func WriteBatch(ctx context.Context, events []*structs.Event) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := Conn.PrepareBatch(ctx, fmt.Sprintf(
		"INSERT INTO %s.events (%s)",
		Database,
		strings.Join(eventInsertColumns, ", "),
	))
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, event := range events {
		if err := batch.Append(eventInsertRow(event)...); err != nil {
			return fmt.Errorf("failed to append event to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// Close closes the ClickHouse connection
func Close() error {
	if Conn != nil {
		return Conn.Close()
	}
	return nil
}

// Writer wraps WriteBatch to implement the services.Writer interface
type Writer struct{}

func (w *Writer) WriteBatch(ctx context.Context, events []*structs.Event) error {
	return WriteBatch(ctx, events)
}
