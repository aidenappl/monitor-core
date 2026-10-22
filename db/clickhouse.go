package db

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/telemetry"
)

// Conn is the global ClickHouse connection: the READ pool every query, exec,
// migration and probe draws from.
var Conn driver.Conn

// WriterConn is the batcher's own pool — WriteBatch is its only user. Nil means
// Connect has not run, runs no data plane, or could not open it; WriteBatch then
// writes through Conn, which is exactly how every write went before the split.
//
// It exists so reads cannot starve ingest. With one shared pool, ten slow
// dashboard reads held every connection and the batcher's PrepareBatch queued
// behind them for up to DialTimeout per attempt — five attempts, then the batch
// was dropped. Two connections because there is one batcher and it flushes one
// batch at a time: one for the INSERT, and one spare so the /health writer ping
// never makes a flush wait for a slot.
var WriterConn driver.Conn

// Pool sizes. The read pool is the documented brake on aggregate query memory
// (see max_memory_usage below), so it is not raised to "make room" for writes;
// the writer is additive instead.
const (
	READ_POOL_MAX_OPEN_CONNS = 10
	READ_POOL_MAX_IDLE_CONNS = 5

	// READ_POOL_DIAL_TIMEOUT is set on the read pool only. In clickhouse-go v2
	// DialTimeout is ALSO the pool-acquire wait: acquire() wraps the caller's
	// context in it before waiting for a free slot, and fails with
	// ErrAcquireConnTimeout when it expires. The driver default is 30s, so a
	// dashboard read queued behind a saturated pool sat for 30s before failing;
	// 10s fails it well inside the /v1 request deadline
	// (middleware.REQUEST_TIMEOUT, 25s) and is still far above any healthy dial.
	READ_POOL_DIAL_TIMEOUT = 10 * time.Second

	// The writer keeps the driver's DialTimeout (30s) on purpose. Shortening it
	// makes a slow dial or a briefly busy slot fail a flush sooner — and a flush
	// that exhausts its retries drops the batch.
	WRITER_POOL_MAX_OPEN_CONNS = 2
	WRITER_POOL_MAX_IDLE_CONNS = 1
)

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

// connParams is what both pools are opened with. Everything but the pool sizing
// is shared, so the two cannot drift into talking to different servers or with
// different per-query limits.
type connParams struct {
	addr, database, username, password string
	maxMemoryUsage                     int
}

// clickhouseOptions is the configuration both pools share. It returns a fresh
// value — Settings map included — on every call, so tuning one pool can never
// reach into the other through an aliased map.
func clickhouseOptions(p connParams) *clickhouse.Options {
	return &clickhouse.Options{
		Addr: []string{p.addr},
		Auth: clickhouse.Auth{
			Database: p.database,
			Username: p.username,
			Password: p.password,
		},
		Debug: false,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,

			// A ceiling on how much memory ONE query may allocate, in bytes.
			// ClickHouse 24.3's own per-query default is 0 — unlimited — so
			// without this the only bound is the server-wide
			// max_server_memory_usage, which 24.3 sizes at 0.9 × the memory it can
			// see: the container's cgroup limit when one is set, host RAM when
			// not. The ClickHouse containers (workers 8 and 9, stacks 32 and 30)
			// carry no memory limit today, so that line sits at ~90% of a host
			// they share with MariaDB, monitor-core and others. One
			// high-cardinality GROUP BY from the analytics endpoints can walk up
			// to it, and from there every query — the batcher's INSERT included —
			// fails with MEMORY_LIMIT_EXCEEDED, or the co-tenants push the host
			// into the kernel OOM killer first. With this cap the same query fails
			// alone: the caller gets MEMORY_LIMIT_EXCEEDED and every other query,
			// and the write path, keep running. Failing one dashboard request is
			// the cheaper outcome by a wide margin.
			//
			// Note this is per query, not per server: the pools cap us at
			// READ_POOL_MAX_OPEN_CONNS reads plus WRITER_POOL_MAX_OPEN_CONNS
			// inserts in flight, so it bounds the blast radius of a runaway query
			// rather than guaranteeing a total footprint. A true server-wide cap is
			// max_server_memory_usage (or a container memory limit, which 24.3
			// honours), and that is the server's own config, not ours to set from
			// a client.
			//
			// ⚠️ Do NOT add max_memory_usage_for_user here to close that gap.
			// Reads and the batcher connect as the SAME ClickHouse user, so a
			// dashboard burst that reached a per-user cap would fail ingest
			// flushes with code 241 — trading a slow page for lost events.
			"max_memory_usage": p.maxMemoryUsage,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		ConnMaxLifetime: time.Hour,
	}
}

// readPoolOptions is Conn's configuration: the shared settings, the read pool's
// size, and the short DialTimeout that bounds how long a read waits for a slot.
func readPoolOptions(p connParams) *clickhouse.Options {
	o := clickhouseOptions(p)
	o.MaxOpenConns = READ_POOL_MAX_OPEN_CONNS
	o.MaxIdleConns = READ_POOL_MAX_IDLE_CONNS
	o.DialTimeout = READ_POOL_DIAL_TIMEOUT
	return o
}

// writerPoolOptions is WriterConn's configuration: the shared settings and the
// writer's size. DialTimeout stays zero, which the driver defaults to 30s.
func writerPoolOptions(p connParams) *clickhouse.Options {
	o := clickhouseOptions(p)
	o.MaxOpenConns = WRITER_POOL_MAX_OPEN_CONNS
	o.MaxIdleConns = WRITER_POOL_MAX_IDLE_CONNS
	return o
}

// Connect establishes a connection to ClickHouse with retry logic.
// maxMemoryUsage is the per-query memory ceiling in bytes (env.ClickHouseMaxMemoryUsage).
//
// It opens two pools against the same server: Conn for reads (retried, and
// fatal to the caller when it never answers — unchanged) and then WriterConn for
// the batcher. The writer is best-effort by design: if it cannot be opened the
// process still boots, with batch writes on Conn exactly as before the split.
func Connect(ctx context.Context, addr, database, username, password string, maxMemoryUsage int) error {
	var conn driver.Conn
	var err error

	// Checked before the retry loop: a malformed name is a config error, and
	// there is nothing for ten backed-off attempts to fix.
	if err := ValidateDatabaseName(database); err != nil {
		return err
	}

	params := connParams{
		addr: addr, database: database, username: username, password: password,
		maxMemoryUsage: maxMemoryUsage,
	}

	// Retry connection up to 10 times with exponential backoff
	for attempt := 1; attempt <= 10; attempt++ {
		conn, err = clickhouse.Open(readPoolOptions(params))
		if err != nil {
			telemetry.WarnErr(ctx, "clickhouse.connect.retrying", err, map[string]any{
				"addr": addr, "database": database, "stage": "open",
				"attempt": attempt, "max_attempts": 10,
				"outcome": fmt.Sprintf("retrying in %ds", attempt),
			})
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		if err = conn.Ping(ctx); err != nil {
			telemetry.WarnErr(ctx, "clickhouse.connect.retrying", err, map[string]any{
				"addr": addr, "database": database, "stage": "ping",
				"attempt": attempt, "max_attempts": 10,
				"outcome": fmt.Sprintf("retrying in %ds", attempt),
			})
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		// The read pool answers, so the server is up; the writer is opened
		// against it once, not retried — its fallback is the pool that just
		// answered.
		writer := connectWriter(ctx, params)
		writerPool := "dedicated"
		if writer == nil {
			writerPool = "shared"
		}

		// Success. Every read and exec from here on goes through observedConn,
		// which gives a failure its query kind and reports slow calls.
		telemetry.Info(ctx, "clickhouse.connect.succeeded", map[string]any{
			"addr": addr, "database": database, "attempts": attempt,
			"writer_pool": writerPool,
		})
		Conn = observedConn{Conn: conn}
		WriterConn = writer
		Database = database
		return nil
	}

	return fmt.Errorf("failed to connect to clickhouse after 10 attempts: %w", err)
}

// connectWriter opens and pings the batcher's pool, returning nil when either
// fails. Nil is a working state, not an error: WriteBatch falls back to Conn,
// which is the pre-split behaviour, so a writer that will not come up costs the
// isolation and nothing else. A writer that opened but did not answer is closed
// rather than kept, since keeping it would put every flush behind a pool the
// read path has just shown is the odd one out.
//
// Reported once, as a warning with the same fields as the read pool's connect
// events: this is a degraded mode, and /health's clickhouse_writer_ok then
// mirrors the read pool, because that is the pool the writes are using.
func connectWriter(ctx context.Context, p connParams) driver.Conn {
	stage := "open"
	writer, err := clickhouse.Open(writerPoolOptions(p))
	if err == nil {
		stage = "ping"
		if err = writer.Ping(ctx); err == nil {
			return observedConn{Conn: writer}
		}
		_ = writer.Close()
	}
	telemetry.WarnErr(ctx, "clickhouse.writer.connect.failed", err, map[string]any{
		"addr": p.addr, "database": p.database, "stage": stage,
		"reason":  "clickhouse_writer_unavailable",
		"outcome": "batch writes share the read pool until the process restarts",
	})
	return nil
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

// WriteBatch inserts a batch of events into ClickHouse, through WriterConn when
// Connect opened one and through Conn otherwise.
func WriteBatch(ctx context.Context, events []*structs.Event) error {
	if len(events) == 0 {
		return nil
	}

	// Failures carry a QueryError so the batcher's event says how long the write
	// ran and what ClickHouse answered — with any quoted literal elided, since an
	// insert error can quote a tenant's row.
	start := time.Now()
	insertError := func(stage string, err error) error {
		return &QueryError{Kind: "db.WriteBatch", Duration: time.Since(start), Err: fmt.Errorf("%s: %w", stage, err)}
	}

	batch, err := writeConn().PrepareBatch(ctx, fmt.Sprintf(
		"INSERT INTO %s.events (%s)",
		Database,
		strings.Join(eventInsertColumns, ", "),
	))
	if err != nil {
		return insertError("failed to prepare batch", err)
	}

	for _, event := range events {
		if err := batch.Append(eventInsertRow(event)...); err != nil {
			return insertError("failed to append event to batch", err)
		}
	}

	if err := batch.Send(); err != nil {
		return insertError("failed to send batch", err)
	}

	return nil
}

// writeConn is the pool WriteBatch draws from: the dedicated writer when
// Connect opened one, otherwise the shared read pool.
func writeConn() driver.Conn {
	if WriterConn != nil {
		return WriterConn
	}
	return Conn
}

// Close closes both ClickHouse pools. Each is closed even if the other fails, and
// both errors are returned.
func Close() error {
	var readErr, writerErr error
	if Conn != nil {
		readErr = Conn.Close()
	}
	if WriterConn != nil {
		writerErr = WriterConn.Close()
	}
	return errors.Join(readErr, writerErr)
}

// Writer wraps WriteBatch to implement the services.Writer interface
type Writer struct{}

func (w *Writer) WriteBatch(ctx context.Context, events []*structs.Event) error {
	return WriteBatch(ctx, events)
}
