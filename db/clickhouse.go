package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/aidenappl/monitor-core/structs"
)

// Conn is the global ClickHouse connection
var Conn driver.Conn

// Database is the current database name
var Database string

// Connect establishes a connection to ClickHouse with retry logic
func Connect(ctx context.Context, addr, database, username, password string) error {
	var conn driver.Conn
	var err error

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

// WriteBatch inserts a batch of events into ClickHouse
func WriteBatch(ctx context.Context, events []*structs.Event) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := Conn.PrepareBatch(ctx, fmt.Sprintf(`
		INSERT INTO %s.events (
			timestamp,
			service,
			env,
			job_id,
			request_id,
			trace_id,
			name,
			level,
			data
		)
	`, Database))
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, event := range events {
		err := batch.Append(
			event.Timestamp,
			event.Service,
			event.Env,
			event.JobID,
			event.RequestID,
			event.TraceID,
			event.Name,
			event.Level,
			event.DataJSON(),
		)
		if err != nil {
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
