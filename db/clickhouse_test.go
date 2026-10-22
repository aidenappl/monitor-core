package db

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/aidenappl/monitor-core/structs"
)

// The name reaches SQL as text, never as a bound parameter, so the accept cases
// pin what production actually runs on and the reject cases pin the shapes that
// would otherwise end a statement early or start a second one.
func TestValidateDatabaseName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		valid bool
	}{
		{name: "the deployed default", input: "monitor", valid: true},
		{name: "underscores and digits", input: "monitor_staging2", valid: true},
		{name: "minimum length", input: "abc", valid: true},

		{name: "empty", input: "", valid: false},
		{name: "too short", input: "ab", valid: false},
		{name: "leading digit", input: "2monitor", valid: false},
		{name: "uppercase", input: "Monitor", valid: false},
		{name: "hyphen", input: "monitor-staging", valid: false},
		{name: "qualified", input: "monitor.events", valid: false},
		{name: "statement terminator", input: "monitor; DROP TABLE events", valid: false},
		{name: "backtick quoted", input: "`monitor`", valid: false},
		{name: "whitespace", input: "monitor ", valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDatabaseName(tt.input)
			if tt.valid && err != nil {
				t.Errorf("ValidateDatabaseName(%q) = %v, want nil", tt.input, err)
			}
			if !tt.valid && err == nil {
				t.Errorf("ValidateDatabaseName(%q) = nil, want an error", tt.input)
			}
		})
	}
}

// eventInsertColumns and eventInsertRow are two hand-maintained lists that have
// to agree position for position, and nothing in the type system makes them.
// Every column but timestamp carries a string, so transposing two of them
// inserts cleanly: no type error, no driver error, no failed batch — just rows
// with service names stored in env, or project names stored in service, that
// every later query and dashboard reads as genuine. That silence is the whole
// reason this test exists.
//
// It works by giving each field a sentinel equal to its own column name, so a
// value sitting in the wrong slot names the slot it should have been in. A
// column added to one list and not the other fails on length before that.
func TestEventInsertColumnsMatchRowOrder(t *testing.T) {
	timestamp := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)

	event := &structs.Event{
		Timestamp: timestamp,
		Service:   "service",
		Project:   "project",
		Env:       "env",
		JobID:     "job_id",
		RequestID: "request_id",
		TraceID:   "trace_id",
		UserID:    "user_id",
		Name:      "name",
		Level:     "level",
		Data:      map[string]interface{}{"key": "value"},
		IssueID:   "issue_id",
	}

	// The two columns whose value is not a plain sentinel string. Everything
	// else is expected to equal the name of its own column.
	special := map[string]interface{}{
		"timestamp": timestamp,
		"data":      event.DataJSON(),
	}

	row := eventInsertRow(event)
	if len(row) != len(eventInsertColumns) {
		t.Fatalf("eventInsertRow returned %d values for %d columns %v: a column was added to one list and not the other",
			len(row), len(eventInsertColumns), eventInsertColumns)
	}

	seen := make(map[string]bool, len(eventInsertColumns))
	for i, column := range eventInsertColumns {
		if seen[column] {
			t.Errorf("column %q is listed twice, at position %d", column, i)
		}
		seen[column] = true

		want, ok := special[column]
		if !ok {
			want = column
		}
		if row[i] != want {
			t.Errorf("position %d is column %q but carries %#v, want %#v", i, column, row[i], want)
		}
	}
}

// The tenancy column has to be written, not merely declared. An events row whose
// project is empty is indistinguishable from a row written before the column
// existed, which is precisely the case the read path treats as the default
// project — so a dropped project on the write path does not fail loudly, it
// quietly files a tenant's traffic under someone else's.
func TestEventInsertWritesProject(t *testing.T) {
	row := eventInsertRow(&structs.Event{Project: "trailblaze"})

	index := -1
	for i, column := range eventInsertColumns {
		if column == "project" {
			index = i
			break
		}
	}
	if index == -1 {
		t.Fatal("eventInsertColumns does not include project: events are being written with no tenant")
	}
	if row[index] != "trailblaze" {
		t.Errorf("project column carries %#v, want %#v", row[index], "trailblaze")
	}
}

// fakePool is a ClickHouse pool that records what WriteBatch and Close do with
// it. The embedded interface is nil, so any other method panics: a write that
// reached for something else would fail here rather than quietly succeed.
type fakePool struct {
	driver.Conn
	prepared []string
	appended int
	sent     int
	closed   int
	closeErr error
}

func (p *fakePool) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	p.prepared = append(p.prepared, query)
	return &fakeBatch{pool: p}, nil
}

func (p *fakePool) Close() error {
	p.closed++
	return p.closeErr
}

type fakeBatch struct {
	driver.Batch
	pool *fakePool
}

func (b *fakeBatch) Append(v ...any) error {
	b.pool.appended++
	return nil
}

func (b *fakeBatch) Send() error {
	b.pool.sent++
	return nil
}

// withPools installs the two pools for one test; a nil argument leaves that
// global nil.
func withPools(t *testing.T, read, writer *fakePool) {
	t.Helper()
	previousConn, previousWriter, previousDatabase := Conn, WriterConn, Database
	Conn, WriterConn, Database = nil, nil, "monitor"
	if read != nil {
		Conn = read
	}
	if writer != nil {
		WriterConn = writer
	}
	t.Cleanup(func() { Conn, WriterConn, Database = previousConn, previousWriter, previousDatabase })
}

// WriteBatch must take the batcher's own pool, or the split does nothing:
// dashboard reads would still hold every connection the flush needs. And with no
// dedicated writer it must still write — through the read pool, exactly as every
// write went before the writer existed — rather than panic on a nil interface or
// drop the batch.
func TestWriteBatchRoutesToTheWriterPool(t *testing.T) {
	events := []*structs.Event{
		{Service: "api", Project: "trailblaze", Name: "a"},
		{Service: "api", Project: "trailblaze", Name: "b"},
	}

	tests := []struct {
		name       string
		withWriter bool
	}{
		{name: "a dedicated writer takes every batch", withWriter: true},
		{name: "no dedicated writer falls back to the read pool", withWriter: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			read := &fakePool{}
			var writer *fakePool
			if tt.withWriter {
				writer = &fakePool{}
			}
			withPools(t, read, writer)

			if err := WriteBatch(context.Background(), events); err != nil {
				t.Fatalf("WriteBatch: %v", err)
			}

			used, unused := read, (*fakePool)(nil)
			if tt.withWriter {
				used, unused = writer, read
			}
			if len(used.prepared) != 1 || used.appended != len(events) || used.sent != 1 {
				t.Errorf("expected pool: prepared %d, appended %d, sent %d — want 1, %d, 1",
					len(used.prepared), used.appended, used.sent, len(events))
			}
			if len(used.prepared) == 1 && !strings.HasPrefix(used.prepared[0], "INSERT INTO monitor.events (") {
				t.Errorf("prepared %q, want an INSERT into monitor.events", used.prepared[0])
			}
			if unused != nil && len(unused.prepared) != 0 {
				t.Errorf("the read pool prepared %d batches while a writer existed — reads can still starve ingest", len(unused.prepared))
			}
		})
	}
}

// Close must release both pools, and one failing to close must not leave the
// other open.
func TestCloseClosesBothPools(t *testing.T) {
	readErr := errors.New("read pool close failed")
	writerErr := errors.New("writer pool close failed")

	tests := []struct {
		name      string
		read      *fakePool
		writer    *fakePool
		wantErrIs []error
	}{
		{name: "both pools", read: &fakePool{}, writer: &fakePool{}},
		{name: "the writer fails to close", read: &fakePool{}, writer: &fakePool{closeErr: writerErr}, wantErrIs: []error{writerErr}},
		{name: "the read pool fails to close", read: &fakePool{closeErr: readErr}, writer: &fakePool{}, wantErrIs: []error{readErr}},
		{name: "both fail", read: &fakePool{closeErr: readErr}, writer: &fakePool{closeErr: writerErr}, wantErrIs: []error{readErr, writerErr}},
		{name: "no dedicated writer", read: &fakePool{}},
		{name: "nothing connected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withPools(t, tt.read, tt.writer)

			err := Close()
			if len(tt.wantErrIs) == 0 && err != nil {
				t.Errorf("Close() = %v, want nil", err)
			}
			for _, want := range tt.wantErrIs {
				if !errors.Is(err, want) {
					t.Errorf("Close() = %v, want it to carry %v", err, want)
				}
			}
			for label, pool := range map[string]*fakePool{"read": tt.read, "writer": tt.writer} {
				if pool != nil && pool.closed != 1 {
					t.Errorf("%s pool closed %d times, want 1", label, pool.closed)
				}
			}
		})
	}
}

// The two pools differ in sizing and nothing else. A writer opened with
// different credentials, database, compression or per-query limits would be a
// second, silently different client of the same server; one sharing the read
// pool's Settings map would let a change to either reach both.
func TestPoolOptions(t *testing.T) {
	params := connParams{
		addr: "clickhouse:9000", database: "monitor", username: "admin", password: "secret",
		maxMemoryUsage: 2147483648,
	}
	read := readPoolOptions(params)
	writer := writerPoolOptions(params)

	sizing := []struct {
		name      string
		got, want any
	}{
		// The read pool's size is the aggregate-memory brake, not a knob to
		// trade for writes: it stays at 10.
		{"read MaxOpenConns", read.MaxOpenConns, 10},
		{"read MaxIdleConns", read.MaxIdleConns, 5},
		{"read DialTimeout", read.DialTimeout, 10 * time.Second},
		{"writer MaxOpenConns", writer.MaxOpenConns, 2},
		{"writer MaxIdleConns", writer.MaxIdleConns, 1},
		// Zero is what leaves the driver's own default (30s) in place.
		{"writer DialTimeout", writer.DialTimeout, time.Duration(0)},
	}
	for _, s := range sizing {
		if s.got != s.want {
			t.Errorf("%s = %v, want %v", s.name, s.got, s.want)
		}
	}

	if got := read.Settings["max_memory_usage"]; got != params.maxMemoryUsage {
		t.Errorf("max_memory_usage = %v, want %d", got, params.maxMemoryUsage)
	}
	if _, set := read.Settings["max_memory_usage_for_user"]; set {
		t.Error("max_memory_usage_for_user is set: reads and the batcher share one ClickHouse user, so a per-user cap fails ingest during a dashboard burst")
	}

	// Everything but the sizing must match.
	unsized := func(o *clickhouse.Options) clickhouse.Options {
		c := *o
		c.MaxOpenConns, c.MaxIdleConns, c.DialTimeout = 0, 0, 0
		return c
	}
	if r, w := unsized(read), unsized(writer); !reflect.DeepEqual(r, w) {
		t.Errorf("the pools differ beyond sizing:\nread   %+v\nwriter %+v", r, w)
	}

	// And nothing may be shared by reference.
	read.Settings["probe"] = 1
	if _, leaked := writer.Settings["probe"]; leaked {
		t.Error("the pools share one Settings map: tuning one would silently tune the other")
	}
	if read.Compression == writer.Compression {
		t.Error("the pools share one Compression value")
	}
}

// A writer that cannot come up is a degraded mode, not a boot failure: Connect
// has already proved the server answers on the read pool, and nil is what sends
// WriteBatch back to that pool. Port 1 on loopback refuses at once, so the ping
// fails without waiting on any timeout.
func TestConnectWriterFallsBackWhenTheWriterCannotConnect(t *testing.T) {
	writer := connectWriter(context.Background(), connParams{
		addr: "127.0.0.1:1", database: "monitor", username: "default",
		maxMemoryUsage: 2147483648,
	})
	if writer != nil {
		_ = writer.Close()
		t.Fatal("connectWriter returned a pool whose ping failed; every flush would queue behind it instead of using the read pool")
	}
}
