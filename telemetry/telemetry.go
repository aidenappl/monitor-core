// Package telemetry reports monitor-core's own errors, warnings and activity to
// Monitor — the appleby zone, like every other appleby.cloud service.
//
// It is the only package in this repo that talks to go-monitor. Everything else
// calls the helpers here, which add three things the SDK does not do on its own
// and which Monitor, of all services, cannot go without:
//
//   - A stdout policy. Warnings and errors are always printed, because when a
//     zone's own ClickHouse or MariaDB is down its telemetry cannot land in that
//     zone, and the container log is the only place the failure is visible
//     while it lasts. Info is printed only while the process boots, when it is a
//     handful of lines; after that it would be one line per dashboard request on
//     hosts whose Docker logs are not rotated.
//   - Coalescing. appleby-core ingests its own telemetry, so any failure that
//     can produce an event which then fails the same way would feed itself.
//     Those go through WarnCoalesced/ErrorCoalesced: at most one event per
//     failure mode per minute, carrying a count of what was suppressed.
//   - Process identity. The control plane and appleby-core run the same binary
//     under one service name, so every event carries mon_role and mon_zone.
//
// Telemetry is never a boot dependency — least of all Monitor's own. Init cannot
// fail or block; with MON_TELEMETRY_INGEST_URL unset nothing is shipped and the
// service runs as it always has, warnings and errors still printed.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	monitor "github.com/aidenappl/go-monitor"
)

// Service is the name every monitor-core event is filed under, in every role.
const Service = "monitor-core"

// MAX_STACK_BYTES bounds the stack trace carried on an event. A panic deep in a
// driver can produce a stack far larger than anything a reader needs, and it is
// shipped on every occurrence.
const MAX_STACK_BYTES = 16 << 10

// Settings is the telemetry configuration. main reads it from the environment
// BEFORE Keyring injects anything (env.LoadTelemetry): Keyring's own failures
// must be reportable, so Keyring can never be where this comes from.
type Settings struct {
	IngestURL string // empty: nothing is shipped
	APIKey    string // an ingest-scope key minted on the zone IngestURL points at
	Env       string
	Zone      string // expected destination zone, asserted once at Init; never sent
	SpoolDir  string // durable spool; empty keeps events in memory only
	Debug     bool   // enables Debug events
	Stdout    bool   // the SDK prints EVERY event as NDJSON — for local verification only
}

var (
	// initialised is set once Init has configured the SDK. Before that — and in
	// the CLI subcommands, which never call Init — nothing can be shipped, so
	// every level is printed, exactly as the log lines these events replaced.
	initialised atomic.Bool
	debugOn     atomic.Bool
	sdkStdout   atomic.Bool
	booting     atomic.Bool
	shipping    atomic.Bool
	startedAt   atomic.Int64
	lastDropAt  atomic.Int64
	identity    atomic.Pointer[processIdentity]
)

type processIdentity struct{ role, zone string }

// exit is os.Exit; tests of Fatal replace it.
var exit = os.Exit

// Init configures the SDK. It never fails the boot: a configuration the SDK
// rejects is printed and the process runs on without telemetry.
func Init(s Settings) {
	startedAt.Store(time.Now().UnixNano())
	booting.Store(true)
	debugOn.Store(s.Debug)
	sdkStdout.Store(s.Stdout)

	// Source is added by emit, which knows the real caller. The SDK's own capture
	// would name this package on every event, because every event is emitted
	// from here.
	captureSource := false
	err := monitor.Init(monitor.Config{
		Service:       Service,
		Env:           s.Env,
		Zone:          s.Zone,
		IngestURL:     s.IngestURL,
		APIKey:        s.APIKey,
		SpoolDir:      s.SpoolDir,
		Debug:         s.Debug,
		DisableStdout: !s.Stdout,
		GzipEnabled:   true,
		CaptureSource: &captureSource,
		// Called on Emit's own goroutine when the buffer is full, so it only
		// records a timestamp. Emitting from here would put an event into the
		// buffer that just refused one.
		OnDrop: func(int64) { lastDropAt.Store(time.Now().UnixNano()) },
		// Monitor holds accounts. No failure needs a user's address to be
		// diagnosed; events identify a user by user_id.
		RedactKeys: []string{"email", "user_email", "admin_email"},
	})
	if err != nil {
		log.Printf("telemetry: Monitor disabled: %v", err)
		return
	}
	initialised.Store(true)
	shipping.Store(s.IngestURL != "")

	switch {
	case s.IngestURL == "":
		log.Printf("telemetry: MON_TELEMETRY_INGEST_URL is not set — events are not being shipped; warnings and errors still print here")
	case s.APIKey == "":
		log.Printf("telemetry: MON_TELEMETRY_API_KEY is not set — the ingest endpoint will refuse every batch")
	}

	Go(context.Background(), "telemetry-coalesce-sweeper", runSweeper)
}

// SetIdentity stamps every later event with the plane and zone this process
// runs. Called once env.Load has settled them.
func SetIdentity(role, zone string) {
	identity.Store(&processIdentity{role: role, zone: zone})
}

// MarkServing ends the boot phase: from here on info events are shipped but no
// longer printed.
func MarkServing() {
	booting.Store(false)
}

// MarkStopping starts the shutdown phase: info is printed again, because the
// last lines in a container log are what an operator reads to learn why it
// stopped.
func MarkStopping() {
	booting.Store(true)
}

// Flush delivers what is buffered now, including the trailing counts of
// anything being coalesced, waiting at most timeout. main calls it the moment a
// stop signal arrives, while the listener is still up: a zone that reports to
// itself (appleby-core) cannot deliver anything once server.Shutdown closes that
// listener, so without a spool its stop events would be lost. On timeout the
// flush carries on in the background and Shutdown bounds it.
func Flush(timeout time.Duration) {
	flushCoalesced(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		monitor.Flush()
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
}

// Shutdown reports the stop and delivers (or, with a spool, persists) what is
// still buffered, including the trailing counts of anything being coalesced.
// Bounded by the SDK to a few seconds.
func Shutdown(reason string) {
	flushCoalesced(true)
	data := map[string]any{"reason": reason}
	if t := startedAt.Load(); t != 0 {
		data["uptime_s"] = int64(time.Since(time.Unix(0, t)).Seconds())
	}
	emit(context.Background(), monitor.LevelInfo, "service.shutdown", data, 2)
	monitor.Shutdown()
}

// WithRequestID puts the request id on every event emitted under ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	return monitor.WithRequestID(ctx, id)
}

// WithUserID attributes every event emitted under ctx to an authenticated user.
// Only ever call it with an identity that has been VERIFIED.
func WithUserID(ctx context.Context, id string) context.Context {
	return monitor.WithUserID(ctx, id)
}

// At emits at a level chosen at run time — the request event, whose level
// follows the status it returned.
func At(ctx context.Context, level, name string, data map[string]any) {
	if level == monitor.LevelDebug && !debugOn.Load() {
		return
	}
	emit(ctx, level, name, data, 2)
}

// AddError adds err's message, type and carried context to data, without a
// stack. For code that assembles an event itself.
func AddError(data map[string]any, err error) map[string]any {
	return withError(data, err, false)
}

// Stack is the calling goroutine's stack, bounded to MAX_STACK_BYTES.
func Stack() string {
	return stack()
}

// Debug emits a step trace. Off unless MON_TELEMETRY_DEBUG=true.
func Debug(ctx context.Context, name string, data map[string]any) {
	if !debugOn.Load() {
		return
	}
	emit(ctx, monitor.LevelDebug, name, data, 2)
}

// Info emits a state change or business action. Keep it to a few identifying
// fields: no bodies, no lists, no before/after diffs.
func Info(ctx context.Context, name string, data map[string]any) {
	emit(ctx, monitor.LevelInfo, name, data, 2)
}

// Warn emits something that worked but should not be ignored.
func Warn(ctx context.Context, name string, data map[string]any) {
	emit(ctx, monitor.LevelWarn, name, data, 2)
}

// WarnErr is a warning caused by an error: the error and its type ride along,
// without a stack.
func WarnErr(ctx context.Context, name string, err error, data map[string]any) {
	emit(ctx, monitor.LevelWarn, name, withError(data, err, false), 2)
}

// Error reports a failure with error, error_type and stack_trace. err may be
// nil for a failure that is not a Go error (a status code, a refusal).
//
// Inside a request, pass r.Context(), and report only failures that do NOT reach
// the responder — a 5xx that does is already reported, once, by the request's
// own event (middleware.LoggingMiddleware).
func Error(ctx context.Context, name string, err error, data map[string]any) {
	emit(ctx, monitor.LevelError, name, withError(data, err, true), 2)
}

// Fatal reports why the process is about to exit, delivers what is buffered, and
// exits 1. log.Fatal alone skips that delivery, so the one event that explains
// why the service is down would never leave the process.
func Fatal(name, message string, err error, data map[string]any) {
	if data == nil {
		data = make(map[string]any, 4)
	}
	data["message"] = message
	emit(context.Background(), monitor.LevelFatal, name, withError(data, err, true), 2)
	monitor.Shutdown()
	if sdkStdout.Load() && initialised.Load() {
		// The SDK printed the event as JSON; leave one human-readable line too.
		log.Printf("FATAL %s: %s: %v", name, message, err)
	}
	exit(1)
}

// Go runs fn on its own goroutine and reports a panic instead of letting it kill
// the process and every buffered event with it. The goroutine then returns; a
// loop that must keep running is the caller's to restart.
func Go(ctx context.Context, name string, fn func(ctx context.Context)) {
	go func() {
		defer Recover(ctx, name, "goroutine "+name+" stopped")
		fn(ctx)
	}()
}

// Recover reports a panic in the calling goroutine. It must be deferred
// directly — `defer telemetry.Recover(ctx, "batcher", "…")` — because recover
// only stops a panic when called by the deferred function itself.
func Recover(ctx context.Context, name, outcome string) {
	if rec := recover(); rec != nil {
		ReportPanic(ctx, name, rec, outcome)
	}
}

// ReportPanic reports a value the caller already recovered. outcome says what
// the panic cost ("batcher stopped — …").
func ReportPanic(ctx context.Context, name string, rec any, outcome string) {
	emit(ctx, monitor.LevelError, "panic.recovered", map[string]any{
		"goroutine":   name,
		"error":       fmt.Sprint(rec),
		"panic_type":  fmt.Sprintf("%T", rec),
		"stack_trace": stack(),
		"reason":      "panic",
		"outcome":     outcome,
	}, 2)
}

// HealthStats is the telemetry shipper's state, reported on GET /health so a
// Monitor that has stopped reporting on itself can be seen from outside.
type HealthStats struct {
	Shipping    bool    `json:"shipping"`
	Enqueued    int64   `json:"enqueued"`
	Flushed     int64   `json:"flushed"`
	Dropped     int64   `json:"dropped"`
	Quarantined int64   `json:"quarantined"`
	Pending     int64   `json:"pending"`
	LastDropAt  *string `json:"last_drop_at"`
}

// Stats reports the shipper's lifetime counters.
func Stats() HealthStats {
	st := monitor.Stats()
	out := HealthStats{
		Shipping:    shipping.Load(),
		Enqueued:    st.Enqueued,
		Flushed:     st.Flushed,
		Dropped:     st.Dropped,
		Quarantined: st.Quarantined,
		Pending:     st.Pending,
	}
	if t := lastDropAt.Load(); t != 0 {
		s := time.Unix(0, t).UTC().Format(time.RFC3339)
		out.LastDropAt = &s
	}
	return out
}

// ErrorText is what an event says an error was. An error can supply a safer
// rendering by implementing TelemetryError — db.QueryError does, so that a
// ClickHouse message quoting the statement never ships its literals.
//
// The safe rendering replaces only the wrapped error's own text, so the context
// the layers above added ("failed to count events: …") survives. If a wrapper
// rewrote the message beyond recognition, the safe text alone is used — never
// the raw one.
func ErrorText(err error) string {
	full := err.Error()
	var te interface {
		error
		TelemetryError() string
	}
	if errors.As(err, &te) {
		inner := te.Error()
		if inner != "" && strings.Contains(full, inner) {
			return strings.Replace(full, inner, te.TelemetryError(), 1)
		}
		return te.TelemetryError()
	}
	return full
}

// ErrorType names the innermost error in the chain — "*mysql.MySQLError", not
// the "*fmt.wrapError" every layer above it added.
func ErrorType(err error) string {
	for {
		next := errors.Unwrap(err)
		if next == nil {
			return reflect.TypeOf(err).String()
		}
		err = next
	}
}

// withError adds err's message, type and any context it carries to data. An
// error carries context by implementing TelemetryFields; keys the caller already
// set win.
func withError(data map[string]any, err error, withStack bool) map[string]any {
	if data == nil {
		data = make(map[string]any, 8)
	}
	if err != nil {
		data["error"] = ErrorText(err)
		data["error_type"] = ErrorType(err)
		var tf interface{ TelemetryFields() map[string]any }
		if errors.As(err, &tf) {
			for k, v := range tf.TelemetryFields() {
				if _, exists := data[k]; !exists {
					data[k] = v
				}
			}
		}
	}
	if withStack {
		data["stack_trace"] = stack()
	}
	return data
}

func stack() string {
	s := debug.Stack()
	if len(s) > MAX_STACK_BYTES {
		s = s[:MAX_STACK_BYTES]
	}
	return string(s)
}

// emit is the one place an event leaves this package. skip is the
// runtime.Caller depth of the code the event is about.
func emit(ctx context.Context, level, name string, data map[string]any, skip int) {
	if ctx == nil {
		ctx = context.Background()
	}
	if data == nil {
		data = make(map[string]any, 6)
	}
	if id := identity.Load(); id != nil {
		data["mon_role"] = id.role
		data["mon_zone"] = id.zone
	}
	if _, set := data["source_file"]; !set {
		if pc, file, line, ok := runtime.Caller(skip); ok {
			data["source_file"] = filepath.Base(file)
			data["source_line"] = line
			if fn := runtime.FuncForPC(pc); fn != nil {
				data["source_func"] = shortFuncName(fn.Name())
			}
		}
	}
	scrubFreeText(data)

	monitor.Emit(ctx, name, data, monitor.WithLevel(level))

	if printable(level) {
		log.Print(formatLine(level, name, data))
	}
}

// printable is the stdout policy described on the package.
func printable(level string) bool {
	if !initialised.Load() {
		return level != monitor.LevelDebug
	}
	if sdkStdout.Load() {
		return false // the SDK already printed it, as NDJSON
	}
	switch level {
	case monitor.LevelWarn, monitor.LevelError, monitor.LevelFatal:
		return true
	case monitor.LevelInfo:
		return booting.Load()
	}
	return false
}

// shortFuncName turns "github.com/aidenappl/monitor-core/services.(*Batcher).flush"
// into "services.(*Batcher).flush".
func shortFuncName(full string) string {
	if i := strings.LastIndex(full, "/"); i >= 0 {
		return full[i+1:]
	}
	return full
}
