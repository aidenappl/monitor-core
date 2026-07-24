package issues

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/google/uuid"
)

// Issue represents a tracked error issue
type Issue struct {
	ID              string     `json:"id"`
	Fingerprint     string     `json:"fingerprint"`
	Service         string     `json:"service"`
	Name            string     `json:"name"`
	Message         string     `json:"message"`
	Path            string     `json:"path"`
	Status          string     `json:"status"`
	OccurrenceCount uint64     `json:"occurrence_count"`
	FirstSeen       time.Time  `json:"first_seen"`
	LastSeen        time.Time  `json:"last_seen"`
	ResolvedAt      *time.Time `json:"resolved_at,omitempty"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// Fingerprint normalization regexes
var (
	uuidRegex   = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	hexRegex    = regexp.MustCompile(`\b0x[0-9a-fA-F]+\b`)
	numberRegex = regexp.MustCompile(`\b\d+\b`)
	urlRegex    = regexp.MustCompile(`https?://[^\s]+`)
)

// Worker-pool configuration for error tracking. Error/fatal events are enqueued
// onto trackQueue (non-blocking) and drained by a small fixed pool of workers, so
// an error storm can't spawn unbounded goroutines and shutdown is respected.
const (
	trackQueueSize = 1000
	trackWorkers   = 4
)

// trackQueue buffers error events awaiting issue tracking. Initialized in Init.
var trackQueue chan trackJob

// trackJob carries a single error event and the context under which it was
// captured for the worker pool to process.
type trackJob struct {
	event *structs.Event
}

// Init creates the issues table if it doesn't exist and starts the error-tracking
// worker pool. The passed ctx is the application shutdown context — workers exit
// when it is cancelled.
func Init(ctx context.Context) error {
	trackQueue = make(chan trackJob, trackQueueSize)
	for i := 0; i < trackWorkers; i++ {
		go trackWorker(ctx)
	}

	if err := db.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+db.Database+`.issues (
			id String,
			fingerprint String,
			service String,
			name String,
			message String,
			path String DEFAULT '',
			status String DEFAULT 'unresolved',
			occurrence_count UInt64 DEFAULT 1,
			first_seen DateTime64(3, 'UTC'),
			last_seen DateTime64(3, 'UTC'),
			resolved_at Nullable(DateTime64(3, 'UTC')),
			updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
		) ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (id)
	`); err != nil {
		return err
	}

	// Migration: add path column if it doesn't exist
	_ = db.Conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.issues ADD COLUMN IF NOT EXISTS path String DEFAULT ''",
		db.Database,
	))

	return nil
}

// TrackError enqueues an error event for issue tracking. It is non-blocking: if
// the worker queue is full (or not yet initialized), the event is dropped and a
// warning logged rather than blocking the ingestion path or spawning a goroutine.
func TrackError(event *structs.Event) {
	if trackQueue == nil {
		return
	}
	select {
	case trackQueue <- trackJob{event: event}:
	default:
		log.Printf("issues: track queue full, dropping error event %s/%s", event.Service, event.Name)
	}
}

// trackWorker drains the track queue, processing one error event at a time until
// the application context is cancelled.
func trackWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-trackQueue:
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("issues: panic tracking error: %v", r)
					}
				}()
				processError(ctx, job.event)
			}()
		}
	}
}

// processError creates or updates an issue for an error event.
func processError(ctx context.Context, event *structs.Event) {
	path := extractPath(event)
	message := extractMessage(event, path)
	fingerprint := generateFingerprint(event.Service, event.Name, message, path)
	now := time.Now().UTC()

	// Check if issue already exists for this fingerprint
	existing, err := getByFingerprint(ctx, fingerprint)
	if err != nil || existing == nil {
		// Create new issue
		id := uuid.New().String()
		if err := db.Conn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s.issues (id, fingerprint, service, name, message, path, status, occurrence_count, first_seen, last_seen, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			db.Database,
		), id, fingerprint, event.Service, event.Name, message, path, "unresolved", uint64(1), now, now, now); err != nil {
			log.Printf("issues: failed to create issue: %v", err)
		}
		return
	}

	// Update existing issue — increment count, update last_seen
	// If it was resolved, mark as unresolved (regression)
	status := existing.Status
	if status == "resolved" {
		status = "unresolved"
	}

	// NOTE: The read-then-write pattern here is susceptible to race conditions under concurrent
	// error bursts. With ClickHouse's ReplacingMergeTree, the row with the latest updated_at wins
	// during merges, so concurrent increments can cause occurrence_count to go backward or lose
	// increments. This is a known limitation — the approximate count is acceptable for error tracking.
	if err := db.Conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.issues (id, fingerprint, service, name, message, path, status, occurrence_count, first_seen, last_seen, resolved_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		db.Database,
	), existing.ID, existing.Fingerprint, existing.Service, existing.Name, message, existing.Path, status, existing.OccurrenceCount+1, existing.FirstSeen, now, existing.ResolvedAt, now); err != nil {
		log.Printf("issues: failed to update issue %s: %v", existing.ID, err)
	}
}

// List returns issues with optional filters
func List(ctx context.Context, status, service string, limit, offset int) ([]Issue, int, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	// Count query
	countQuery := fmt.Sprintf("SELECT count(DISTINCT id) FROM %s.issues FINAL WHERE 1=1", db.Database)
	var countArgs []interface{}
	if status != "" {
		countQuery += " AND status = ?"
		countArgs = append(countArgs, status)
	}
	if service != "" {
		countQuery += " AND service = ?"
		countArgs = append(countArgs, service)
	}

	var total uint64
	if err := db.Conn.QueryRow(ctx, countQuery, countArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count query failed: %w", err)
	}

	// Data query
	dataQuery := fmt.Sprintf(
		"SELECT id, fingerprint, service, name, message, path, status, occurrence_count, first_seen, last_seen, resolved_at, updated_at FROM %s.issues FINAL WHERE 1=1",
		db.Database,
	)
	var dataArgs []interface{}
	if status != "" {
		dataQuery += " AND status = ?"
		dataArgs = append(dataArgs, status)
	}
	if service != "" {
		dataQuery += " AND service = ?"
		dataArgs = append(dataArgs, service)
	}
	dataQuery += fmt.Sprintf(" ORDER BY last_seen DESC LIMIT %d OFFSET %d", limit, offset)

	rows, err := db.Conn.Query(ctx, dataQuery, dataArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list issues: %w", err)
	}
	defer rows.Close()

	var issues []Issue
	for rows.Next() {
		var issue Issue
		if err := rows.Scan(&issue.ID, &issue.Fingerprint, &issue.Service, &issue.Name, &issue.Message, &issue.Path, &issue.Status, &issue.OccurrenceCount, &issue.FirstSeen, &issue.LastSeen, &issue.ResolvedAt, &issue.UpdatedAt); err != nil {
			return nil, 0, fmt.Errorf("failed to scan issue: %w", err)
		}
		issues = append(issues, issue)
	}
	return issues, int(total), nil
}

// Get returns an issue by ID
func Get(ctx context.Context, id string) (*Issue, error) {
	row := db.Conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT id, fingerprint, service, name, message, path, status, occurrence_count, first_seen, last_seen, resolved_at, updated_at FROM %s.issues FINAL WHERE id = ?",
		db.Database,
	), id)

	var issue Issue
	if err := row.Scan(&issue.ID, &issue.Fingerprint, &issue.Service, &issue.Name, &issue.Message, &issue.Path, &issue.Status, &issue.OccurrenceCount, &issue.FirstSeen, &issue.LastSeen, &issue.ResolvedAt, &issue.UpdatedAt); err != nil {
		return nil, fmt.Errorf("issue not found")
	}
	return &issue, nil
}

// UpdateStatus updates the status of an issue
func UpdateStatus(ctx context.Context, id, status string) error {
	existing, err := Get(ctx, id)
	if err != nil {
		return err
	}

	validStatuses := map[string]bool{
		"unresolved": true,
		"resolved":   true,
		"ignored":    true,
	}
	if !validStatuses[status] {
		return fmt.Errorf("invalid status: %s (must be unresolved, resolved, or ignored)", status)
	}

	now := time.Now().UTC()
	var resolvedAt *time.Time
	if status == "resolved" {
		resolvedAt = &now
	}

	return db.Conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.issues (id, fingerprint, service, name, message, path, status, occurrence_count, first_seen, last_seen, resolved_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		db.Database,
	), existing.ID, existing.Fingerprint, existing.Service, existing.Name, existing.Message, existing.Path, status, existing.OccurrenceCount, existing.FirstSeen, existing.LastSeen, resolvedAt, now)
}

// GetFingerprint returns the fingerprint for an issue (for event lookup)
func GetFingerprint(ctx context.Context, id string) (string, error) {
	issue, err := Get(ctx, id)
	if err != nil {
		return "", err
	}
	return issue.Fingerprint, nil
}

func getByFingerprint(ctx context.Context, fingerprint string) (*Issue, error) {
	row := db.Conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT id, fingerprint, service, name, message, path, status, occurrence_count, first_seen, last_seen, resolved_at, updated_at FROM %s.issues FINAL WHERE fingerprint = ?",
		db.Database,
	), fingerprint)

	var issue Issue
	if err := row.Scan(&issue.ID, &issue.Fingerprint, &issue.Service, &issue.Name, &issue.Message, &issue.Path, &issue.Status, &issue.OccurrenceCount, &issue.FirstSeen, &issue.LastSeen, &issue.ResolvedAt, &issue.UpdatedAt); err != nil {
		return nil, err
	}
	return &issue, nil
}

func extractPath(event *structs.Event) string {
	if event.Data == nil {
		return ""
	}
	if p, ok := event.Data["path"]; ok {
		if s, ok := p.(string); ok {
			return s
		}
	}
	if p, ok := event.Data["uri"]; ok {
		if s, ok := p.(string); ok {
			return s
		}
	}
	return ""
}

func extractMessage(event *structs.Event, path string) string {
	if event.Data == nil {
		return event.Name
	}

	// Get the error string
	var errStr string
	if msg, ok := event.Data["error"]; ok {
		if s, ok := msg.(string); ok {
			errStr = s
		}
	}
	if errStr == "" {
		if msg, ok := event.Data["error_message"]; ok {
			if s, ok := msg.(string); ok {
				errStr = s
			}
		}
	}
	if errStr == "" {
		if msg, ok := event.Data["message"]; ok {
			if s, ok := msg.(string); ok {
				errStr = s
			}
		}
	}

	// Build descriptive message when path and error are available
	if path != "" && errStr != "" {
		method := ""
		if m, ok := event.Data["method"]; ok {
			if s, ok := m.(string); ok {
				method = s + " "
			}
		}
		return method + path + ": " + errStr
	}

	if errStr != "" {
		return errStr
	}
	return event.Name
}

func generateFingerprint(service, name, message, path string) string {
	normalized := normalizeMessage(message)
	raw := service + "|" + name + "|" + path + "|" + normalized
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])
}

func normalizeMessage(message string) string {
	// Strip UUIDs
	message = uuidRegex.ReplaceAllString(message, "<UUID>")
	// Strip hex strings
	message = hexRegex.ReplaceAllString(message, "<HEX>")
	// Strip URLs
	message = urlRegex.ReplaceAllString(message, "<URL>")
	// Strip numbers
	message = numberRegex.ReplaceAllString(message, "<N>")
	return message
}
