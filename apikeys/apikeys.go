package apikeys

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/google/uuid"
)

// Scope defines the permission level of an API key.
type Scope string

const (
	ScopeAdmin  Scope = "admin"  // Full read/write access to all routes
	ScopeIngest Scope = "ingest" // Write-only access to /v1/events ingest
)

type APIKey struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Scope      Scope      `json:"scope"`
	KeyPrefix  string     `json:"key_prefix"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
}

type CreateResult struct {
	APIKey
	Key string `json:"key"`
}

// cachedKey stores the key ID and scope for fast validation.
type cachedKey struct {
	ID    string
	Scope Scope
}

// cache stores hashed keys for fast validation without DB lookups on every request.
var (
	cache   map[string]cachedKey // key_hash -> {id, scope}
	cacheMu sync.RWMutex
)

// Init creates the api_keys table if it doesn't exist and loads the key cache.
func Init(ctx context.Context) error {
	err := db.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+db.Database+`.api_keys (
			id String,
			name String,
			key_hash String,
			key_prefix String,
			scope String DEFAULT 'admin',
			created_at DateTime64(3, 'UTC') DEFAULT now64(3),
			last_used_at Nullable(DateTime64(3, 'UTC'))
		) ENGINE = MergeTree ORDER BY (id) SETTINGS index_granularity = 8192
	`)
	if err != nil {
		return fmt.Errorf("failed to create api_keys table: %w", err)
	}
	return refreshCache(ctx)
}

func refreshCache(ctx context.Context) error {
	rows, err := db.Conn.Query(ctx, fmt.Sprintf("SELECT id, key_hash, scope FROM %s.api_keys", db.Database))
	if err != nil {
		return fmt.Errorf("failed to load api keys: %w", err)
	}
	defer rows.Close()

	newCache := make(map[string]cachedKey)
	for rows.Next() {
		var id, hash, scope string
		if err := rows.Scan(&id, &hash, &scope); err != nil {
			return fmt.Errorf("failed to scan api key: %w", err)
		}
		s := Scope(scope)
		if s != ScopeAdmin && s != ScopeIngest {
			s = ScopeAdmin // backward compat for keys without scope
		}
		newCache[hash] = cachedKey{ID: id, Scope: s}
	}

	cacheMu.Lock()
	cache = newCache
	cacheMu.Unlock()
	return nil
}

// Validate checks if a raw API key is valid. Returns true if valid.
func Validate(rawKey string) bool {
	hash := hashKey(rawKey)
	cacheMu.RLock()
	defer cacheMu.RUnlock()
	_, ok := cache[hash]
	return ok
}

// ValidateWithScope checks if a raw API key is valid and returns its scope.
// Returns empty string if the key is invalid.
func ValidateWithScope(rawKey string) Scope {
	hash := hashKey(rawKey)
	cacheMu.RLock()
	defer cacheMu.RUnlock()
	if entry, ok := cache[hash]; ok {
		return entry.Scope
	}
	return ""
}

// Create generates a new API key, stores its hash, and returns the raw key (shown once).
func Create(ctx context.Context, name string, scope Scope) (*CreateResult, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if scope != ScopeAdmin && scope != ScopeIngest {
		return nil, fmt.Errorf("scope must be 'admin' or 'ingest'")
	}

	rawKey, err := generateKey()
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}

	id := uuid.New().String()
	hash := hashKey(rawKey)
	prefix := rawKey[:12]
	now := time.Now().UTC()

	err = db.Conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.api_keys (id, name, key_hash, key_prefix, scope, created_at) VALUES (?, ?, ?, ?, ?, ?)",
		db.Database,
	), id, name, hash, prefix, string(scope), now)
	if err != nil {
		return nil, fmt.Errorf("failed to insert api key: %w", err)
	}

	// Update cache
	cacheMu.Lock()
	cache[hash] = cachedKey{ID: id, Scope: scope}
	cacheMu.Unlock()

	return &CreateResult{
		APIKey: APIKey{
			ID:        id,
			Name:      name,
			Scope:     scope,
			KeyPrefix: prefix,
			CreatedAt: now,
		},
		Key: rawKey,
	}, nil
}

// List returns all API keys (without the raw key or hash).
func List(ctx context.Context) ([]APIKey, error) {
	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT id, name, key_prefix, scope, created_at, last_used_at FROM %s.api_keys ORDER BY created_at DESC",
		db.Database,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to list api keys: %w", err)
	}
	defer rows.Close()

	var keys []APIKey
	for rows.Next() {
		var k APIKey
		var scope string
		if err := rows.Scan(&k.ID, &k.Name, &k.KeyPrefix, &scope, &k.CreatedAt, &k.LastUsedAt); err != nil {
			return nil, fmt.Errorf("failed to scan api key: %w", err)
		}
		k.Scope = Scope(scope)
		if k.Scope != ScopeAdmin && k.Scope != ScopeIngest {
			k.Scope = ScopeAdmin
		}
		keys = append(keys, k)
	}
	return keys, nil
}

// Delete removes an API key by ID.
func Delete(ctx context.Context, id string) error {
	// Get the hash before deleting so we can remove from cache
	var hash string
	row := db.Conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT key_hash FROM %s.api_keys WHERE id = ?", db.Database,
	), id)
	if err := row.Scan(&hash); err != nil {
		return fmt.Errorf("api key not found")
	}

	err := db.Conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.api_keys DELETE WHERE id = ?", db.Database,
	), id)
	if err != nil {
		return fmt.Errorf("failed to delete api key: %w", err)
	}

	cacheMu.Lock()
	delete(cache, hash)
	cacheMu.Unlock()

	return nil
}

func generateKey() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func hashKey(rawKey string) string {
	h := sha256.Sum256([]byte(rawKey))
	return hex.EncodeToString(h[:])
}
