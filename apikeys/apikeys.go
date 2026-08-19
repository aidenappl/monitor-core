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
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/google/uuid"
)

// Scope and APIKey are aliased to the shared structs types so existing callers
// (routes, middleware) keep using apikeys.APIKey / apikeys.ScopeAdmin unchanged
// while the storage backend moved from ClickHouse to MariaDB.
type Scope = structs.Scope

const (
	ScopeAdmin  = structs.ScopeAdmin
	ScopeIngest = structs.ScopeIngest
)

type APIKey = structs.APIKey

type CreateResult struct {
	APIKey
	Key string `json:"key"`
}

// cachedKey stores the key ID and scope for fast validation.
type cachedKey struct {
	ID    string
	Name  string
	Scope Scope
}

// Identity is the resolved owner of a valid API key. Name is carried so an
// audited action can be attributed to a recognisable actor ("monitor-mcp")
// rather than an opaque id.
type Identity struct {
	ID    string
	Name  string
	Scope Scope
}

// cache stores hashed keys for fast validation without DB lookups on every request.
var (
	cache   map[string]cachedKey // key_hash -> {id, scope}
	cacheMu sync.RWMutex
)

// Init loads the key cache from MariaDB. The api_keys table is created by the
// MariaDB migration runner (db.RunMigrations), not here. The ctx parameter is
// retained for call-site compatibility.
func Init(ctx context.Context) error {
	return refreshCache()
}

func refreshCache() error {
	keys, err := query.ListAPIKeys(db.SQL)
	if err != nil {
		return fmt.Errorf("failed to load api keys: %w", err)
	}

	newCache := make(map[string]cachedKey)
	for _, k := range keys {
		s := k.Scope
		if s != ScopeAdmin && s != ScopeIngest {
			s = ScopeAdmin // backward compat for keys without scope
		}
		newCache[k.KeyHash] = cachedKey{ID: k.ID, Name: k.Name, Scope: s}
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
	identity, ok := ValidateWithIdentity(rawKey)
	if !ok {
		return ""
	}
	return identity.Scope
}

// ValidateWithIdentity checks if a raw API key is valid and returns who it
// belongs to. Prefer this over ValidateWithScope on any path that records an
// actor — attributing a status change or a comment to "monitor-mcp" is the
// difference between an audit trail and a shrug.
func ValidateWithIdentity(rawKey string) (Identity, bool) {
	hash := hashKey(rawKey)
	cacheMu.RLock()
	defer cacheMu.RUnlock()
	entry, ok := cache[hash]
	if !ok {
		return Identity{}, false
	}
	return Identity{ID: entry.ID, Name: entry.Name, Scope: entry.Scope}, true
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

	err = query.CreateAPIKey(db.SQL, query.CreateAPIKeyRequest{
		ID:        id,
		Name:      name,
		KeyHash:   hash,
		KeyPrefix: prefix,
		Scope:     scope,
		CreatedAt: now,
	})
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
	keys, err := query.ListAPIKeys(db.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to list api keys: %w", err)
	}
	for i := range keys {
		if keys[i].Scope != ScopeAdmin && keys[i].Scope != ScopeIngest {
			keys[i].Scope = ScopeAdmin
		}
	}
	return keys, nil
}

// Delete removes an API key by ID.
func Delete(ctx context.Context, id string) error {
	// Get the hash before deleting so we can remove from cache.
	existing, err := query.GetAPIKeyByID(db.SQL, id)
	if err != nil {
		return fmt.Errorf("failed to look up api key: %w", err)
	}
	if existing == nil {
		return fmt.Errorf("api key not found")
	}

	if err := query.DeleteAPIKey(db.SQL, id); err != nil {
		return fmt.Errorf("failed to delete api key: %w", err)
	}

	cacheMu.Lock()
	delete(cache, existing.KeyHash)
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
