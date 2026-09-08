package apikeys

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

// Errors callers are expected to branch on. Both are CLIENT mistakes, not server
// faults, and they exist because routes/api_keys.go otherwise maps every failure
// from Create to a 500 — which migration 117 turned from a cosmetic wart into a
// real one by adding UNIQUE(project_id, name) where no uniqueness on name had
// existed before. Naming a key twice in one project is now a routine, correctable
// user action, and answering it with "internal server error" tells the caller
// nothing about what to change.
var (
	// ErrUnknownProject means the named project does not exist in this zone.
	ErrUnknownProject = errors.New("unknown project")
	// ErrDuplicateKeyName means the project already owns a key by that name.
	ErrDuplicateKeyName = errors.New("duplicate api key name")
)

// MYSQL_ERR_DUPLICATE_ENTRY is MariaDB's ER_DUP_ENTRY.
const MYSQL_ERR_DUPLICATE_ENTRY = 1062

// isDuplicateEntry reports whether err is a unique-constraint violation.
//
// The driver is imported for its error type only; db/sql.go still owns the
// blank import that registers it. Matching on the numeric code rather than the
// message keeps this working across MariaDB locales, which translate the text.
func isDuplicateEntry(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == MYSQL_ERR_DUPLICATE_ENTRY
}

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

// cachedKey stores the key ID, scope and tenant binding for fast validation.
//
// The project is cached alongside the scope rather than looked up on use, and
// that is the point of the whole structure: ingest resolves a project on every
// single event, so a per-request query would put MariaDB on the hot path of a
// store whose entire design is to keep it off there.
type cachedKey struct {
	ID          string
	Name        string
	Scope       Scope
	ProjectID   int64
	ProjectSlug string
}

// Identity is the resolved owner of a valid API key. Name is carried so an
// audited action can be attributed to a recognisable actor ("monitor-mcp")
// rather than an opaque id.
//
// ProjectID and ProjectSlug are the tenant this credential files under. They are
// the SERVER's answer to "whose data is this", derived from the row behind the
// presented key — never read from the request — for the same reason
// Event.IssueID is never trusted from a client: a caller that could name its own
// project could file its events under someone else's, and every downstream
// number would be wrong with nothing to distinguish it from real traffic.
//
// Both are carried, not just the id, because the two callers want different
// things: ClickHouse stores the slug in the LowCardinality(String) project
// column, while anything relational (a future per-project quota, an audit row)
// wants the id. Deriving one from the other at the call site would mean a query
// per event or per request.
type Identity struct {
	ID          string
	Name        string
	Scope       Scope
	ProjectID   int64
	ProjectSlug string
}

// CACHE_REFRESH_INTERVAL is how often the key cache is re-read from MariaDB.
// It is the upper bound on how long a revoked key keeps working, so it trades
// one small query per interval against that window.
const CACHE_REFRESH_INTERVAL = 30 * time.Second

// cache stores hashed keys for fast validation without DB lookups on every request.
// Initialised empty rather than left nil so Create's cache write cannot panic on
// a nil map when the first load failed.
var (
	cache   = make(map[string]cachedKey) // key_hash -> {id, name, scope}
	cacheMu sync.RWMutex
)

// Init loads the key cache from MariaDB and starts the background refresher.
// The api_keys table is created by the MariaDB migration runner
// (db.RunMigrations), not here.
//
// The refresher starts even when the initial load fails: a MariaDB blip during
// boot otherwise leaves every DB-stored key permanently unusable, because
// main.go logs Init's error as a warning and serves on regardless.
func Init(ctx context.Context) error {
	err := refreshCache()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in api key cache refresher: %v", r)
			}
		}()
		runCacheRefresher(ctx)
	}()

	return err
}

// runCacheRefresher re-reads the api_keys table until ctx is cancelled.
//
// Without it the cache is loaded exactly once at boot, so a key revoked
// anywhere other than in this process — by another replica, or straight in the
// database — keeps authenticating until the container restarts. Delete() evicts
// from the local map, which covers a single replica deleting its own key and
// nothing else.
func runCacheRefresher(ctx context.Context) {
	ticker := time.NewTicker(CACHE_REFRESH_INTERVAL)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// refreshCache swaps in a fully built map or returns without
			// touching the old one, so a failed refresh keeps serving the
			// previous keys. That is the right failure mode: a transient
			// MariaDB outage must not revoke every key at once.
			if err := refreshCache(); err != nil {
				log.Printf("apikeys: cache refresh failed, keeping previous cache: %v", err)
			}
		}
	}
}

// refreshCache rebuilds the whole hash->key map from one query.
//
// query.ListAllAPIKeys joins projects, so the tenant binding arrives with the key
// in the same round trip. The alternative — cache project_id and resolve slugs
// separately — would add a second query per refresh and a window in which a key
// is cached with an id whose slug is not yet known, which is exactly the state
// ingest must never observe.
func refreshCache() error {
	keys, err := query.ListAllAPIKeys(db.SQL)
	if err != nil {
		return fmt.Errorf("failed to load api keys: %w", err)
	}

	newCache := make(map[string]cachedKey)
	for _, k := range keys {
		s := k.Scope
		if s != ScopeAdmin && s != ScopeIngest {
			s = ScopeAdmin // backward compat for keys without scope
		}
		newCache[k.KeyHash] = cachedKey{
			ID:          k.ID,
			Name:        k.Name,
			Scope:       s,
			ProjectID:   k.ProjectID,
			ProjectSlug: k.ProjectSlug,
		}
	}

	cacheMu.Lock()
	cache = newCache
	cacheMu.Unlock()
	return nil
}

// Validate checks if a raw API key is valid. Returns true if valid.
//
// It discards both the scope and the identity behind the key, so an authorising
// caller cannot tell an ingest key from an admin one and an auditing caller has
// nothing to attribute the action to. Prefer ValidateWithScope or
// ValidateWithIdentity on any path that authorises or records.
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
	return Identity{
		ID:          entry.ID,
		Name:        entry.Name,
		Scope:       entry.Scope,
		ProjectID:   entry.ProjectID,
		ProjectSlug: entry.ProjectSlug,
	}, true
}

// Create generates a new API key, stores its hash, and returns the raw key (shown once).
//
// projectSlug names the tenant the key files under. It is a required part of the
// row — project_id is NOT NULL — but an EMPTY projectSlug is accepted and falls
// back to env.DefaultProjectSlug, the same project bootstrap seeds and every
// pre-existing key was backfilled to (migration 117). That default is what keeps
// the existing admin UI and monitor-mcp working unchanged: neither sends a
// project yet, and a hard requirement here would break key creation for both on
// the deploy that shipped this rather than at a time anyone chose.
func Create(ctx context.Context, name string, scope Scope, projectSlug string) (*CreateResult, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if scope != ScopeAdmin && scope != ScopeIngest {
		return nil, fmt.Errorf("scope must be 'admin' or 'ingest'")
	}

	project, err := resolveProject(ctx, projectSlug)
	if err != nil {
		return nil, err
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
		ProjectID: project.ID,
		CreatedAt: now,
	})
	if err != nil {
		// MariaDB 1062 is the only failure here a caller can act on, and after
		// migration 117 it has two possible causes: the project already owns a
		// key by this name (uq_api_keys_project_name), or the generated key
		// collided on key_hash. The second is a 256-bit collision — treat 1062
		// as the name clash, because assuming otherwise would report an
		// impossible event and hide the routine one.
		if isDuplicateEntry(err) {
			return nil, fmt.Errorf("%w: %q already exists in project %q", ErrDuplicateKeyName, name, project.Slug)
		}
		return nil, fmt.Errorf("failed to insert api key: %w", err)
	}

	// Update cache. Every field the cache carries has to be written here, not
	// just the ones the insert needed: this entry serves requests for up to
	// CACHE_REFRESH_INTERVAL before the database replaces it. Name was the field
	// this got wrong once — omitting it made every action authenticated by a
	// freshly minted key carry an empty actor label for that window — and the
	// project fields are the same trap with a worse blast radius, since an empty
	// project slug would file the key's first 30 seconds of events under no
	// tenant at all.
	cacheMu.Lock()
	cache[hash] = cachedKey{
		ID:          id,
		Name:        name,
		Scope:       scope,
		ProjectID:   project.ID,
		ProjectSlug: project.Slug,
	}
	cacheMu.Unlock()

	return &CreateResult{
		APIKey: APIKey{
			ID:          id,
			Name:        name,
			Scope:       scope,
			ProjectID:   project.ID,
			ProjectSlug: project.Slug,
			KeyPrefix:   prefix,
			CreatedAt:   now,
		},
		Key: rawKey,
	}, nil
}

// resolveProject turns a project slug into the row a key binds to, defaulting to
// env.DefaultProjectSlug when the caller names none.
//
// The zone comes from env.ZoneSlug because Phase 1 is SINGLE-ZONE, and it is
// looked up rather than assumed because a project slug is unique only WITHIN its
// zone — resolving 'default' without a zone would match whichever row sorted
// first, which is right today and silently wrong the moment a second zone
// exists. This is the same pair of lookups bootstrap.EnsureZoneAndProject does,
// against the same two env vars, so the key lands in the project that boot
// seeded rather than in a different one that happens to share a slug.
//
// Project STATUS is deliberately not checked. The foreign key is the integrity
// boundary here, and bootstrap already set the posture for a retired registry
// row: it logs a warning and carries on stamping, because a retired project that
// still has live events is a state to make visible, not one to start rejecting
// writes over. Refusing here would contradict that and would additionally break
// the no-project-named default path the moment someone retired 'default'.
func resolveProject(ctx context.Context, projectSlug string) (*structs.Project, error) {
	// Precedence: what the caller asked for, then what the caller is LOOKING at,
	// then the install default.
	//
	// The middle rung is the one that matters. Without it, a user who has
	// switched the console to "atlas" and mints an ingest key gets a key bound to
	// "default" — so the service they wire it into reports into a project they
	// were not even looking at, and the mistake surfaces days later as an empty
	// dashboard with events piling up somewhere else. The selector on screen is
	// the only expression of intent available at that moment, so it is the right
	// answer when the request itself carries none.
	slug := strings.TrimSpace(projectSlug)
	if slug == "" {
		if selected, ok := scope.GetProject(ctx); ok {
			slug = strings.TrimSpace(selected)
		}
	}
	if slug == "" {
		slug = strings.TrimSpace(env.DefaultProjectSlug)
	}

	zoneSlug := strings.TrimSpace(env.ZoneSlug)
	zone, err := query.GetZoneBySlug(db.SQL, zoneSlug)
	if err != nil {
		return nil, fmt.Errorf("failed to look up zone %q: %w", zoneSlug, err)
	}
	if zone == nil {
		return nil, fmt.Errorf("zone %q does not exist", zoneSlug)
	}

	project, err := query.GetProjectBySlug(db.SQL, zone.ID, slug)
	if err != nil {
		return nil, fmt.Errorf("failed to look up project %q in zone %q: %w", slug, zoneSlug, err)
	}
	if project == nil {
		return nil, fmt.Errorf("%w: project %q in zone %q", ErrUnknownProject, slug, zoneSlug)
	}
	return project, nil
}

// List returns the API keys of the request's project (without the raw key or
// hash).
//
// Scoped to the caller's project rather than the whole install: the page
// rendering this carries a project selector, and a list that ignores it shows
// keys the user cannot mint and did not ask for. Refuses rather than falling
// back to every key if no project resolved — the same posture
// scope.ProjectPredicate takes, and for the same reason.
func List(ctx context.Context) ([]APIKey, error) {
	project, ok := scope.GetProject(ctx)
	if !ok {
		return nil, scope.ErrNoProject
	}
	keys, err := query.ListAPIKeys(db.SQL, strings.TrimSpace(env.ZoneSlug), project)
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

// Delete removes an API key by ID, within the caller's project.
//
// Scoped for the same reason List is, but the stakes are higher: this is a
// DESTRUCTIVE cross-tenant verb. Unscoped, an admin key bound to one project
// could delete another project's ingest credential by id alone and silently stop
// that tenant's services reporting, with a 401 at the producer as the only
// symptom. A key outside the caller's project reads as absent, so the id cannot
// even be probed for existence.
//
// The project is passed to the DELETE as well as the lookup — the mutation
// defends itself rather than trusting the read above it.
func Delete(ctx context.Context, id string) error {
	project, ok := scope.GetProject(ctx)
	if !ok {
		return scope.ErrNoProject
	}

	// Get the hash before deleting so we can remove from cache.
	existing, err := query.GetAPIKeyByID(db.SQL, strings.TrimSpace(env.ZoneSlug), project, id)
	if err != nil {
		return fmt.Errorf("failed to look up api key: %w", err)
	}
	if existing == nil {
		return fmt.Errorf("api key not found")
	}

	if err := query.DeleteAPIKey(db.SQL, strings.TrimSpace(env.ZoneSlug), project, id); err != nil {
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
