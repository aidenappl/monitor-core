// Package registry is the read-side cache of the tenancy registry: the zone this
// install serves and the projects that live inside it.
//
// It exists because of one call site. middleware.withSessionProject validates a
// project SELECTOR on every authenticated session request, and validating a slug
// against MariaDB per request would put the relational store on the hot path of
// every dashboard poll, every SSE reconnect and every analytics panel — the same
// cost the apikeys cache was built to keep off ingest, arriving from the other
// direction. So this package is deliberately apikeys' twin, down to the shape:
// an in-memory map, rebuilt on a ticker, loaded at Init, keeping the previous map
// when a refresh fails.
//
// It is a package of its own rather than a few helpers in middleware for the
// reason scope/ gives about itself. The value is written by a background
// goroutine and read by an HTTP middleware, and a cache with a ticker inside an
// HTTP package invites the next reader to assume the refresh is request-driven
// and reason about staleness wrongly. It does not belong in scope/ either: scope
// is imported by services/, routes/, issues/ and alerts/, and giving it a MariaDB
// dependency would drag the whole query layer into every one of them in service
// of a predicate that only ever needed a string.
//
// It caches exactly ONE zone — env.ZoneSlug. Phase 1 is single-zone by decision,
// and encoding that here rather than pretending otherwise is the point: a second
// zone means keying the map by zone as well, and that is a change that should be
// visible in this file rather than absorbed silently by a map that already had
// room for it.
package registry

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/structs"
)

// CACHE_REFRESH_INTERVAL is how often the registry is re-read from MariaDB, and
// it is the upper bound on how long a retired project stays selectable. It is
// deliberately the same 30s as apikeys.CACHE_REFRESH_INTERVAL — two caches over
// the same database with two different staleness windows would mean an api key
// and a project could disagree about what exists, for a window nobody chose.
//
// The consequence of the window is much smaller here than there, and it is worth
// saying why so nobody tunes this expecting a security property. A stale api key
// is a live credential. A stale project is a name in a switcher that resolves to
// a tenant whose rows are still in ClickHouse and still that tenant's — retiring
// a project has never been a revocation, since the events survive their row by
// the 30-day TTL and the daily rollup outlives it permanently. Read this number
// as "how long the switcher lags the registry", not as "how long access persists
// after it is withdrawn".
const CACHE_REFRESH_INTERVAL = 30 * time.Second

// PROJECT_PAGE_SIZE and MAX_PROJECT_PAGES bound the load.
//
// The page size is explicit because query.ListProjects clamps an unset Limit to
// db.DEFAULT_LIMIT (50). That is the right default for a UI listing and a silent
// truncation for a cache: a 51st project would simply never be selectable, with
// no error, nothing in the logs and nothing about the symptom pointing at
// pagination. Asking for db.MAX_LIMIT per page and paging until a short page
// arrives removes the cliff.
//
// The page cap is the other half of the same guard. A query that stopped
// advancing — a bad Offset, a driver returning a full page forever — would
// otherwise spin here holding the refresher goroutine, which is a worse failure
// than the truncation being guarded against. Ten thousand projects on a
// single-zone install is not a number to design for; it is a number that means
// something is wrong, so it is reported as an error rather than absorbed.
const (
	PROJECT_PAGE_SIZE = db.MAX_LIMIT
	MAX_PROJECT_PAGES = 20
)

// snapshot is one atomically-replaced view of the registry.
//
// The zone and its projects are swapped TOGETHER rather than held in two
// independent variables, because a reader that saw a newly loaded zone beside the
// previous zone's project map would be validating slugs against a tenant set that
// never existed at any point in time. With one zone the pairing is trivially
// stable; the structure is what keeps it stable when there is more than one.
type snapshot struct {
	zone     *structs.Zone
	projects map[string]structs.Project
}

// current starts with an EMPTY map rather than a nil one so a lookup that
// arrives before the first successful load answers "no" instead of panicking —
// and "no" is the correct answer. An unloaded registry cannot vouch for a slug,
// so every explicit selection is refused while the default project keeps working.
// That is the fail-closed direction: the alternative, honouring an unvalidated
// selection until the cache warms, would mean the one window in which the server
// knows least about the registry is also the window in which it trusts the client
// most.
var (
	current = snapshot{projects: map[string]structs.Project{}}
	mu      sync.RWMutex
)

// Init loads the registry cache from MariaDB and starts the background
// refresher. The zones and projects tables are created by the MariaDB migration
// runner (db.RunMigrations) and seeded by bootstrap.EnsureZoneAndProject, which
// main.go runs — fail-fast — before this.
//
// The refresher starts even when the initial load fails, exactly as apikeys.Init
// does: main.go logs this error as a warning and serves on, so a MariaDB blip
// during boot would otherwise leave every project selection refused until the
// container restarts.
func Init(ctx context.Context) error {
	err := refreshCache()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in registry cache refresher: %v", r)
			}
		}()
		runCacheRefresher(ctx)
	}()

	return err
}

// runCacheRefresher re-reads the registry until ctx is cancelled.
//
// Without it the cache is loaded once at boot, so a project created — or retired
// — anywhere other than in this process keeps the switcher wrong until the
// container restarts. There is no local eviction counterpart to apikeys.Delete
// here, because this package deliberately owns no writes: the registry is seeded
// and managed out of band in this phase, so the ticker is the ONLY thing that
// makes a change visible.
func runCacheRefresher(ctx context.Context) {
	ticker := time.NewTicker(CACHE_REFRESH_INTERVAL)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// refreshCache swaps in a fully built snapshot or returns without
			// touching the old one, so a failed refresh keeps serving the
			// previous registry. That is the right failure mode: a transient
			// MariaDB outage must not make every project unselectable at once
			// and blank the dashboard for everyone holding a project in their URL.
			if err := refreshCache(); err != nil {
				log.Printf("registry: cache refresh failed, keeping previous cache: %v", err)
			}
		}
	}
}

// refreshCache rebuilds the snapshot from the process-wide MariaDB handle.
func refreshCache() error {
	return refreshCacheFrom(db.SQL)
}

// refreshCacheFrom is refreshCache against an explicit engine.
//
// The split exists so this package's tests can drive a real load through a mocked
// Queryable. apikeys does not have it, and AGENTS.md records the consequence:
// its DB half is only covered negatively, "because the cache is package-private
// and needs a live DB to populate". A db.Queryable first argument is the house
// convention everywhere else in the repo; there was never a reason for the cache
// loaders to be the exception.
func refreshCacheFrom(engine db.Queryable) error {
	zoneSlug := strings.TrimSpace(env.ZoneSlug)

	// Resolved by SLUG on every refresh rather than by an id captured at boot.
	// The id is stable — rows are never deleted — but MON_ZONE_SLUG is not
	// boot-only config (see the note on it in env/env.go), and a process that
	// cached the id would keep serving the previous zone's projects after the
	// slug moved, which is the same class of silent mis-attribution the whole
	// tenancy dimension exists to prevent.
	zone, err := query.GetZoneBySlug(engine, zoneSlug)
	if err != nil {
		return fmt.Errorf("failed to look up zone %q: %w", zoneSlug, err)
	}
	if zone == nil {
		// Not a swap to an empty snapshot. bootstrap.EnsureZoneAndProject is
		// fail-fast and runs before Init, so a missing zone at this point means
		// the row was removed underneath a running process — and emptying the
		// cache in response would turn one bad configuration change into every
		// session losing its selected project at once.
		return fmt.Errorf("zone %q does not exist", zoneSlug)
	}

	projects, err := loadProjects(engine, zone.ID)
	if err != nil {
		return err
	}

	mu.Lock()
	current = snapshot{zone: zone, projects: projects}
	mu.Unlock()
	return nil
}

// loadProjects reads every ACTIVE project in one zone, keyed by slug.
//
// Active-only is what makes the map answer the question the caller actually has.
// query.GetProjectBySlug deliberately returns retired rows — a spent slug must
// never read as free — but "does this row exist" and "may a session select it"
// are different questions, and a cache that conflated them would let a retired
// project stay selectable forever with the retirement having no observable
// effect at all.
func loadProjects(engine db.Queryable, zoneID int64) (map[string]structs.Project, error) {
	projects := make(map[string]structs.Project)

	for page := 0; page < MAX_PROJECT_PAGES; page++ {
		batch, err := query.ListProjects(engine, zoneID, query.ListProjectsRequest{
			Limit:  PROJECT_PAGE_SIZE,
			Offset: page * PROJECT_PAGE_SIZE,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to load projects for zone %d: %w", zoneID, err)
		}

		for _, project := range batch {
			projects[project.Slug] = project
		}

		if len(batch) < PROJECT_PAGE_SIZE {
			return projects, nil
		}
	}

	return nil, fmt.Errorf("zone %d returned %d full pages of projects (%d rows) — refusing to cache a truncated registry",
		zoneID, MAX_PROJECT_PAGES, MAX_PROJECT_PAGES*PROJECT_PAGE_SIZE)
}

// LookupProject returns the active project named by slug in this install's zone.
//
// Retired projects are absent by construction (loadProjects asks for active rows
// only), so a caller cannot accidentally read "exists" as "selectable".
func LookupProject(slug string) (structs.Project, bool) {
	mu.RLock()
	defer mu.RUnlock()
	project, ok := current.projects[slug]
	return project, ok
}

// HasActiveProject reports whether slug names a project a session may select.
//
// The empty string is never a project — it is not in the map — which matters
// beyond tidiness: an empty project slug is what a pre-006 ClickHouse row reads
// back as, and scope.Matches refuses the same pairing for the same reason.
func HasActiveProject(slug string) bool {
	_, ok := LookupProject(slug)
	return ok
}
