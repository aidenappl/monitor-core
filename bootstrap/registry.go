package bootstrap

import (
	"fmt"
	"log"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/tools"
)

// EnsureZoneAndProject seeds the single zone and its single project on a fresh
// install, and is a no-op once both rows exist — the same shape as
// EnsureAdminUser. Called from main.go after the MariaDB migrations run.
//
// Phase 1 is deliberately SINGLE-ZONE. This does not enumerate zones, route
// between them or pull a fleet config; it guarantees exactly one zone named by
// MON_ZONE_SLUG and exactly one project named by MON_DEFAULT_PROJECT, so that
// every code path downstream can assume a project always exists. The default
// project is what an api_keys row binds to and what the env master key — which
// has no api_keys row at all — stamps its events with, so a fresh install with
// no project would leave the tenant dimension null on real traffic.
//
// It seeds only what is MISSING. A zone that already exists is left exactly as
// it is, including its display_name and including a status of 'deleted': the
// operator retired it on purpose, and a boot step that silently resurrected a
// retired zone would be a worse surprise than a boot step that does nothing.
func EnsureZoneAndProject(engine db.Queryable) error {
	zoneSlug := strings.TrimSpace(env.ZoneSlug)
	projectSlug := strings.TrimSpace(env.DefaultProjectSlug)

	// Validate before touching the database. A slug is immutable and its name is
	// never reusable, so a typo in MON_ZONE_SLUG that reaches an INSERT is
	// permanent — the row can only ever be retired, never corrected, and its name
	// stays spent forever. Failing the boot is the cheap outcome; the CHECK
	// constraints in 116 would catch a malformed slug anyway, but not a reserved
	// one, and not with an error naming the env var.
	if err := tools.ValidateSlug(zoneSlug); err != nil {
		return fmt.Errorf("invalid MON_ZONE_SLUG: %w", err)
	}
	if err := tools.ValidateSlug(projectSlug); err != nil {
		return fmt.Errorf("invalid MON_DEFAULT_PROJECT: %w", err)
	}

	zone, err := ensureZone(engine, zoneSlug)
	if err != nil {
		return err
	}

	project, err := query.GetProjectBySlug(engine, zone.ID, projectSlug)
	if err != nil {
		return fmt.Errorf("failed to look up project %q in zone %q: %w", projectSlug, zoneSlug, err)
	}
	if project == nil {
		project, err = query.CreateProject(engine, query.CreateProjectRequest{
			ZoneID:      zone.ID,
			Slug:        projectSlug,
			DisplayName: defaultDisplayName(projectSlug),
		})
		if err != nil {
			// Same concurrent-boot race as the zone below.
			project, err = recoverProjectRace(engine, zone.ID, projectSlug, err)
			if err != nil {
				return err
			}
		} else {
			log.Printf("bootstrap: created project %q (id=%d) in zone %q", project.Slug, project.ID, zone.Slug)
		}
	}

	if zone.Status != structs.ZoneStatusActive || project.Status != structs.ProjectStatusActive {
		// Not fatal — the rows exist and everything resolves — but a Monitor whose
		// only zone or only project is retired will file every event under a tenant
		// the UI hides by default, which is worth one loud line at boot.
		log.Printf("bootstrap: WARNING zone %q is %s and project %q is %s — ingestion still stamps this project",
			zone.Slug, zone.Status, project.Slug, project.Status)
	}
	return nil
}

// ensureZone reads the zone and creates it when missing.
//
// The endpoints come from MON_PUBLIC_URL — the origin THIS process answers on.
// A zone serves ingest and query from the same base (different paths), so both
// columns get the same value, and a fresh zone therefore registers itself
// correctly without the operator having to type its own URL back in.
//
// Seeding them is not optional: query.CreateZone requires a valid ingest_url
// since db/migrations/125_zone_endpoints.sql added the columns, so omitting
// them fails the boot outright. That went unnoticed because an install whose
// zones row predates 125 returns above and never reaches CreateZone — only a
// genuinely fresh zone runs this path, which is exactly when getting it wrong
// is least recoverable.
//
// A wrong-but-valid URL here is recoverable: unlike the slug, both columns are
// mutable through the admin registry, and probe.Zone reports `mismatched` when
// the box answering does not claim this slug.
func ensureZone(engine db.Queryable, slug string) (*structs.Zone, error) {
	zone, err := query.GetZoneBySlug(engine, slug)
	if err != nil {
		return nil, fmt.Errorf("failed to look up zone %q: %w", slug, err)
	}
	if zone != nil {
		return zone, nil
	}

	// Named explicitly rather than letting NormalizeEndpointURL report the bare
	// "URL is required": that message sends an operator hunting through the
	// registry for a field they never set, when the fix is one env var.
	endpoint := strings.TrimSpace(env.PublicBaseURL)
	if endpoint == "" {
		return nil, fmt.Errorf("cannot create zone %q: MON_PUBLIC_URL is empty — it is this zone's ingest and query endpoint", slug)
	}

	zone, err = query.CreateZone(engine, query.CreateZoneRequest{
		Slug:        slug,
		DisplayName: defaultDisplayName(slug),
		IngestURL:   endpoint,
		QueryURL:    endpoint,
	})
	if err != nil {
		return recoverZoneRace(engine, slug, err)
	}

	log.Printf("bootstrap: created zone %q (id=%d)", zone.Slug, zone.ID)
	return zone, nil
}

// recoverZoneRace turns a lost creation race into a success.
//
// Read-then-create is not atomic, and monitor-core runs more than one replica in
// production — two containers restarting together both read "no zone" and both
// insert. Exactly one wins; the loser gets a duplicate-key error on
// uq_zones_slug, which is not a failure of anything, since the row it wanted now
// exists. Re-reading is the check: if the zone is there, the race is the
// explanation and the boot continues. If it is not, the original error was
// something else and is returned unchanged rather than being swallowed.
//
// The re-read rather than a driver error-code test is deliberate — it needs no
// dependency on go-sql-driver's error type, and it establishes the thing that
// actually matters (the row exists) instead of a proxy for it.
func recoverZoneRace(engine db.Queryable, slug string, createErr error) (*structs.Zone, error) {
	zone, getErr := query.GetZoneBySlug(engine, slug)
	if getErr != nil || zone == nil {
		return nil, fmt.Errorf("failed to create zone %q: %w", slug, createErr)
	}
	log.Printf("bootstrap: zone %q already created by another process — continuing", slug)
	return zone, nil
}

// recoverProjectRace is recoverZoneRace for uq_projects_zone_slug.
func recoverProjectRace(engine db.Queryable, zoneID int64, slug string, createErr error) (*structs.Project, error) {
	project, getErr := query.GetProjectBySlug(engine, zoneID, slug)
	if getErr != nil || project == nil {
		return nil, fmt.Errorf("failed to create project %q in zone %d: %w", slug, zoneID, createErr)
	}
	log.Printf("bootstrap: project %q already created by another process — continuing", slug)
	return project, nil
}

// defaultDisplayName derives a first label from a slug: "trailblaze" →
// "Trailblaze", "second-zone" → "Second Zone".
//
// It only ever runs on the row it creates. display_name is mutable and
// non-unique precisely so the operator can replace this with whatever the thing
// is really called, without touching the immutable slug underneath.
func defaultDisplayName(slug string) string {
	words := strings.Split(slug, "-")
	for i, word := range words {
		if word == "" {
			continue
		}
		words[i] = strings.ToUpper(word[:1]) + word[1:]
	}
	return strings.Join(words, " ")
}
