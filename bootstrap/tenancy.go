package bootstrap

import (
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/structs"
)

// VerifyConfigTenancy reports rows filed under a project this zone does not have.
//
// Migrations 118 and 127-133 backfill their new `project` column with the
// LITERAL 'default', because SQL cannot read the environment. That is right for
// every install that left MON_DEFAULT_PROJECT alone — which is the compiled-in
// value and every install today — and silently wrong for one that overrode it:
// its existing dashboards, alert rules and saved views land under a project
// nobody selects, so they simply vanish from the UI with no error anywhere.
//
// This is the check that turns that into a sentence at boot.
//
// ⚠️ A WARNING, NOT A FATAL, and the asymmetry with the migrations is
// deliberate. The `MODIFY ... NOT NULL` step in each migration is loud because a
// row the backfill MISSED is a corrupt row. A row filed under an unknown project
// is not corrupt — it is orphaned, it is recoverable with one UPDATE, and
// refusing to boot over it would take an install offline for a data problem that
// is not making anything wrong, only invisible. Same posture registry.Init takes.
//
// It reads through raw SQL rather than the query builders on purpose: those are
// project-SCOPED by construction now, so they cannot be asked "what projects
// exist in this table?" — the question this function needs is precisely the one
// the scoped layer is designed to make unaskable.
func VerifyConfigTenancy(engine db.Queryable) {
	known, err := knownProjectSlugs(engine)
	if err != nil {
		log.Printf("⚠️ could not verify config tenancy (this is a diagnostic, not a failure): %v", err)
		return
	}
	if len(known) == 0 {
		// bootstrap.EnsureZoneAndProject runs before this and is fail-fast, so an
		// empty set means something changed underneath a running process. Saying
		// nothing would be worse than saying this.
		log.Printf("⚠️ config tenancy check skipped: this zone has no active projects")
		return
	}

	for _, table := range projectScopedTables() {
		orphans, err := orphanedProjects(engine, table, known)
		if err != nil {
			// A table that does not exist yet is not an error worth shouting
			// about — the migration adding its column may not have run on this
			// install, and the next boot will check again.
			continue
		}
		if len(orphans) == 0 {
			continue
		}
		log.Printf("⚠️ %s holds rows under project(s) %s, which are not active projects in zone %q — "+
			"those rows exist but no session can select them. If MON_DEFAULT_PROJECT was changed after "+
			"these rows were written, re-file them with: UPDATE %s SET project = %q WHERE project = '<old>'",
			table, strings.Join(orphans, ", "), env.ZoneSlug, table, env.DefaultProjectSlug)
	}
}

// projectScopedTables reads the tables to check from the tenancy register, so
// adding a project-scoped table gets it checked without editing this function.
func projectScopedTables() []string {
	var tables []string
	for table, tenancy := range structs.TableTenancy {
		// Only the `monitor.` schema carries a literal `project` column;
		// api_keys reaches its project through a foreign key and is verified by
		// that constraint instead.
		if tenancy == structs.TenancyProject && strings.HasPrefix(table, "monitor.") {
			tables = append(tables, table)
		}
	}
	sort.Strings(tables)
	return tables
}

func knownProjectSlugs(engine db.Queryable) (map[string]bool, error) {
	zone, err := query.GetZoneBySlug(engine, strings.TrimSpace(env.ZoneSlug))
	if err != nil {
		return nil, fmt.Errorf("look up zone %q: %w", env.ZoneSlug, err)
	}
	if zone == nil {
		return nil, fmt.Errorf("zone %q does not exist", env.ZoneSlug)
	}

	rows, err := engine.Query("SELECT slug FROM projects WHERE zone_id = ? AND status = 'active'", zone.ID)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()

	known := map[string]bool{}
	for rows.Next() {
		var slug string
		if err := rows.Scan(&slug); err != nil {
			return nil, err
		}
		known[slug] = true
	}
	return known, rows.Err()
}

// orphanedProjects returns the distinct project values in one table that are not
// active projects in this zone.
//
// The table name is interpolated rather than bound because it is an IDENTIFIER,
// which SQL cannot parameterise. It is safe here for a reason worth stating: the
// values come from structs.TableTenancy, a compiled-in map of string literals,
// and never from a request. No caller can influence it.
func orphanedProjects(engine db.Queryable, table string, known map[string]bool) ([]string, error) {
	rows, err := engine.Query("SELECT DISTINCT project FROM " + table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var orphans []string
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			return nil, err
		}
		if !known[project] {
			orphans = append(orphans, fmt.Sprintf("%q", project))
		}
	}
	sort.Strings(orphans)
	return orphans, rows.Err()
}
