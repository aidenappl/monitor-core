package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/gorilla/mux"
)

// This file is the contract for MON_ROLE, and the contract has two halves.
//
// The half that protects production is `both`: the deployed configuration, the
// default, and therefore the one configuration in which this change must do
// nothing whatsoever. TestBothIsTheDeployedSurface pins its entire route
// inventory, so a future edit that gates a route on a role cannot quietly remove
// it from the running install — it fails here first.
//
// The half that makes the split real is that each single-plane role must not
// register a route it cannot serve. A route that exists and cannot work does not
// return a tidy 500: db.Conn is an INTERFACE, so an app process reaching for
// ClickHouse panics on a nil method call and net/http answers by closing the
// connection with no response at all. 404 is the honest answer, and only
// registration-time gating produces it.

// routeSet walks a built router and returns every "METHOD /path" it will match.
//
// GetPathTemplate/GetMethods both error on routes that carry no such matcher —
// the /v1 PathPrefix route is one — and those are skipped rather than treated as
// a failure: they match nothing on their own.
func routeSet(t *testing.T, role env.Role) map[string]bool {
	t.Helper()

	set := map[string]bool{}
	err := buildRouter(role).Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
		path, err := route.GetPathTemplate()
		if err != nil {
			return nil
		}
		methods, err := route.GetMethods()
		if err != nil {
			return nil
		}
		for _, method := range methods {
			set[fmt.Sprintf("%s %s", method, path)] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to walk the %s router: %v", role, err)
	}
	return set
}

// allowRegistration forces MON_ALLOW_REGISTRATION on for the duration of a test.
// buildRouter reads it, and it is false in a test binary that never calls
// env.Load, so without this the pinned inventory below would be missing
// POST /auth/register and would not notice if the real one lost it too.
func allowRegistration(t *testing.T) {
	t.Helper()
	original := env.AllowRegistration
	env.AllowRegistration = true
	t.Cleanup(func() { env.AllowRegistration = original })
}

// deployedSurface is every route monitor-core serves today, in the deployed
// `both` configuration. Generated from the router, verified against the
// registration sequence in main.go as it stood before roles existed.
//
// Adding a route means adding a line here. That is the point: this list is the
// only thing standing between "gated a route behind a role" and "removed a route
// from production", and the two are the same edit until someone reads this file.
var deployedSurface = []string{
	"DELETE /admin/sso-providers/{slug}",
	"DELETE /auth/self/identities/{slug}",
	"DELETE /v1/alert-rules/{id}",
	"DELETE /v1/api-keys/{id}",
	"DELETE /v1/dashboards/{id}",
	"DELETE /v1/issues/{id}/comments/{commentID}",
	"DELETE /v1/issues/{id}/links/{linkID}",
	"DELETE /v1/notification-channels/{id}",
	"DELETE /v1/notification-policies/{id}",
	"DELETE /v1/service-groups/{id}",
	"DELETE /v1/service-repos/{service}",
	"DELETE /v1/views/{id}",
	"GET /admin/sso-providers",
	"GET /auth/self",
	"GET /auth/self/identities",
	"GET /auth/sso/callback",
	"GET /auth/sso/config",
	"GET /auth/sso/icon/{slug}",
	"GET /auth/sso/{slug}/login",
	"GET /health",
	"GET /ready",
	"GET /v1/alert-history",
	"GET /v1/alert-rules",
	"GET /v1/alert-rules/{id}",
	"GET /v1/alerts/stream",
	"GET /v1/analytics",
	"GET /v1/api-keys",
	"GET /v1/dashboards",
	"GET /v1/dashboards/{id}",
	"GET /v1/data/keys",
	"GET /v1/data/values",
	"GET /v1/events",
	"GET /v1/events/stream",
	"GET /v1/issues",
	"GET /v1/issues/{id}",
	"GET /v1/issues/{id}/events",
	"GET /v1/issues/{id}/history",
	"GET /v1/issues/{id}/links",
	"GET /v1/issues/{id}/timeline",
	"GET /v1/labels/{label}/values",
	"GET /v1/notification-channels",
	"GET /v1/notification-policies",
	"GET /v1/notification-policies/{id}",
	"GET /v1/service-groups",
	"GET /v1/service-repos",
	"GET /v1/service-repos/{service}",
	"GET /v1/timeseries",
	"GET /v1/views",
	"GET /v1/zones",
	"GET /v1/zones/{zone}/projects",
	"PATCH /v1/issues/{id}/comments/{commentID}",
	"POST /admin/projects/{id}/retire",
	"POST /admin/sso-providers",
	"POST /admin/zones",
	"POST /admin/zones/{id}/probe",
	"POST /admin/zones/{id}/projects",
	"POST /admin/zones/{id}/retire",
	"POST /auth/login",
	"POST /auth/logout",
	"POST /auth/refresh",
	"POST /auth/register",
	"POST /auth/self/identities/{slug}",
	"POST /auth/sso/forta/backchannel-logout",
	"POST /v1/alert-rules",
	"POST /v1/alert-rules/{id}/test",
	"POST /v1/analytics",
	"POST /v1/api-keys",
	"POST /v1/compare",
	"POST /v1/dashboards",
	"POST /v1/events",
	"POST /v1/gauge",
	"POST /v1/issues/{id}/comments",
	"POST /v1/issues/{id}/links",
	"POST /v1/notification-channels",
	"POST /v1/notification-channels/{id}/test",
	"POST /v1/notification-policies",
	"POST /v1/service-groups",
	"POST /v1/timeseries",
	"POST /v1/topn",
	"POST /v1/views",
	"POST /webhooks/github",
	"PUT /admin/projects/{id}",
	"PUT /admin/sso-providers/{slug}",
	"PUT /admin/zones/{id}",
	"PUT /auth/self",
	"PUT /v1/alert-rules/{id}",
	"PUT /v1/dashboards/{id}",
	"PUT /v1/issues/{id}",
	"PUT /v1/notification-policies/reorder",
	"PUT /v1/notification-policies/{id}",
	"PUT /v1/service-groups/{id}",
	"PUT /v1/service-repos/{service}",
}

// dataPlaneOnly are the routes an app process MUST NOT register. Every one of
// them reaches ClickHouse (directly, or through an occurrence-history read on a
// MariaDB row), the ingest queue, or an SSE hub that no control plane creates.
var dataPlaneOnly = []string{
	"POST /v1/events",               // ingest queue + event hub
	"GET /v1/events",                // ClickHouse
	"GET /v1/events/stream",         // event hub
	"GET /v1/labels/{label}/values", // ClickHouse
	"GET /v1/data/keys",             // ClickHouse
	"GET /v1/data/values",           // ClickHouse
	"POST /v1/analytics",            // ClickHouse
	"GET /v1/analytics",             // ClickHouse
	"POST /v1/timeseries",           // ClickHouse
	"GET /v1/timeseries",            // ClickHouse
	"POST /v1/topn",                 // ClickHouse
	"POST /v1/gauge",                // ClickHouse
	"POST /v1/compare",              // ClickHouse
	"GET /v1/dashboards",            // ClickHouse-backed store
	"GET /v1/views",                 // ClickHouse-backed store
	"GET /v1/alert-rules",           // ClickHouse-backed store
	"GET /v1/alert-history",         // ClickHouse-backed store
	"GET /v1/notification-channels", // ClickHouse-backed store
	"GET /v1/service-groups",        // ClickHouse-backed store
	"GET /v1/notification-policies", // ClickHouse-backed store
	"GET /v1/alerts/stream",         // alert notification hub
	"GET /v1/issues",                // MariaDB + ClickHouse history decoration
	"GET /v1/issues/{id}",           // MariaDB + ClickHouse sparkline
	"GET /v1/issues/{id}/events",    // ClickHouse
	"GET /v1/issues/{id}/history",   // ClickHouse rollup
	"POST /webhooks/github",         // writes the zone-owned issue tracker
}

// controlPlaneOnly are the routes a zone MUST NOT register: everything that
// issues a Monitor session or administers the identity subsystem. A zone still
// VERIFIES an access token — QueryAuthMiddleware falls back to SessionMiddleware
// on /v1 — but it seeds no admin, installs no SSO checkpoint and owns no
// provider rows, so a login served here could only ever fail.
var controlPlaneOnly = []string{
	"POST /auth/login",
	"POST /auth/register",
	"POST /auth/refresh",
	"POST /auth/logout",
	"GET /auth/self",
	"PUT /auth/self",
	"GET /auth/self/identities",
	"POST /auth/self/identities/{slug}",
	"DELETE /auth/self/identities/{slug}",
	"GET /auth/sso/config",
	"GET /auth/sso/{slug}/login",
	"GET /auth/sso/icon/{slug}",
	"GET /auth/sso/callback",
	"POST /auth/sso/forta/backchannel-logout",
	"GET /admin/sso-providers",
	"POST /admin/sso-providers",
	"PUT /admin/sso-providers/{slug}",
	"DELETE /admin/sso-providers/{slug}",

	// The tenancy registry WRITE surface. A zone must not be able to mint or edit
	// registry rows: the registry is the map that says which box a zone's data
	// lives on, and a process that can write its own entry can point a row at
	// itself — or at another tenant — with every read through it then returning
	// the wrong data under a name that still looks right. The READS
	// (GET /v1/zones, GET /v1/zones/{zone}/projects) stay in both roles, because
	// a switcher has to work wherever it is served from.
	"POST /admin/zones",
	"PUT /admin/zones/{id}",
	"POST /admin/zones/{id}/retire",
	"POST /admin/zones/{id}/probe",
	"POST /admin/zones/{id}/projects",
	"PUT /admin/projects/{id}",
	"POST /admin/projects/{id}/retire",
}

// TestBothIsTheDeployedSurface is the no-op guard. MON_ROLE defaults to `both`,
// so this is the configuration every existing install runs, and its route
// inventory must be exactly what it was before roles existed — not a superset,
// not a subset.
func TestBothIsTheDeployedSurface(t *testing.T) {
	allowRegistration(t)

	got := routeSet(t, env.RoleBoth)

	want := map[string]bool{}
	for _, route := range deployedSurface {
		want[route] = true
	}

	for route := range want {
		if !got[route] {
			t.Errorf("MON_ROLE=both no longer serves %q — this route is live in production", route)
		}
	}
	for route := range got {
		if !want[route] {
			t.Errorf("MON_ROLE=both serves %q, which is not in deployedSurface — add it there if it is intended", route)
		}
	}
}

// TestBothIsTheUnionOfTheTwoPlanes proves the split loses nothing. Every route
// the deployed process serves must be served by app, by zone, or by both; a
// route that fell out of the gating entirely would otherwise show up only as a
// 404 in whichever split topology was deployed first.
func TestBothIsTheUnionOfTheTwoPlanes(t *testing.T) {
	allowRegistration(t)

	both := routeSet(t, env.RoleBoth)
	app := routeSet(t, env.RoleApp)
	zone := routeSet(t, env.RoleZone)

	for route := range both {
		if !app[route] && !zone[route] {
			t.Errorf("%q is served by `both` but by neither plane alone — it would 404 in any split deployment", route)
		}
	}
	for route := range app {
		if !both[route] {
			t.Errorf("app serves %q, which `both` does not — a role must never add a route", route)
		}
	}
	for route := range zone {
		if !both[route] {
			t.Errorf("zone serves %q, which `both` does not — a role must never add a route", route)
		}
	}
}

// TestAppRegistersNothingItCannotServe is the enforcement half of "an app
// process holds no ClickHouse connection". main.go leaves db.Conn nil there, so
// each of these routes would panic on its first request; not registering them
// turns that into a 404, which is a true statement about the plane rather than a
// broken-looking server.
func TestAppRegistersNothingItCannotServe(t *testing.T) {
	allowRegistration(t)

	app := routeSet(t, env.RoleApp)
	for _, route := range dataPlaneOnly {
		if app[route] {
			t.Errorf("MON_ROLE=app registers %q, which needs ClickHouse or the ingest machinery it does not have", route)
		}
	}

	// The other direction, so this cannot pass by registering nothing at all.
	for _, route := range controlPlaneOnly {
		if !app[route] {
			t.Errorf("MON_ROLE=app does not register %q; the control plane owns identity", route)
		}
	}
	for _, route := range []string{"GET /health", "GET /ready"} {
		if !app[route] {
			t.Errorf("MON_ROLE=app does not register %q; probes must exist in every role", route)
		}
	}
}

// TestZoneRegistersNoIdentitySurface is the mirror: a zone mints no sessions and
// administers no providers, and must not offer a door that only leads to a 401.
func TestZoneRegistersNoIdentitySurface(t *testing.T) {
	allowRegistration(t)

	zone := routeSet(t, env.RoleZone)
	for _, route := range controlPlaneOnly {
		if zone[route] {
			t.Errorf("MON_ROLE=zone registers %q; a zone verifies sessions but does not issue or administer them", route)
		}
	}

	for _, route := range dataPlaneOnly {
		if !zone[route] {
			t.Errorf("MON_ROLE=zone does not register %q; the zone owns events, alerts and issues", route)
		}
	}
	for _, route := range []string{"GET /health", "GET /ready"} {
		if !zone[route] {
			t.Errorf("MON_ROLE=zone does not register %q; probes must exist in every role", route)
		}
	}
}

// TestNoRoleRegistersADuplicatePath is a cheap sanity check on the gating edits
// themselves: wrapping runs of registrations in `if` blocks is exactly the kind
// of change that duplicates a line while moving a brace, and mux would happily
// register the same route twice with the shadowed second one never matching.
func TestNoRoleRegistersADuplicatePath(t *testing.T) {
	allowRegistration(t)

	for _, role := range []env.Role{env.RoleApp, env.RoleZone, env.RoleBoth} {
		seen := map[string]int{}
		err := buildRouter(role).Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
			path, err := route.GetPathTemplate()
			if err != nil {
				return nil
			}
			methods, err := route.GetMethods()
			if err != nil {
				return nil
			}
			for _, method := range methods {
				seen[fmt.Sprintf("%s %s", method, path)]++
			}
			return nil
		})
		if err != nil {
			t.Fatalf("failed to walk the %s router: %v", role, err)
		}

		duplicates := []string{}
		for route, count := range seen {
			if count > 1 {
				duplicates = append(duplicates, fmt.Sprintf("%s (×%d)", route, count))
			}
		}
		if len(duplicates) > 0 {
			sort.Strings(duplicates)
			t.Errorf("MON_ROLE=%s registers duplicate routes: %s", role, strings.Join(duplicates, ", "))
		}
	}
}

// TestClickHouseUseWithoutAConnectionPanics pins the property the app role is
// built on: db.Conn is an interface, and a nil interface does not degrade
// gracefully — it panics.
//
// This is what makes "an app process simply does not connect" a sufficient
// enforcement rather than a hopeful one. If a future edit registers a
// ClickHouse-backed route in an app process, the failure is loud and immediate,
// not a query against nothing that returns an empty page and reads as "no events
// in this project".
//
// Nothing in this package's tests ever assigns db.Conn, so it is nil here for
// the same reason it is nil in an app process; the guard below says so out loud
// rather than assuming it.
func TestClickHouseUseWithoutAConnectionPanics(t *testing.T) {
	if db.Conn != nil {
		t.Skip("db.Conn is non-nil in this test binary; nothing to prove about the nil case")
	}

	defer func() {
		if recover() == nil {
			t.Error("using a nil db.Conn did not panic — a control plane could then query the event store silently")
		}
	}()

	_ = db.Conn.Exec(context.Background(), "SELECT 1")
	t.Error("unreachable: the call above must panic")
}
