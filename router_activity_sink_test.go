package main

import (
	"strings"
	"testing"

	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/routes"
)

// sinkExcluded are mutating routes deliberately NOT reported to the activity
// sink: sign-in/session plumbing the identity provider already records,
// machine traffic, read-only POSTs, and actions that change nothing.
var sinkExcluded = map[string]string{
	"POST /auth/login":                         "sign-in",
	"POST /auth/register":                      "sign-up",
	"POST /auth/refresh":                       "session plumbing",
	"POST /auth/logout":                        "session plumbing",
	"PUT /auth/self":                           "own profile",
	"POST /auth/self/identities/{slug}":        "own sign-in methods",
	"DELETE /auth/self/identities/{slug}":      "own sign-in methods",
	"POST /auth/sso/{slug}/backchannel-logout": "provider → Monitor",
	"POST /v1/events":                          "ingest (API key)",
	"POST /webhooks/github":                    "GitHub",
	"POST /v1/analytics":                       "read",
	"POST /v1/timeseries":                      "read",
	"POST /v1/topn":                            "read",
	"POST /v1/gauge":                           "read",
	"POST /v1/compare":                         "read",
	"POST /v1/alert-rules/{id}/test":           "changes nothing",
	"POST /v1/alert-rules/preview-routing":     "read",
	"POST /v1/notification-channels/{id}/test": "changes nothing",
	"POST /admin/zones/{id}/probe":             "health probe",
}

// Every reported action names a real route (a typo would silently never
// match), and every mutating route is either reported or excluded on purpose.
func TestActivitySinkCoversMutatingRoutes(t *testing.T) {
	allowRegistration(t)
	set := routeSet(t, env.RoleBoth)

	named := map[string]bool{}
	for _, k := range routes.ActivitySinkRoutes() {
		named[k] = true
		if !set[k] {
			t.Errorf("sinkActions names %q, which is not a registered route", k)
		}
	}
	for k := range sinkExcluded {
		if !set[k] {
			t.Errorf("sinkExcluded names %q, which is not a registered route", k)
		}
	}
	for k := range set {
		method := strings.SplitN(k, " ", 2)[0]
		switch method {
		case "POST", "PUT", "PATCH", "DELETE":
		default:
			continue
		}
		if !named[k] && sinkExcluded[k] == "" {
			t.Errorf("mutating route %q is neither in routes.sinkActions nor sinkExcluded", k)
		}
	}
}
