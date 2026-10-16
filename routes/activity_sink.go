package routes

import (
	"context"
	"net/http"
	"strings"
	"sync"

	"github.com/aidenappl/go-forta/activity"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/sso"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/telemetry"
	"github.com/gorilla/mux"
)

// Activity sink: successful changes a person signed in through SSO makes in
// Monitor — issue triage, comments, alert rules, channels, API keys, admin
// changes — are mirrored to an external sink, the SSO provider's per-user
// activity trail, so "everything this person did" spans platforms.
//
// Opt-in: off unless ACTIVITY_REPORT_URL and ACTIVITY_REPORT_PROVIDER (an
// sso_providers slug) are both set. The sink knows people by that provider's
// subject, so only users with an identity from it are reported, and it
// authenticates with that provider's client id/secret.
//
// Monitor has no general audit table, so this is one middleware keyed on the
// matched route. Unlike a generic mapping it reports ONLY routes named in
// sinkActions: several /v1 POSTs are reads (analytics, timeseries, topn…).
// Fire-and-forget; never fails or slows a request. Identifiers only.

var (
	sinkMu       sync.Mutex
	sinkReporter *activity.Reporter
	sinkClient   string // ClientID + "\x00" + ClientSecret the reporter was built with
)

// activitySink returns a reporter for the configured provider's current
// client, or nil when not configured. Provider rows are editable at runtime,
// so the reporter follows the client.
func activitySink() *activity.Reporter {
	if env.ActivityReportURL == "" || env.ActivityReportProvider == "" {
		return nil
	}
	p, err := sso.LoadProvider(db.SQL, env.ActivityReportProvider)
	if err != nil || p == nil || p.ClientID == "" || p.ClientSecret == "" {
		return nil
	}
	client := p.ClientID + "\x00" + p.ClientSecret
	sinkMu.Lock()
	defer sinkMu.Unlock()
	if sinkReporter != nil && client == sinkClient {
		return sinkReporter
	}
	if old := sinkReporter; old != nil {
		go old.Close(context.Background())
	}
	sinkReporter = activity.New(activity.Config{
		Endpoint:     env.ActivityReportURL,
		ClientID:     p.ClientID,
		ClientSecret: p.ClientSecret,
		OnError: func(action string, err error) {
			telemetry.Warn(context.Background(), "activity_sink.report.failed", map[string]any{"action": action, "error": err.Error()})
		},
	})
	sinkClient = client
	return sinkReporter
}

// sinkActions names every reported route: method + " " + full route template.
// A mutating route missing here is not reported — add it when you add the route.
var sinkActions = map[string]string{
	"PUT /v1/service-repos/{service}":             "service_repo.update",
	"DELETE /v1/service-repos/{service}":          "service_repo.delete",
	"POST /v1/api-keys":                           "api_key.create",
	"PUT /v1/api-keys/{id}":                       "api_key.update",
	"DELETE /v1/api-keys/{id}":                    "api_key.delete",
	"POST /v1/dashboards":                         "dashboard.create",
	"PUT /v1/dashboards/{id}":                     "dashboard.update",
	"DELETE /v1/dashboards/{id}":                  "dashboard.delete",
	"POST /v1/views":                              "saved_view.create",
	"PUT /v1/views/{id}":                          "saved_view.update",
	"DELETE /v1/views/{id}":                       "saved_view.delete",
	"POST /v1/alert-rules":                        "alert_rule.create",
	"PUT /v1/alert-rules/{id}":                    "alert_rule.update",
	"DELETE /v1/alert-rules/{id}":                 "alert_rule.delete",
	"POST /v1/notification-channels":              "notification_channel.create",
	"PUT /v1/notification-channels/{id}":          "notification_channel.update",
	"DELETE /v1/notification-channels/{id}":       "notification_channel.delete",
	"POST /v1/service-groups":                     "service_group.create",
	"PUT /v1/service-groups/{id}":                 "service_group.update",
	"DELETE /v1/service-groups/{id}":              "service_group.delete",
	"POST /v1/notification-policies":              "notification_policy.create",
	"PUT /v1/notification-policies/reorder":       "notification_policy.reorder",
	"PUT /v1/notification-policies/{id}":          "notification_policy.update",
	"DELETE /v1/notification-policies/{id}":       "notification_policy.delete",
	"POST /v1/issues/bulk":                        "issue.bulk_update",
	"PUT /v1/issues/{id}":                         "issue.update",
	"POST /v1/issues/{id}/comments":               "issue_comment.create",
	"PATCH /v1/issues/{id}/comments/{commentID}":  "issue_comment.update",
	"DELETE /v1/issues/{id}/comments/{commentID}": "issue_comment.delete",
	"POST /v1/issues/{id}/links":                  "issue_link.create",
	"DELETE /v1/issues/{id}/links/{linkID}":       "issue_link.delete",
	"PUT /admin/users/{id}":                       "user.update",
	"POST /admin/zones":                           "zone.create",
	"PUT /admin/zones/{id}":                       "zone.update",
	"POST /admin/zones/{id}/retire":               "zone.retire",
	"POST /admin/zones/{id}/projects":             "project.create",
	"PUT /admin/projects/{id}":                    "project.update",
	"POST /admin/projects/{id}/retire":            "project.retire",
	"POST /admin/sso-providers":                   "sso_provider.create",
	"PUT /admin/sso-providers/{slug}":             "sso_provider.update",
	"DELETE /admin/sso-providers/{slug}":          "sso_provider.delete",
}

// sinkWriter records the status; Unwrap keeps http.ResponseController working.
type sinkWriter struct {
	http.ResponseWriter
	status int
}

func (w *sinkWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *sinkWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func (w *sinkWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// ActivitySinkMiddleware reports successful (2xx) named changes by a user
// actor. Mount it after the auth middleware that sets the actor.
func ActivitySinkMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if env.ActivityReportURL == "" || env.ActivityReportProvider == "" {
			next.ServeHTTP(w, r)
			return
		}
		tpl := ""
		if cr := mux.CurrentRoute(r); cr != nil {
			tpl, _ = cr.GetPathTemplate()
		}
		action := sinkActions[r.Method+" "+tpl]
		if action == "" {
			next.ServeHTTP(w, r)
			return
		}
		sw := &sinkWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		if sw.status < 200 || sw.status > 299 {
			return
		}
		actor, ok := middleware.GetActor(r.Context())
		if !ok || actor.Kind != structs.ActorKindUser || actor.UserID == nil {
			return
		}
		sinkReport(*actor.UserID, action, sinkDetails(r))
	})
}

// sinkReport resolves the user's subject at the configured provider and hands
// the event to the reporter, off the request path. A variable for tests.
var sinkReport = func(userID int64, action string, details map[string]string) {
	go func() {
		rep := activitySink()
		if rep == nil {
			return
		}
		// A user with no identity from that provider (password-only, or another
		// IdP) is unknown to the sink. A zone-only process without the identity
		// tables also lands here and skips quietly.
		id, err := query.GetIdentityByUserAndProvider(db.SQL, userID, env.ActivityReportProvider)
		if err != nil || id == nil || id.ProviderUserID == "" {
			return
		}
		rep.Report(id.ProviderUserID, action, activity.Success, details)
	}()
}

// sinkDetails collects identifiers only: route ids, and the zone for data-plane
// changes (issues live in a zone).
func sinkDetails(r *http.Request) map[string]string {
	d := map[string]string{}
	for k, v := range mux.Vars(r) {
		if v != "" {
			d[strings.ToLower(k)] = v
		}
	}
	if strings.HasPrefix(r.URL.Path, "/v1/") && env.ZoneSlug != "" {
		d["zone"] = env.ZoneSlug
	}
	return d
}

// ActivitySinkRoutes returns the reported routes ("METHOD /template"), for the
// router test that holds sinkActions to the real route table.
func ActivitySinkRoutes() []string {
	out := make([]string, 0, len(sinkActions))
	for k := range sinkActions {
		out = append(out, k)
	}
	return out
}
