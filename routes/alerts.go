package routes

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/aidenappl/monitor-core/alerts"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/gorilla/mux"
)

// WHICH LAYER EACH HANDLER BELOW CALLS, AND WHY IT IS NOT UNIFORM.
//
// The alerting CONFIGURATION moved to MariaDB in migrations 119-122, so most of
// these handlers now go straight to query/ — handler to query, the house shape.
// Three do not, and the exception is not stylistic: alerts.ListRules,
// alerts.GetRule and alerts.DeleteRule each touch BOTH stores, joining a MariaDB
// rule to (or removing it alongside) its ClickHouse alert_states row. Alert
// history is ClickHouse-only and stays on alerts.ListHistory.
//
// So: if a handler here calls `alerts.`, the operation spans two databases. If it
// calls `query.`, it is one MariaDB statement. Nothing in this file forwards
// through a package for the sake of symmetry.
//
// EVERY HANDLER BELOW RESOLVES A PROJECT FIRST, through requireProject — the
// package-level helper defined in routes/issues.go, deliberately not a second
// copy. Migrations 127-130 gave all four alerting tables a `project` column, and
// until then every one of these endpoints read and wrote across every tenant in
// the zone: the rule list, the channel list (with its credentials), the routing
// policies, the alert history, and the SSE stream.
//
// The refusal is a 500, not a 401 or 403, for the reason requireProject's own
// header gives: every /v1 route sits behind QueryAuthMiddleware, which injects a
// project on all three of its branches, so reaching that line means a route was
// registered outside it — a wiring fault in this server, not a bad credential.

// AlertNotifHub is the global alert notification hub (set from main.go)
var AlertNotifHub *alerts.AlertHub

func HandleListAlertRules(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	rules, err := alerts.ListRules(r.Context(), project)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list alert rules", err)
		return
	}
	if rules == nil {
		rules = []alerts.RuleWithState{}
	}
	responder.New(w, rules)
}

func HandleCreateAlertRule(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	var req query.CreateAlertRuleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// The project comes from the credential and is passed alongside the body,
	// never read out of it — CreateAlertRuleRequest has no project field, so a
	// caller cannot file a rule into another tenant.
	created, err := query.CreateAlertRule(db.SQL, project, req)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	responder.New(w, created)
}

func HandleGetAlertRule(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	// A rule in another project reads as ABSENT and therefore 404s here, rather
	// than 403ing — the caller cannot learn that the id exists.
	rule, err := alerts.GetRule(r.Context(), project, id)
	if err != nil {
		responder.Error(w, http.StatusNotFound, "alert rule not found")
		return
	}

	responder.New(w, rule)
}

func HandleUpdateAlertRule(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	var req query.UpdateAlertRuleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	updated, err := query.UpdateAlertRule(db.SQL, project, id, req)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	responder.New(w, updated)
}

func HandleDeleteAlertRule(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	if err := alerts.DeleteRule(r.Context(), project, id); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete alert rule", err)
		return
	}

	responder.New(w, nil, "alert rule deleted")
}

func HandleTestAlertRule(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	// Scoped load, then evaluate on the REQUEST's context. That pairing is what
	// makes the test endpoint report the same number the timer fires on: the
	// rule can only be one this project owns, so its own project and the
	// caller's are the same, and both paths reach the same scoped aggregate.
	ruleWithState, err := alerts.GetRule(r.Context(), project, id)
	if err != nil {
		responder.Error(w, http.StatusNotFound, "alert rule not found")
		return
	}

	value, firing, err := alerts.EvaluateRuleNow(r.Context(), &ruleWithState.AlertRule)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to evaluate alert rule", err)
		return
	}

	responder.New(w, map[string]interface{}{
		"value":     value,
		"threshold": ruleWithState.Threshold,
		"condition": ruleWithState.Condition,
		"type":      ruleWithState.Type,
		"firing":    firing,
	})
}

func HandleListAlertHistory(w http.ResponseWriter, r *http.Request) {
	ruleID := r.URL.Query().Get("rule_id")
	limit := 50
	offset := 0

	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil {
			offset = parsed
		}
	}

	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	history, err := alerts.ListHistory(r.Context(), project, ruleID, limit, offset)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list alert history", err)
		return
	}
	if history == nil {
		history = []alerts.HistoryEntry{}
	}
	responder.New(w, history)
}

func HandleListNotificationChannels(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	channels, err := query.ListNotificationChannels(db.SQL, project)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list notification channels", err)
		return
	}
	responder.New(w, channels)
}

func HandleCreateNotificationChannel(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	var req query.CreateNotificationChannelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	created, err := query.CreateNotificationChannel(db.SQL, project, req)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	responder.New(w, created)
}

func HandleDeleteNotificationChannel(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	if _, err := query.DeleteNotificationChannel(db.SQL, project, id); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete notification channel", err)
		return
	}

	responder.New(w, nil, "notification channel deleted")
}

func HandleTestNotificationChannel(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}

	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	// Scoped, and this is the endpoint where it matters most: it SENDS through
	// the channel's stored credentials. Unscoped, one tenant could fire test
	// pages into another tenant's PagerDuty service by id alone.
	ch, err := query.GetNotificationChannel(db.SQL, project, id)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to fetch notification channel", err)
		return
	}
	if ch == nil {
		responder.Error(w, http.StatusNotFound, "notification channel not found")
		return
	}

	notifier := alerts.NewNotifier()
	if err := notifier.SendTest(ch); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "test notification failed", err)
		return
	}

	responder.New(w, nil, "test notification sent")
}

// HandleStreamAlerts handles GET /v1/alerts/stream (SSE for alert state changes)
func HandleStreamAlerts(w http.ResponseWriter, r *http.Request) {
	if AlertNotifHub == nil {
		http.Error(w, "alert notifications not initialized", http.StatusInternalServerError)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	// Resolved BEFORE the SSE headers go out, so the refusal is still an
	// ordinary JSON error response rather than a half-opened event stream.
	//
	// The project is SERVER-DERIVED and there is no query parameter that reaches
	// it, the rule routes/stream.go states for the event tail. Before this,
	// Subscribe took no filter and every subscriber received every rule's state
	// changes — rule names and the messages built from them, live, with no
	// stored query left behind to notice it by.
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	sub := AlertNotifHub.Subscribe(project)
	if sub == nil {
		http.Error(w, "too many concurrent subscribers", http.StatusServiceUnavailable)
		return
	}
	defer AlertNotifHub.Unsubscribe(sub.ID)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	// Clear the server's WriteTimeout for this connection so the SSE stream is
	// not severed after WriteTimeout (30s in main.go). Relies on the logging
	// middleware exposing Unwrap() so the controller can reach the raw conn.
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})

	flusher.Flush()

	log.Printf("alert SSE subscriber connected: %s (project: %s)", sub.ID, project)

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			log.Printf("alert SSE subscriber disconnected: %s", sub.ID)
			return
		case <-keepalive.C:
			rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		case event, ok := <-sub.Events:
			if !ok {
				return
			}
			data, err := json.Marshal(event)
			if err != nil {
				continue
			}
			rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// ─── Service Groups ───

func HandleListServiceGroups(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	groups, err := query.ListServiceGroups(db.SQL, project)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list service groups", err)
		return
	}
	responder.New(w, groups)
}

func HandleCreateServiceGroup(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	var req query.CreateServiceGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	created, err := query.CreateServiceGroup(db.SQL, project, req)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	responder.New(w, created)
}

func HandleUpdateServiceGroup(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	var req query.UpdateServiceGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	updated, err := query.UpdateServiceGroup(db.SQL, project, id, req)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	responder.New(w, updated)
}

func HandleDeleteServiceGroup(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	if _, err := query.DeleteServiceGroup(db.SQL, project, id); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete service group", err)
		return
	}
	responder.New(w, nil, "service group deleted")
}

// ─── Notification Policies ───

func HandleListPolicies(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	policies, err := query.ListNotificationPolicies(db.SQL, project)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list notification policies", err)
		return
	}
	responder.New(w, policies)
}

func HandleCreatePolicy(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	var req query.CreateNotificationPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	created, err := query.CreateNotificationPolicy(db.SQL, project, req)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	responder.New(w, created)
}

func HandleGetPolicy(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	policy, err := query.GetNotificationPolicy(db.SQL, project, id)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to fetch notification policy", err)
		return
	}
	if policy == nil {
		responder.Error(w, http.StatusNotFound, "notification policy not found")
		return
	}
	responder.New(w, policy)
}

func HandleUpdatePolicy(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	var req query.UpdateNotificationPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	updated, err := query.UpdateNotificationPolicy(db.SQL, project, id, req)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	responder.New(w, updated)
}

func HandleDeletePolicy(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return
	}
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	if err := query.DeleteNotificationPolicy(db.SQL, project, id); err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	responder.New(w, nil, "notification policy deleted")
}

func HandleReorderPolicies(w http.ResponseWriter, r *http.Request) {
	project, ok := requireProject(w, r)
	if !ok {
		return
	}

	var body struct {
		IDs []string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if len(body.IDs) == 0 {
		responder.Error(w, http.StatusBadRequest, "ids are required")
		return
	}
	// An id naming another tenant's policy is rejected as "not found" inside the
	// transaction rather than reordered — the reorder's locking read is itself
	// project-scoped, so a foreign id is never in the candidate set.
	if err := query.ReorderNotificationPolicies(db.SQL, project, body.IDs); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to reorder policies", err)
		return
	}
	responder.New(w, nil, "policies reordered")
}
