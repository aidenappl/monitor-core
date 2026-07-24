package routes

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/aidenappl/monitor-core/alerts"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/gorilla/mux"
)

// AlertNotifHub is the global alert notification hub (set from main.go)
var AlertNotifHub *alerts.AlertHub

func HandleListAlertRules(w http.ResponseWriter, r *http.Request) {
	rules, err := alerts.ListRules(r.Context())
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
	var rule alerts.Rule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	created, err := alerts.CreateRule(r.Context(), rule)
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

	rule, err := alerts.GetRule(r.Context(), id)
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

	var req alerts.UpdateRuleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	updated, err := alerts.UpdateRule(r.Context(), id, req)
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

	if err := alerts.DeleteRule(r.Context(), id); err != nil {
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

	ruleWithState, err := alerts.GetRule(r.Context(), id)
	if err != nil {
		responder.Error(w, http.StatusNotFound, "alert rule not found")
		return
	}

	value, firing, err := alerts.EvaluateRuleNow(r.Context(), &ruleWithState.Rule)
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

	history, err := alerts.ListHistory(r.Context(), ruleID, limit, offset)
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
	channels, err := alerts.ListChannels(r.Context())
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list notification channels", err)
		return
	}
	if channels == nil {
		channels = []alerts.Channel{}
	}
	responder.New(w, channels)
}

func HandleCreateNotificationChannel(w http.ResponseWriter, r *http.Request) {
	var ch alerts.Channel
	if err := json.NewDecoder(r.Body).Decode(&ch); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	created, err := alerts.CreateChannel(r.Context(), ch)
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

	if err := alerts.DeleteChannel(r.Context(), id); err != nil {
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

	ch, err := alerts.GetChannel(r.Context(), id)
	if err != nil {
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

	sub := AlertNotifHub.Subscribe()
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

	log.Printf("alert SSE subscriber connected: %s", sub.ID)

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
	groups, err := alerts.ListServiceGroups(r.Context())
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list service groups", err)
		return
	}
	if groups == nil {
		groups = []alerts.ServiceGroup{}
	}
	responder.New(w, groups)
}

func HandleCreateServiceGroup(w http.ResponseWriter, r *http.Request) {
	var sg alerts.ServiceGroup
	if err := json.NewDecoder(r.Body).Decode(&sg); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	created, err := alerts.CreateServiceGroup(r.Context(), sg)
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
	var sg alerts.ServiceGroup
	if err := json.NewDecoder(r.Body).Decode(&sg); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	updated, err := alerts.UpdateServiceGroup(r.Context(), id, sg)
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
	if err := alerts.DeleteServiceGroup(r.Context(), id); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete service group", err)
		return
	}
	responder.New(w, nil, "service group deleted")
}

// ─── Notification Policies ───

func HandleListPolicies(w http.ResponseWriter, r *http.Request) {
	policies, err := alerts.ListPolicies(r.Context())
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list notification policies", err)
		return
	}
	if policies == nil {
		policies = []alerts.NotificationPolicy{}
	}
	responder.New(w, policies)
}

func HandleCreatePolicy(w http.ResponseWriter, r *http.Request) {
	var p alerts.NotificationPolicy
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	created, err := alerts.CreatePolicy(r.Context(), p)
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
	policy, err := alerts.GetPolicy(r.Context(), id)
	if err != nil {
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
	var p alerts.NotificationPolicy
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	updated, err := alerts.UpdatePolicy(r.Context(), id, p)
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
	if err := alerts.DeletePolicy(r.Context(), id); err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	responder.New(w, nil, "notification policy deleted")
}

func HandleReorderPolicies(w http.ResponseWriter, r *http.Request) {
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
	if err := alerts.ReorderPolicies(r.Context(), body.IDs); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to reorder policies", err)
		return
	}
	responder.New(w, nil, "policies reordered")
}
