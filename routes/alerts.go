package routes

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/aidenappl/monitor-core/alerts"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/gorilla/mux"
)

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

	var rule alerts.Rule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	updated, err := alerts.UpdateRule(r.Context(), id, rule)
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

	value, err := alerts.EvaluateRuleNow(r.Context(), &ruleWithState.Rule)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to evaluate alert rule", err)
		return
	}

	firing := alerts.CheckCondition(value, ruleWithState.Condition, ruleWithState.Threshold)

	responder.New(w, map[string]interface{}{
		"value":     value,
		"threshold": ruleWithState.Threshold,
		"condition": ruleWithState.Condition,
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
