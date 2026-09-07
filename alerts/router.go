package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/structs"
)

type AlertContext struct {
	RuleID          string
	RuleName        string
	Priority        string
	Service         string
	Env             string
	Status          string // "firing" or "resolved"
	Value           float64
	Message         string
	ServiceGroupIDs []string
}

type Router struct {
	notifier *Notifier
}

func NewRouter() *Router {
	return &Router{notifier: NewNotifier()}
}

func BuildAlertContext(ctx context.Context, rule *structs.AlertRule, status string, value float64, message string) *AlertContext {
	ac := &AlertContext{
		RuleID:   rule.ID,
		RuleName: rule.Name,
		Priority: rule.Priority,
		Status:   status,
		Value:    value,
		Message:  message,
	}

	var filters []structs.QueryFilter
	if rule.QueryFilters != "" && rule.QueryFilters != "[]" {
		if err := json.Unmarshal([]byte(rule.QueryFilters), &filters); err == nil {
			for _, f := range filters {
				if f.Operator != "eq" {
					continue
				}
				val, ok := f.Value.(string)
				if !ok {
					continue
				}
				switch f.Field {
				case "service":
					if ac.Service == "" {
						ac.Service = val
					}
				case "env":
					if ac.Env == "" {
						ac.Env = val
					}
				}
			}
		}
	}

	if ac.Service != "" {
		groupIDs, err := ResolveServiceGroups(ctx, ac.Service)
		if err == nil {
			ac.ServiceGroupIDs = groupIDs
		}
	}
	if ac.ServiceGroupIDs == nil {
		ac.ServiceGroupIDs = []string{}
	}

	return ac
}

func (r *Router) Route(ctx context.Context, alertCtx *AlertContext, rule *structs.AlertRule) error {
	policies, err := query.ListNotificationPolicies(db.SQL)
	if err != nil {
		return fmt.Errorf("failed to list policies: %w", err)
	}

	matched := make(map[string]bool)
	policyMatched := false

	for _, policy := range policies {
		if !policy.Enabled {
			continue
		}

		var matchers PolicyMatchers
		if err := json.Unmarshal([]byte(policy.Matchers), &matchers); err != nil {
			log.Printf("alert router: failed to parse matchers for policy %s: %v", policy.ID, err)
			continue
		}

		if !matchPolicy(alertCtx, &matchers) {
			continue
		}

		policyMatched = true

		var channelIDs []string
		if err := json.Unmarshal([]byte(policy.ChannelIDs), &channelIDs); err != nil {
			log.Printf("alert router: failed to parse channel IDs for policy %s: %v", policy.ID, err)
			continue
		}
		for _, id := range channelIDs {
			matched[id] = true
		}

		if !policy.ContinueMatching {
			break
		}
	}

	// Fall back to rule-level channels if no policies matched
	if !policyMatched {
		var channelIDs []string
		if err := json.Unmarshal([]byte(rule.NotificationChannelIDs), &channelIDs); err == nil {
			for _, id := range channelIDs {
				matched[id] = true
			}
		}
	}

	for chID := range matched {
		// A channel id that no longer resolves is LOGGED AND SKIPPED, which is
		// what it did before and is worth naming now that the store can tell the
		// two cases apart: query.GetNotificationChannel returns (nil, nil) for a
		// deleted channel and an error for a broken read. Both end here, because
		// neither is a reason to abandon the remaining channels of a firing
		// alert. The dangling id itself has no fix at this layer — it lives in a
		// JSON array with no foreign key behind it (see
		// query.DeleteNotificationChannel).
		ch, err := query.GetNotificationChannel(db.SQL, chID)
		if err != nil {
			log.Printf("alert router: failed to get channel %s: %v", chID, err)
			continue
		}
		if ch == nil {
			log.Printf("alert router: channel %s no longer exists, skipping", chID)
			continue
		}

		// Dispatch each channel notification on its own goroutine so a single
		// slow/unreachable channel can't block rule evaluation (the router runs
		// on the evaluator's single goroutine). The notifier already applies its
		// own 10s timeout per send; the recover guards against a panicking
		// notifier taking the process down.
		status := alertCtx.Status
		ruleName := alertCtx.RuleName
		message := alertCtx.Message
		value := alertCtx.Value
		go func(ch *structs.NotificationChannel) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Printf("alert router: panic sending to channel %s: %v", ch.ID, rec)
				}
			}()

			var sendErr error
			if status == "resolved" {
				sendErr = r.notifier.SendResolved(ch, ruleName, message, value)
			} else {
				sendErr = r.notifier.Send(ch, ruleName, message, value)
			}
			if sendErr != nil {
				log.Printf("alert router: failed to send to channel %s: %v", ch.ID, sendErr)
			}
		}(ch)
	}

	return nil
}

func matchPolicy(alertCtx *AlertContext, matchers *PolicyMatchers) bool {
	if matchers.Priority != "" && matchers.Priority != alertCtx.Priority {
		return false
	}

	if len(matchers.Services) > 0 {
		found := false
		for _, s := range matchers.Services {
			if s == alertCtx.Service {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if matchers.ServiceGroup != "" {
		found := false
		for _, gid := range alertCtx.ServiceGroupIDs {
			if gid == matchers.ServiceGroup {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if matchers.Status != "" && matchers.Status != alertCtx.Status {
		return false
	}

	if matchers.Env != "" && matchers.Env != alertCtx.Env {
		return false
	}

	if matchers.RuleName != "" && !strings.Contains(strings.ToLower(alertCtx.RuleName), strings.ToLower(matchers.RuleName)) {
		return false
	}

	return true
}
