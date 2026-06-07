package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

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

func BuildAlertContext(ctx context.Context, rule *Rule, status string, value float64, message string) *AlertContext {
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

func (r *Router) Route(ctx context.Context, alertCtx *AlertContext, rule *Rule) error {
	policies, err := ListPolicies(ctx)
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
		ch, err := GetChannel(ctx, chID)
		if err != nil {
			log.Printf("alert router: failed to get channel %s: %v", chID, err)
			continue
		}

		var sendErr error
		if alertCtx.Status == "resolved" {
			sendErr = r.notifier.SendResolved(ch, alertCtx.RuleName, alertCtx.Message, alertCtx.Value)
		} else {
			sendErr = r.notifier.Send(ch, alertCtx.RuleName, alertCtx.Message, alertCtx.Value)
		}
		if sendErr != nil {
			log.Printf("alert router: failed to send to channel %s: %v", chID, sendErr)
		}
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
