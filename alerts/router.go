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

// BuildAlertContext derives the routable facts of a firing rule.
//
// It reads the SERVICE GROUPS of the rule's own project — not of whatever
// project the context carries. The rule row is the authority on whose alert this
// is, and on the timer path there is no context project at all.
func BuildAlertContext(ctx context.Context, rule *structs.AlertRule, status string, value float64, message string) *AlertContext {
	// Loaded unconditionally rather than only when a service is known, so this
	// has ONE shape and buildAlertContextFrom is the only place the membership
	// rule lives. A project's group list is a handful of rows.
	groups, err := query.ListServiceGroups(db.SQL, rule.Project)
	if err != nil {
		// Non-fatal, preserving what ResolveServiceGroups did: a group lookup
		// that fails costs a policy keyed on service_group its match, which is a
		// narrower loss than dropping the alert entirely.
		log.Printf("alert router: failed to resolve service groups for project %s: %v", rule.Project, err)
		groups = nil
	}
	_ = ctx
	return buildAlertContextFrom(groups, rule, status, value, message)
}

// buildAlertContextFrom is the pure half, taking the project's service groups as
// data rather than reading them.
//
// Split out because GET /v1/alert-rules needs the same derivation for every rule
// on the page and must not pay a query per row for it — see ListRules. Sharing
// the function is what keeps the has_destinations badge and the actual routing
// decision from being two different opinions about the same rule.
func buildAlertContextFrom(groups []structs.ServiceGroup, rule *structs.AlertRule, status string, value float64, message string) *AlertContext {
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
		ac.ServiceGroupIDs = matchServiceGroups(groups, ac.Service)
	}
	if ac.ServiceGroupIDs == nil {
		ac.ServiceGroupIDs = []string{}
	}

	return ac
}

// Route dispatches a firing or resolved alert to every channel it matches, and
// reports HOW MANY it actually reached.
//
// THE COUNT IS THE POINT OF THE RETURN VALUE. Every one of the six rules on the
// deployed instance carried `notification_channel_ids: []`, so alerting
// evaluated on schedule, transitioned state, wrote history — and notified
// nobody. Nothing in this function said so: a rule with no destinations took the
// same path as one with them, ran the same loop zero times, and returned nil.
// The WARN below is the line that would have made that visible on the first
// firing rather than on the first missed incident.
//
// Everything reads the RULE's project, not the context's. Route is called from
// the evaluator's timer, which has no request and no credential — the rule row
// is the only thing that knows whose alert this is.
func (r *Router) Route(ctx context.Context, alertCtx *AlertContext, rule *structs.AlertRule) (int, error) {
	_ = ctx

	policies, err := query.ListNotificationPolicies(db.SQL, rule.Project)
	if err != nil {
		return 0, fmt.Errorf("failed to list policies: %w", err)
	}

	delivered := 0
	for _, chID := range matchedChannelIDs(alertCtx, rule, policies) {
		// A channel id that no longer resolves is LOGGED AND SKIPPED, which is
		// what it did before and is worth naming now that the store can tell the
		// two cases apart: query.GetNotificationChannel returns (nil, nil) for a
		// deleted channel and an error for a broken read. Both end here, because
		// neither is a reason to abandon the remaining channels of a firing
		// alert. The dangling id itself has no fix at this layer — it lives in a
		// JSON array with no foreign key behind it (see
		// query.DeleteNotificationChannel).
		//
		// The lookup is scoped to the rule's project, so an id naming another
		// tenant's channel reads as absent and lands in exactly this branch —
		// a cross-scope reference is a missing destination, never a send into
		// somebody else's PagerDuty.
		ch, err := query.GetNotificationChannel(db.SQL, rule.Project, chID)
		if err != nil {
			log.Printf("alert router: failed to get channel %s: %v", chID, err)
			continue
		}
		if ch == nil {
			log.Printf("alert router: channel %s no longer exists, skipping", chID)
			continue
		}
		delivered++

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

	// WARN rather than an error, because there is nothing to fail: the rule
	// evaluated correctly, the state transition is real and belongs in history.
	// What is wrong is the CONFIGURATION, and the only place that becomes
	// visible is here, at the moment it costs somebody a notification. Logged on
	// resolve as well as on fire — a resolution nobody is told about is the same
	// missing signal.
	if delivered == 0 {
		log.Printf("alert router: WARN alert %s fired and matched no notification destination (rule %s, project %s, status %s) — check the rule's notification_channel_ids and the project's notification policies",
			alertCtx.RuleName, rule.ID, rule.Project, alertCtx.Status)
	}

	return delivered, nil
}

// matchedChannelIDs walks the project's policies in routing order and returns
// the channel ids a firing alert reaches, deduplicated, in the order the
// policies named them.
//
// PURE, and deliberately so: it is the one description of what "this rule's
// destinations" means, and both the live dispatch above and the has_destinations
// badge on GET /v1/alert-rules call it. Two implementations of this would drift
// into a badge that says a rule is wired up while the router sends nowhere.
//
// ORDER IS PRESERVED rather than collected into a map, so the result is
// deterministic. The old code iterated a map, which made the dispatch order —
// and therefore anything asserted about it — random.
func matchedChannelIDs(alertCtx *AlertContext, rule *structs.AlertRule, policies []structs.NotificationPolicy) []string {
	seen := map[string]bool{}
	ids := []string{}
	policyMatched := false

	add := func(candidates []string) {
		for _, id := range candidates {
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			ids = append(ids, id)
		}
	}

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
		add(channelIDs)

		if !policy.ContinueMatching {
			break
		}
	}

	// Fall back to rule-level channels if no policies matched.
	//
	// ⚠️ "MATCHED" HERE MEANS MATCHED, NOT DELIVERED, and that asymmetry is what
	// made the seeded default policies so damaging: they matched every alert by
	// priority and carried an empty channel list, so this fallback was skipped
	// and the rule's own channels were never consulted. The seeder now writes
	// them DISABLED (see query.SeedDefaultNotificationPolicies), which is what
	// keeps the `!policy.Enabled` continue above from ever setting policyMatched
	// for a policy that routes nowhere.
	if !policyMatched {
		var channelIDs []string
		if err := json.Unmarshal([]byte(rule.NotificationChannelIDs), &channelIDs); err == nil {
			add(channelIDs)
		}
	}

	return ids
}

// countDestinations reports how many of a rule's matched channel ids resolve to a
// channel that actually exists in the project.
//
// `known` is a set built from one ListNotificationChannels read, so a whole page
// of rules costs one query rather than one per id. It answers the same question
// Route answers by loading each channel — "would this alert reach anybody" —
// which is why it must stay downstream of matchedChannelIDs rather than counting
// the rule's raw notification_channel_ids: a dangling id is not a destination.
func countDestinations(alertCtx *AlertContext, rule *structs.AlertRule, policies []structs.NotificationPolicy, known map[string]bool) int {
	count := 0
	for _, id := range matchedChannelIDs(alertCtx, rule, policies) {
		if known[id] {
			count++
		}
	}
	return count
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
