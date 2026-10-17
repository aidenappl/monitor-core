package alerts

import (
	"context"
	"fmt"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/structs"
)

// RoutingPreview is POST /v1/alert-rules/preview-routing's answer: where a
// draft rule's FIRING alert would go if it fired now.
type RoutingPreview struct {
	// Policies is every policy in the project, in routing order, with its
	// verdict — the ones a stopping match kept the router from reaching
	// included, so a form can show why a later policy never applies.
	Policies []PolicyPreview `json:"policies"`
	// Channels is where the alert would be delivered: existing channels only,
	// in dispatch order.
	Channels []ChannelPreview `json:"channels"`
	// UsedRuleChannels is true when no policy matched and the rule's own
	// notification_channel_ids were used instead.
	UsedRuleChannels bool `json:"used_rule_channels"`
	// MissingChannelIDs are routed-to ids that name no channel in this project.
	// The router skips them, so they are not destinations.
	MissingChannelIDs []string `json:"missing_channel_ids"`
}

type PolicyPreview struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Matched bool   `json:"matched"`
	Reason  string `json:"reason"`
}

type ChannelPreview struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

// PreviewRouting routes a rule that is not stored, against the rule's project's
// policies, channels and service groups. It writes and sends nothing.
//
// Unlike routingInputs (the has_destinations badge), a failed read is an ERROR
// here: the preview's whole purpose is to say where alerts go, and an empty
// answer from a failed policy read would say "nowhere" with confidence.
func PreviewRouting(ctx context.Context, rule *structs.AlertRule) (*RoutingPreview, error) {
	policies, err := query.ListNotificationPolicies(db.SQL, rule.Project)
	if err != nil {
		return nil, fmt.Errorf("failed to list notification policies: %w", err)
	}
	channels, err := query.ListNotificationChannels(db.SQL, rule.Project)
	if err != nil {
		return nil, fmt.Errorf("failed to list notification channels: %w", err)
	}
	groups, err := query.ListServiceGroups(db.SQL, rule.Project)
	if err != nil {
		return nil, fmt.Errorf("failed to list service groups: %w", err)
	}
	return previewRoutingFrom(groups, policies, channels, rule), nil
}

// previewRoutingFrom is the pure half. It derives the alert exactly as the
// evaluator does (buildAlertContextFrom, status "firing") and routes it with
// traceRouting, the function Route itself uses.
func previewRoutingFrom(groups []structs.ServiceGroup, policies []structs.NotificationPolicy, channels []structs.NotificationChannel, rule *structs.AlertRule) *RoutingPreview {
	alertCtx := buildAlertContextFrom(groups, rule, "firing", 0, "")
	trace := traceRouting(alertCtx, rule, policies)

	preview := &RoutingPreview{
		Policies:          make([]PolicyPreview, 0, len(trace.Decisions)),
		Channels:          []ChannelPreview{},
		UsedRuleChannels:  trace.UsedRuleChannels,
		MissingChannelIDs: []string{},
	}
	for _, d := range trace.Decisions {
		preview.Policies = append(preview.Policies, PolicyPreview{
			ID: d.Policy.ID, Name: d.Policy.Name, Matched: d.Matched, Reason: d.Reason,
		})
	}

	byID := make(map[string]*structs.NotificationChannel, len(channels))
	for i := range channels {
		byID[channels[i].ID] = &channels[i]
	}
	for _, id := range trace.ChannelIDs {
		ch, ok := byID[id]
		if !ok {
			preview.MissingChannelIDs = append(preview.MissingChannelIDs, id)
			continue
		}
		preview.Channels = append(preview.Channels, ChannelPreview{ID: ch.ID, Name: ch.Name, Type: ch.Type})
	}
	return preview
}
