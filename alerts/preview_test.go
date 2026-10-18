package alerts

import (
	"reflect"
	"testing"

	"github.com/aidenappl/monitor-core/structs"
)

func channel(id, name, typ string) structs.NotificationChannel {
	return structs.NotificationChannel{ID: id, Project: "atlas", Name: name, Type: typ}
}

// TestPreviewRoutingFrom pins the preview's verdict per policy: routing order,
// the disabled skip, a matcher's reason, continue_matching letting the walk go
// on, a stopping match marking every later policy "not reached", the rule-level
// fallback, and dangling ids reported apart from channels.
func TestPreviewRoutingFrom(t *testing.T) {
	channels := []structs.NotificationChannel{
		channel("slack", "Ops Slack", "slack"),
		channel("pd", "PagerDuty", "pagerduty"),
		channel("rule-ch", "Rule email", "email"),
	}
	groups := []structs.ServiceGroup{{ID: "g-core", Project: "atlas", Name: "core", Services: `["api"]`}}

	tests := []struct {
		name         string
		rule         structs.AlertRule
		policies     []structs.NotificationPolicy
		wantPolicies []PolicyPreview
		wantChannels []ChannelPreview
		wantFallback bool
		wantMissing  []string
	}{
		{
			name: "order, continue_matching and a stopping match",
			rule: structs.AlertRule{Project: "atlas", Name: "api errors", Priority: "P1", QueryFilters: `[{"field":"service","operator":"eq","value":"api"}]`},
			policies: []structs.NotificationPolicy{
				policy("critical", `{"priority":"P0"}`, `["pd"]`, true, false),
				policy("off", `{}`, `["pd"]`, false, false),
				policy("core group", `{"service_group":"g-core"}`, `["slack"]`, true, true),
				policy("high", `{"priority":"P1"}`, `["pd","slack"]`, true, false),
				policy("catch-all", `{}`, `["rule-ch"]`, true, false),
			},
			wantPolicies: []PolicyPreview{
				{ID: "critical", Name: "critical", Matched: false, Reason: "priority is P1; this policy matches P0"},
				{ID: "off", Name: "off", Matched: false, Reason: "disabled"},
				{ID: "core group", Name: "core group", Matched: true, Reason: "matched"},
				{ID: "high", Name: "high", Matched: true, Reason: "matched; continue_matching is off, so later policies are not consulted"},
				{ID: "catch-all", Name: "catch-all", Matched: false, Reason: `not reached: "high" matched first and does not continue matching`},
			},
			// Deduplicated, in the order the policies named them.
			wantChannels: []ChannelPreview{{ID: "slack", Name: "Ops Slack", Type: "slack"}, {ID: "pd", Name: "PagerDuty", Type: "pagerduty"}},
			wantMissing:  []string{},
		},
		{
			name: "no policy matched: the rule's own channels, dangling ids reported",
			rule: structs.AlertRule{Project: "atlas", Name: "x", Priority: "P3", NotificationChannelIDs: `["rule-ch","deleted"]`},
			policies: []structs.NotificationPolicy{
				policy("services", `{"services":["web"]}`, `["pd"]`, true, false),
				policy("env", `{"env":"prod"}`, `["pd"]`, true, false),
			},
			wantPolicies: []PolicyPreview{
				{ID: "services", Name: "services", Matched: false, Reason: "the rule filters on no single service (service eq); this policy matches web"},
				{ID: "env", Name: "env", Matched: false, Reason: "the rule filters on no single env (env eq); this policy matches prod"},
			},
			wantChannels: []ChannelPreview{{ID: "rule-ch", Name: "Rule email", Type: "email"}},
			wantFallback: true,
			wantMissing:  []string{"deleted"},
		},
		{
			// The seeded-defaults trap: a match with no channels still skips the
			// rule-level fallback, and the preview says so.
			name:         "an empty matching policy suppresses the fallback",
			rule:         structs.AlertRule{Project: "atlas", Name: "x", Priority: "P2", NotificationChannelIDs: `["rule-ch"]`},
			policies:     []structs.NotificationPolicy{policy("medium", `{"priority":"P2"}`, `[]`, true, false)},
			wantPolicies: []PolicyPreview{{ID: "medium", Name: "medium", Matched: true, Reason: "matched, but it names no channels; continue_matching is off, so later policies are not consulted"}},
			wantChannels: []ChannelPreview{},
			wantMissing:  []string{},
		},
		{
			name:         "status matcher: the preview routes a firing alert",
			rule:         structs.AlertRule{Project: "atlas", Name: "x", Priority: "P2"},
			policies:     []structs.NotificationPolicy{policy("resolved only", `{"status":"resolved"}`, `["slack"]`, true, false)},
			wantPolicies: []PolicyPreview{{ID: "resolved only", Name: "resolved only", Matched: false, Reason: "status is firing; this policy matches resolved"}},
			wantChannels: []ChannelPreview{},
			wantFallback: true,
			wantMissing:  []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := previewRoutingFrom(groups, tt.policies, channels, &tt.rule)
			if !reflect.DeepEqual(got.Policies, tt.wantPolicies) {
				t.Errorf("policies =\n\t%+v\nwant\n\t%+v", got.Policies, tt.wantPolicies)
			}
			if !reflect.DeepEqual(got.Channels, tt.wantChannels) {
				t.Errorf("channels = %+v, want %+v", got.Channels, tt.wantChannels)
			}
			if got.UsedRuleChannels != tt.wantFallback {
				t.Errorf("used_rule_channels = %v, want %v", got.UsedRuleChannels, tt.wantFallback)
			}
			if !reflect.DeepEqual(got.MissingChannelIDs, tt.wantMissing) {
				t.Errorf("missing_channel_ids = %v, want %v", got.MissingChannelIDs, tt.wantMissing)
			}
		})
	}
}

// TestPreviewAgreesWithTheRouter: what the preview lists (channels plus missing
// ids) is exactly matchedChannelIDs — the list Route dispatches — for the same
// rule and policies. The preview is not a second opinion about routing.
func TestPreviewAgreesWithTheRouter(t *testing.T) {
	rule := &structs.AlertRule{Project: "atlas", Name: "api errors", Priority: "P1", NotificationChannelIDs: `["c"]`}
	policySets := [][]structs.NotificationPolicy{
		nil,
		{policy("a", `{"priority":"P1"}`, `["a","b"]`, true, true), policy("b", `{}`, `["b","c"]`, true, false), policy("z", `{}`, `["z"]`, true, false)},
		{policy("bad", `not json`, `["a"]`, true, false), policy("d", `{}`, `["d"]`, false, false)},
		{policy("badch", `{}`, `not json`, true, false), policy("y", `{}`, `["y"]`, true, false)},
	}
	channels := []structs.NotificationChannel{channel("a", "A", "slack"), channel("c", "C", "email")}
	for i, policies := range policySets {
		want := matchedChannelIDs(buildAlertContextFrom(nil, rule, "firing", 0, ""), rule, policies)
		p := previewRoutingFrom(nil, policies, channels, rule)
		known := map[string]bool{"a": true, "c": true}
		var gotChannels, gotMissing, wantChannels, wantMissing []string
		for _, c := range p.Channels {
			gotChannels = append(gotChannels, c.ID)
		}
		gotMissing = append(gotMissing, p.MissingChannelIDs...)
		for _, id := range want {
			if known[id] {
				wantChannels = append(wantChannels, id)
			} else {
				wantMissing = append(wantMissing, id)
			}
		}
		if !reflect.DeepEqual(gotChannels, wantChannels) || !reflect.DeepEqual(gotMissing, wantMissing) {
			t.Errorf("set %d: preview channels %v missing %v, router %v", i, gotChannels, gotMissing, want)
		}
		if len(p.Policies) != len(policies) {
			t.Errorf("set %d: %d verdicts for %d policies", i, len(p.Policies), len(policies))
		}
	}
}
