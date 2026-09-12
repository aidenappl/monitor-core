package alerts

import (
	"reflect"
	"testing"

	"github.com/aidenappl/monitor-core/structs"
)

// policy is a small constructor so the tables below read as routing decisions
// rather than as struct literals.
func policy(name, matchers, channelIDs string, enabled, cont bool) structs.NotificationPolicy {
	return structs.NotificationPolicy{
		ID:               name,
		Project:          "atlas",
		Name:             name,
		Matchers:         matchers,
		ChannelIDs:       channelIDs,
		Enabled:          enabled,
		ContinueMatching: cont,
	}
}

// TestMatchedChannelIDsFallsBackOnlyWhenNoPolicyMatched IS THE "ROUTES NOWHERE"
// TEST, and it pins the exact interaction that made alerting silently
// undeliverable on the live instance.
//
// The rule's own notification_channel_ids are a FALLBACK, consulted only when no
// policy matched. The four seeded default policies matched every alert by
// priority (P0-P3 covers the whole enum) and carried `channel_ids: []`, so they
// set policyMatched, suppressed the fallback, and routed to nothing. Six enabled
// rules evaluated on schedule and notified nobody, with no error and no log line.
//
// Seeding them DISABLED is what fixes it, and the "disabled policy" case below
// is the assertion that the disabled arm really does leave the fallback reachable
// — not merely that it skips the policy.
func TestMatchedChannelIDsFallsBackOnlyWhenNoPolicyMatched(t *testing.T) {
	rule := &structs.AlertRule{
		Project:                "atlas",
		Priority:               "P1",
		NotificationChannelIDs: `["rule-channel"]`,
	}
	alertCtx := buildAlertContextFrom(nil, rule, "firing", 0, "")

	tests := []struct {
		name     string
		policies []structs.NotificationPolicy
		want     []string
	}{
		{
			name: "no policies at all falls back to the rule",
			want: []string{"rule-channel"},
		},
		{
			// The live defect, reproduced. An ENABLED matching policy with an
			// empty channel list swallows the fallback and the alert goes
			// nowhere.
			name:     "enabled empty-channel default suppresses the fallback",
			policies: []structs.NotificationPolicy{policy("P1 default", `{"priority":"P1"}`, "[]", true, false)},
			want:     []string{},
		},
		{
			// The fix. A disabled policy never sets policyMatched, so the rule's
			// own channels are honoured — which is what an operator with
			// channels configured and no policies actually expects.
			name:     "disabled default leaves the fallback reachable",
			policies: []structs.NotificationPolicy{policy("P1 default", `{"priority":"P1"}`, "[]", false, false)},
			want:     []string{"rule-channel"},
		},
		{
			name:     "a matching policy with channels wins over the rule",
			policies: []structs.NotificationPolicy{policy("P1 pager", `{"priority":"P1"}`, `["pd"]`, true, false)},
			want:     []string{"pd"},
		},
		{
			// A policy that does not match must not consume the fallback either.
			name:     "non-matching policy leaves the fallback reachable",
			policies: []structs.NotificationPolicy{policy("P0 only", `{"priority":"P0"}`, `["pd"]`, true, false)},
			want:     []string{"rule-channel"},
		},
		{
			// continue_matching accumulates, and the first-past-the-post default
			// stops the walk. Order is the routing order.
			name: "continue_matching accumulates then stops",
			policies: []structs.NotificationPolicy{
				policy("all", `{}`, `["slack"]`, true, true),
				policy("P1", `{"priority":"P1"}`, `["pd"]`, true, false),
				policy("never reached", `{}`, `["email"]`, true, true),
			},
			want: []string{"slack", "pd"},
		},
		{
			// Deduplicated, and in the order the policies named them. The old
			// implementation collected into a map, so the dispatch order was
			// random and nothing about it could be asserted.
			name: "duplicates collapse and order is the policy order",
			policies: []structs.NotificationPolicy{
				policy("all", `{}`, `["slack","pd"]`, true, true),
				policy("also", `{}`, `["pd","email"]`, true, true),
			},
			want: []string{"slack", "pd", "email"},
		},
		{
			// A policy whose matchers will not parse is skipped, and skipping it
			// must NOT count as a match — otherwise one malformed row silently
			// disables the fallback for every alert.
			name:     "unparseable matchers do not count as a match",
			policies: []structs.NotificationPolicy{policy("broken", `not json`, `["pd"]`, true, false)},
			want:     []string{"rule-channel"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchedChannelIDs(alertCtx, rule, tt.policies)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("matchedChannelIDs = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestCountDestinationsCountsOnlyResolvableChannels is what has_destinations
// reports on GET /v1/alert-rules.
//
// A DANGLING ID IS NOT A DESTINATION. Channel ids live in JSON arrays with no
// foreign key behind them, so deleting a channel leaves its id in every rule and
// policy that named it (see query.DeleteNotificationChannel) — and the router
// logs and skips it at send time. Counting the raw ids would report a rule as
// wired up while every one of its notifications is dropped, which is precisely
// the reassurance this field exists to withhold.
func TestCountDestinationsCountsOnlyResolvableChannels(t *testing.T) {
	rule := &structs.AlertRule{
		Project:                "atlas",
		Priority:               "P1",
		NotificationChannelIDs: `["live","deleted"]`,
	}
	alertCtx := buildAlertContextFrom(nil, rule, "firing", 0, "")

	tests := []struct {
		name  string
		known map[string]bool
		want  int
	}{
		{"both resolve", map[string]bool{"live": true, "deleted": true}, 2},
		{"one dangling id is not a destination", map[string]bool{"live": true}, 1},
		{"no channels exist in the project", map[string]bool{}, 0},
		// The live state of the deployed instance: every rule carried
		// `notification_channel_ids: []`, so there was nothing to resolve.
		{"nothing configured at all", nil, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := countDestinations(alertCtx, rule, nil, tt.known); got != tt.want {
				t.Errorf("countDestinations = %d, want %d", got, tt.want)
			}
		})
	}

	t.Run("a rule with no channel ids has no destinations", func(t *testing.T) {
		empty := &structs.AlertRule{Project: "atlas", Priority: "P1", NotificationChannelIDs: "[]"}
		ctx := buildAlertContextFrom(nil, empty, "firing", 0, "")
		if got := countDestinations(ctx, empty, nil, map[string]bool{"live": true}); got != 0 {
			t.Errorf("countDestinations = %d, want 0 — this is the state the WARN exists for", got)
		}
	})
}

// TestBuildAlertContextFromResolvesGroupsWithinTheProject.
//
// A service group is matched by SERVICE NAME, and service names are unique only
// within one project's event stream — two tenants each running an `api` is the
// ordinary case (migration 130). The groups handed in here are always one
// project's, which is what makes the id put on the AlertContext meaningful to
// the policy that matches on it.
func TestBuildAlertContextFromResolvesGroupsWithinTheProject(t *testing.T) {
	groups := []structs.ServiceGroup{
		{ID: "g-payments", Project: "atlas", Services: `["atlas-api","atlas-worker"]`},
		{ID: "g-edge", Project: "atlas", Services: `["cdn"]`},
		// A group whose services will not parse is skipped rather than reported,
		// so one bad row cannot stop the others being resolved.
		{ID: "g-broken", Project: "atlas", Services: `{"not":"an array"}`},
	}
	rule := &structs.AlertRule{
		Project:      "atlas",
		Priority:     "P1",
		QueryFilters: `[{"field":"service","operator":"eq","value":"atlas-api"},{"field":"env","operator":"eq","value":"production"}]`,
	}

	ac := buildAlertContextFrom(groups, rule, "firing", 12, "msg")

	if ac.Service != "atlas-api" || ac.Env != "production" {
		t.Errorf("service/env = %q/%q, want atlas-api/production", ac.Service, ac.Env)
	}
	if !reflect.DeepEqual(ac.ServiceGroupIDs, []string{"g-payments"}) {
		t.Errorf("ServiceGroupIDs = %v, want [g-payments]", ac.ServiceGroupIDs)
	}

	// A service-group matcher resolves against exactly those ids.
	policies := []structs.NotificationPolicy{
		policy("payments", `{"service_group":"g-payments"}`, `["pd"]`, true, false),
	}
	if got := matchedChannelIDs(ac, rule, policies); !reflect.DeepEqual(got, []string{"pd"}) {
		t.Errorf("matchedChannelIDs = %v, want [pd]", got)
	}
}

// TestServiceGroupIDsIsNeverNil. It is marshalled into no response, but
// matchPolicy ranges over it and a nil slice reads identically — this pins the
// contract buildAlertContextFrom promises rather than the behaviour it happens
// to have.
func TestServiceGroupIDsIsNeverNil(t *testing.T) {
	ac := buildAlertContextFrom(nil, &structs.AlertRule{Project: "atlas"}, "firing", 0, "")
	if ac.ServiceGroupIDs == nil {
		t.Error("ServiceGroupIDs is nil")
	}
}
