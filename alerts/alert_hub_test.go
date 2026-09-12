package alerts

import (
	"testing"

	"github.com/aidenappl/monitor-core/env"
)

// withHubDefaultProject pins env.DefaultProjectSlug for a test. env.Load() never
// runs under `go test`, so the var is empty unless a test sets it, and
// scope.Matches' transition arm keys off it.
func withHubDefaultProject(t *testing.T, slug string) {
	t.Helper()
	previous := env.DefaultProjectSlug
	env.DefaultProjectSlug = slug
	t.Cleanup(func() { env.DefaultProjectSlug = previous })
}

// TestAlertHubDeliversOnlyToTheEventsProject is the tenancy boundary of the live
// alert tail.
//
// Subscribe() took no filter and Publish fanned every event out to every
// subscriber. An alert event is not a bare status flag — it carries rule_name,
// which operators name after services, customers and environments, and the
// message the evaluator builds from it ("Alert 'payments 5xx spike' is firing:
// value 412.00 gt threshold 50.00"). A stream leaves no stored query behind, so
// nothing about that leak was visible afterwards either.
//
// The empty-project rows are the transition arm scope.ProjectPredicate
// documents, asserted here so the live tail and the stored history cannot answer
// the same question differently.
func TestAlertHubDeliversOnlyToTheEventsProject(t *testing.T) {
	withHubDefaultProject(t, "default")

	tests := []struct {
		name         string
		subscriber   string
		eventProject string
		wantDelivery bool
	}{
		{"same project", "atlas", "atlas", true},
		{"other tenant's rule", "atlas", "forta", false},
		{"default sees its own", "default", "default", true},
		// A pre-007 alert_history/state row, or an event published before the
		// column existed, belongs to the default project alone.
		{"default sees unstamped", "default", "", true},
		{"non-default never sees unstamped", "atlas", "", false},
		// The fail-closed case: a subscriber that resolved no project must
		// receive NOTHING, never the untagged rows by naive string equality.
		{"unresolved subscriber gets nothing", "", "", false},
		{"unresolved subscriber gets nothing from a real project", "", "atlas", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hub := NewAlertHub(4)
			sub := hub.Subscribe(tt.subscriber)
			if sub == nil {
				t.Fatal("Subscribe returned nil with capacity available")
			}
			defer hub.Unsubscribe(sub.ID)

			hub.PublishStateChange(tt.eventProject, "r-1", "5xx spike", "firing", "msg", 42)

			select {
			case event := <-sub.Events:
				if !tt.wantDelivery {
					t.Errorf("subscriber on %q received %q's alert %q", tt.subscriber, tt.eventProject, event.Message)
				}
				if event.Project != tt.eventProject {
					t.Errorf("Project = %q, want %q", event.Project, tt.eventProject)
				}
			default:
				if tt.wantDelivery {
					t.Errorf("subscriber on %q received nothing for its own project", tt.subscriber)
				}
			}
		})
	}
}

// TestAlertHubFansOutWithinAProject is the negative control. Without it the test
// above passes just as happily against a Publish that delivers to nobody.
func TestAlertHubFansOutWithinAProject(t *testing.T) {
	withHubDefaultProject(t, "default")

	hub := NewAlertHub(4)
	mine := hub.Subscribe("atlas")
	alsoMine := hub.Subscribe("atlas")
	theirs := hub.Subscribe("forta")
	if mine == nil || alsoMine == nil || theirs == nil {
		t.Fatal("Subscribe returned nil with capacity available")
	}

	hub.PublishStateChange("atlas", "r-1", "5xx spike", "firing", "msg", 42)

	for _, sub := range []*AlertSubscriber{mine, alsoMine} {
		select {
		case <-sub.Events:
		default:
			t.Errorf("subscriber %s missed an alert for its own project", sub.ID)
		}
	}
	select {
	case <-theirs.Events:
		t.Error("a subscriber on another project received the alert")
	default:
	}
}

// TestAlertHubSubscriberLimitStillApplies. The limit and the project filter are
// independent, and the limit is what keeps an SSE endpoint from being a memory
// exhaustion vector — adding the filter must not have moved the check.
func TestAlertHubSubscriberLimitStillApplies(t *testing.T) {
	hub := NewAlertHub(1)
	if first := hub.Subscribe("atlas"); first == nil {
		t.Fatal("the first subscriber was refused")
	}
	if second := hub.Subscribe("atlas"); second != nil {
		t.Error("a second subscriber was admitted past maxSubs")
	}
}
