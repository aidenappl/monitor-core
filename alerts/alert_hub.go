package alerts

import (
	"sync"
	"time"

	"github.com/aidenappl/monitor-core/scope"
	"github.com/google/uuid"
)

// AlertEvent represents an alert state change broadcast to SSE subscribers
type AlertEvent struct {
	Type   string `json:"type"`
	RuleID string `json:"rule_id"`
	// Project is the tenant whose rule changed state, taken from the rule's own
	// column. It is what Publish filters on, and it is on the wire because the
	// alerting page shows the project a firing rule belongs to.
	Project   string  `json:"project"`
	RuleName  string  `json:"rule_name"`
	Status    string  `json:"status"`
	Value     float64 `json:"value"`
	Message   string  `json:"message"`
	Timestamp string  `json:"timestamp"`
}

// AlertSubscriber represents a client listening for alert events
type AlertSubscriber struct {
	ID string
	// Project is the tenant this subscriber may see, resolved from the request's
	// credential by routes.HandleStreamAlerts. It is NOT a client-chosen filter
	// and there is no query parameter that reaches it — the same rule
	// routes/stream.go states for the event stream.
	Project string
	Events  chan *AlertEvent
}

// AlertHub is a fan-out pub/sub hub for alert state changes
type AlertHub struct {
	mu          sync.RWMutex
	subscribers map[string]*AlertSubscriber
	maxSubs     int
}

// NewAlertHub creates a new hub with a subscriber limit
func NewAlertHub(maxSubscribers int) *AlertHub {
	return &AlertHub{
		subscribers: make(map[string]*AlertSubscriber),
		maxSubs:     maxSubscribers,
	}
}

// Subscribe creates a new subscriber bound to one project.
//
// Before this took a project, EVERY subscriber received EVERY rule's state
// changes — and an alert event carries the rule's name and the human message
// built from it ("Alert '5xx spike on payments' is firing: value 412.00 gt
// threshold 50.00"), so a live tail was a running commentary on another tenant's
// failures. A stream leaves no stored query to read back, so nothing about it
// was visible after the fact either.
//
// An EMPTY project is accepted and receives nothing: scope.Matches refuses it
// outright, so a caller that skipped the project resolution gets a silent, empty
// stream rather than every tenant's. Fail-closed at the second lock, exactly as
// scope.Matches' own guard describes — the handler's requireProject is the first.
func (h *AlertHub) Subscribe(project string) *AlertSubscriber {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.subscribers) >= h.maxSubs {
		return nil
	}

	sub := &AlertSubscriber{
		ID:      uuid.New().String(),
		Project: project,
		Events:  make(chan *AlertEvent, 64),
	}
	h.subscribers[sub.ID] = sub
	return sub
}

// Unsubscribe removes a subscriber
func (h *AlertHub) Unsubscribe(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if sub, ok := h.subscribers[id]; ok {
		close(sub.Events)
		delete(h.subscribers, id)
	}
}

// Publish sends an alert event to every subscriber the event belongs to.
//
// Membership is delegated to scope.Matches rather than compared here, for the
// reason services/hub.go gives for the event stream: the live tail and the
// stored history must not answer the same question differently, including
// during the empty-string transition window scope.ProjectPredicate documents.
func (h *AlertHub) Publish(event *AlertEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, sub := range h.subscribers {
		if !scope.Matches(sub.Project, event.Project) {
			continue
		}
		select {
		case sub.Events <- event:
		default:
		}
	}
}

// PublishStateChange creates and publishes an alert event for a state transition.
//
// The project comes from the RULE, not from a context: this is called from the
// evaluator's timer goroutine, where the rule row is the only thing that knows
// whose alert this is.
func (h *AlertHub) PublishStateChange(project, ruleID, ruleName, status, message string, value float64) {
	h.Publish(&AlertEvent{
		Type:      "alert_state_change",
		Project:   project,
		RuleID:    ruleID,
		RuleName:  ruleName,
		Status:    status,
		Value:     value,
		Message:   message,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
}
