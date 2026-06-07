package alerts

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

// AlertEvent represents an alert state change broadcast to SSE subscribers
type AlertEvent struct {
	Type      string  `json:"type"`
	RuleID    string  `json:"rule_id"`
	RuleName  string  `json:"rule_name"`
	Status    string  `json:"status"`
	Value     float64 `json:"value"`
	Message   string  `json:"message"`
	Timestamp string  `json:"timestamp"`
}

// AlertSubscriber represents a client listening for alert events
type AlertSubscriber struct {
	ID     string
	Events chan *AlertEvent
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

// Subscribe creates a new subscriber
func (h *AlertHub) Subscribe() *AlertSubscriber {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.subscribers) >= h.maxSubs {
		return nil
	}

	sub := &AlertSubscriber{
		ID:     uuid.New().String(),
		Events: make(chan *AlertEvent, 64),
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

// Publish sends an alert event to all subscribers
func (h *AlertHub) Publish(event *AlertEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, sub := range h.subscribers {
		select {
		case sub.Events <- event:
		default:
		}
	}
}

// PublishStateChange creates and publishes an alert event for a state transition
func (h *AlertHub) PublishStateChange(ruleID, ruleName, status, message string, value float64) {
	h.Publish(&AlertEvent{
		Type:      "alert_state_change",
		RuleID:    ruleID,
		RuleName:  ruleName,
		Status:    status,
		Value:     value,
		Message:   message,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
}
