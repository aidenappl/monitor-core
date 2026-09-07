package services

import (
	"sync"

	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/google/uuid"
)

// Subscriber represents a client subscribed to live events
type Subscriber struct {
	ID      string
	Filters map[string]string
	Events  chan *structs.Event
}

// Hub is a fan-out pub/sub hub for live event streaming
type Hub struct {
	mu          sync.RWMutex
	subscribers map[string]*Subscriber
	maxSubs     int
}

// NewHub creates a new Hub with a maximum subscriber limit
func NewHub(maxSubscribers int) *Hub {
	return &Hub{
		subscribers: make(map[string]*Subscriber),
		maxSubs:     maxSubscribers,
	}
}

// Subscribe creates a new subscriber with the given filters
// Returns nil if the maximum number of subscribers has been reached
func (h *Hub) Subscribe(filters map[string]string) *Subscriber {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.subscribers) >= h.maxSubs {
		return nil
	}

	sub := &Subscriber{
		ID:      uuid.New().String(),
		Filters: filters,
		Events:  make(chan *structs.Event, 256),
	}
	h.subscribers[sub.ID] = sub
	return sub
}

// Unsubscribe removes a subscriber
func (h *Hub) Unsubscribe(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if sub, ok := h.subscribers[id]; ok {
		close(sub.Events)
		delete(h.subscribers, id)
	}
}

// Publish sends an event to all matching subscribers (non-blocking)
func (h *Hub) Publish(event *structs.Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, sub := range h.subscribers {
		if matchesFilters(event, sub.Filters) {
			select {
			case sub.Events <- event:
			default:
				// Channel full, skip this event for this subscriber
			}
		}
	}
}

// SubscriberCount returns the current number of subscribers
func (h *Hub) SubscriberCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subscribers)
}

func matchesFilters(event *structs.Event, filters map[string]string) bool {
	for key, value := range filters {
		switch key {
		case "service":
			if event.Service != value {
				return false
			}
		case "project":
			// The tenancy boundary for the live tail. Unlike every other key
			// here this one is NOT a client filter — routes/stream.go sets it
			// from the authenticated credential and there is no query parameter
			// that can reach it — so a missing case would not merely widen a
			// subscriber's own filter, it would hand them every project's
			// events in real time.
			//
			// Membership is delegated to scope.Matches rather than compared
			// here, so the live tail and the stored query cannot answer the
			// same question differently — including during the empty-string
			// transition window, which scope.ProjectPredicate documents.
			if !scope.Matches(value, event.Project) {
				return false
			}
		case "env":
			if event.Env != value {
				return false
			}
		case "level":
			if event.Level != value {
				return false
			}
		case "name":
			if event.Name != value {
				return false
			}
		default:
			// Fail closed on any key this switch does not know about. Without
			// this the loop would simply skip the unrecognised key and fall
			// through to `return true`, so a subscriber who filtered on a key
			// we forgot to handle here would silently receive EVERY event
			// instead of none. The only symptom is "more data than I asked
			// for" — no error, no log line, and nothing a happy-path test
			// would notice. Today the stream route's allowlist keeps this
			// unreachable, but the two lists live in different files and the
			// safe side of that drift is matching nothing.
			return false
		}
	}
	return true
}
