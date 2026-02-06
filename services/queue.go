package services

import (
	"log"
	"sync/atomic"

	"github.com/aidenappl/monitor-ingest/structs"
)

// Queue is a buffered channel for events
type Queue struct {
	events   chan *structs.Event
	dropped  atomic.Int64
	enqueued atomic.Int64
}

// NewQueue creates a new event queue with the specified buffer size
func NewQueue(size int) *Queue {
	return &Queue{
		events: make(chan *structs.Event, size),
	}
}

// Enqueue adds an event to the queue
// Returns false if the queue is full (event dropped)
func (q *Queue) Enqueue(event *structs.Event) bool {
	select {
	case q.events <- event:
		q.enqueued.Add(1)
		return true
	default:
		q.dropped.Add(1)
		log.Printf("queue overflow: dropped event %s", event.Name)
		return false
	}
}

// Events returns the channel for consuming events
func (q *Queue) Events() <-chan *structs.Event {
	return q.events
}

// Stats returns queue statistics
func (q *Queue) Stats() (enqueued, dropped int64, pending int) {
	return q.enqueued.Load(), q.dropped.Load(), len(q.events)
}

// Close closes the queue channel
func (q *Queue) Close() {
	close(q.events)
}
