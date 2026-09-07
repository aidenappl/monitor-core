package services

import (
	"log"
	"sync/atomic"

	"github.com/aidenappl/monitor-core/structs"
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

// RecordDropped adds n events that were accepted by the queue but lost further
// down the pipeline to the SAME counter the overflow path above feeds.
//
// The counter lives here rather than on the Batcher because this is what
// `/health` already reports, and an event destroyed by a write that never
// succeeded is lost exactly as surely as one the queue refused — reporting only
// the second made a zone whose ClickHouse was completely dead answer
// `{"status":"ok","dropped":0}` while shredding every event. The Batcher
// already holds a *Queue, so this needed no new plumbing.
func (q *Queue) RecordDropped(n int64) {
	if n <= 0 {
		return
	}
	q.dropped.Add(n)
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
