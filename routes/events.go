package routes

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-ingest/services"
	"github.com/aidenappl/monitor-ingest/structs"
)

// Queue is the global event queue (set from main.go)
var Queue *services.Queue

// HealthHandler returns queue stats
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	enqueued, dropped, pending := Queue.Stats()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "ok",
		"enqueued": enqueued,
		"dropped":  dropped,
		"pending":  pending,
	})
}

// IngestEventsHandler processes incoming NDJSON events
func IngestEventsHandler(w http.ResponseWriter, r *http.Request) {
	bodyReader, err := getBodyReader(r)
	if err != nil {
		log.Printf("failed to get body reader: %v", err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer bodyReader.Close()

	count, err := parseAndEnqueue(bodyReader)
	if err != nil {
		log.Printf("failed to parse events: %v", err)
		http.Error(w, fmt.Sprintf("Invalid event: %v", err), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"accepted": count,
	})
}

func getBodyReader(r *http.Request) (io.ReadCloser, error) {
	contentEncoding := r.Header.Get("Content-Encoding")
	if strings.Contains(strings.ToLower(contentEncoding), "gzip") {
		gzReader, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		return gzReader, nil
	}
	return r.Body, nil
}

func parseAndEnqueue(reader io.Reader) (int, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	count := 0
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()

		if len(line) == 0 {
			continue
		}

		var event structs.Event
		if err := json.Unmarshal(line, &event); err != nil {
			return count, fmt.Errorf("line %d: invalid JSON: %w", lineNum, err)
		}

		if err := event.Validate(); err != nil {
			return count, fmt.Errorf("line %d: %w", lineNum, err)
		}

		Queue.Enqueue(&event)
		count++
	}

	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("error reading body: %w", err)
	}

	return count, nil
}
