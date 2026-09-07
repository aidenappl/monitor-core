package structs

import (
	"testing"
	"time"
)

// TestCorrelationIDContract pins the wire contract with the SDKs.
//
// ⚠️ This is the test that would have caught a real, shipped landmine.
// go-monitor changed its default job_id and request_id from a UUID to an
// 8-hex-character token while this validator still required a hyphenated UUID.
// Because parseEvents is all-or-nothing and go-monitor drops 4xx without
// retrying, any service adopting that build would have lost 100% of its events —
// one line on stderr, nothing in Monitor, and a brand-new zone endpoint as the
// most likely place to first notice and misdiagnose it.
//
// Every "accepted" case below is a format some SDK build has actually emitted.
// Deleting one is a decision to break whatever emits it, and there is no deploy
// ordering in which narrowing this before the SDKs is safe.
func TestCorrelationIDContract(t *testing.T) {
	accepted := map[string]string{
		"hyphenated UUID (v0.0.8, every deployed service today)": "7653624f-5015-4fcb-9a94-cd82bdc0e3b8",
		"unhyphenated UUID (what the old comment claimed)":       "7653624f50154fcb9a94cd82bdc0e3b8",
		"64-bit compact token (go-monitor's log-friendly form)":  "a1b2c3d4e5f60718",
		"32-bit compact token (the shipped-then-repaired form)":  "a1b2c3d4",
		"uppercase hex": "A1B2C3D4E5F60718",
	}
	for name, id := range accepted {
		t.Run("accepts "+name, func(t *testing.T) {
			e := &Event{Timestamp: time.Now(), Service: "s", Name: "n", JobID: id}
			if err := e.Validate(); err != nil {
				t.Errorf("rejected %q: %v — this format is emitted by a real SDK build, so rejecting it destroys that service's events", id, err)
			}
		})
	}

	rejected := map[string]string{
		"too short to be collision-safe or meaningful": "a1b2c3",
		"longer than any id we mint":                   "a1b2c3d4e5f60718a1b2c3d4e5f60718a1b2c3d4e5f60718a1b2c3d4e5f60718ff",
		"non-hex characters":                           "not-an-id-at-all!!",
		"SQL-ish payload":                              "1' OR '1'='1",
		"whitespace":                                   "a1b2c3d4 ",
	}
	for name, id := range rejected {
		t.Run("rejects "+name, func(t *testing.T) {
			e := &Event{Timestamp: time.Now(), Service: "s", Name: "n", JobID: id}
			if err := e.Validate(); err == nil {
				t.Errorf("accepted %q — validation is the only guard on these fields", id)
			}
		})
	}
}

// TestCorrelationIDAppliesToAllThreeFields guards against a fix applied to one
// field and forgotten on the other two, which is how the fields drift apart.
func TestCorrelationIDAppliesToAllThreeFields(t *testing.T) {
	const compact = "a1b2c3d4e5f60718"
	for _, tc := range []struct {
		field string
		build func(string) *Event
	}{
		{"job_id", func(v string) *Event { return &Event{Timestamp: time.Now(), Service: "s", Name: "n", JobID: v} }},
		{"request_id", func(v string) *Event { return &Event{Timestamp: time.Now(), Service: "s", Name: "n", RequestID: v} }},
		{"trace_id", func(v string) *Event { return &Event{Timestamp: time.Now(), Service: "s", Name: "n", TraceID: v} }},
	} {
		t.Run(tc.field, func(t *testing.T) {
			if err := tc.build(compact).Validate(); err != nil {
				t.Errorf("%s rejected a valid compact token: %v", tc.field, err)
			}
			if err := tc.build("nope!").Validate(); err == nil {
				t.Errorf("%s accepted garbage", tc.field)
			}
		})
	}
}
