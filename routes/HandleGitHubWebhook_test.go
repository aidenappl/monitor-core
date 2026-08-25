package routes

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/structs"
)

func withWebhookSecret(t *testing.T, secret string) {
	t.Helper()
	previous := env.GitHubWebhookSecret
	env.GitHubWebhookSecret = secret
	t.Cleanup(func() { env.GitHubWebhookSecret = previous })
}

func signBody(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func postWebhook(t *testing.T, event, body, signature string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", strings.NewReader(body))
	req.Header.Set("X-GitHub-Event", event)
	if signature != "" {
		req.Header.Set("X-Hub-Signature-256", signature)
	}
	rec := httptest.NewRecorder()
	HandleGitHubWebhook(rec, req)
	return rec
}

// TestWebhookRejectsUnsigned is the load-bearing test for this route. It is
// mounted outside /v1 with no QueryAuthMiddleware, so anyone who learns the URL
// can POST to it — the signature check is the only thing between them and a
// write to the timeline.
func TestWebhookRejectsUnsigned(t *testing.T) {
	withWebhookSecret(t, "s3cr3t")

	const body = `{"action":"closed","pull_request":{"number":1},"repository":{"name":"r","owner":{"login":"o"}}}`

	tests := []struct {
		name      string
		signature string
	}{
		{name: "no signature at all", signature: ""},
		{name: "signature from the wrong secret", signature: signBody("wrong", body)},
		{name: "signature over a different body", signature: signBody("s3cr3t", `{"action":"opened"}`)},
		{name: "garbage", signature: "sha256=notevenhex"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := postWebhook(t, "pull_request", body, tt.signature)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
			}
		})
	}
}

// TestWebhookFailsClosedWhenUnconfigured asserts an install with no secret
// rejects deliveries rather than accepting them unauthenticated.
func TestWebhookFailsClosedWhenUnconfigured(t *testing.T) {
	withWebhookSecret(t, "")

	rec := postWebhook(t, "pull_request", `{"action":"closed"}`, signBody("anything", `{"action":"closed"}`))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d — an unconfigured webhook must not accept writes", rec.Code, http.StatusUnauthorized)
	}
}

// TestWebhookAnswersPing covers GitHub's handshake, which is what turns the
// green tick on in the repo's webhook settings.
func TestWebhookAnswersPing(t *testing.T) {
	withWebhookSecret(t, "s3cr3t")

	const body = `{"zen":"Non-blocking is better than blocking."}`
	rec := postWebhook(t, "ping", body, signBody("s3cr3t", body))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "pong") {
		t.Errorf("body = %q, want it to contain pong", rec.Body.String())
	}
}

// TestWebhookIgnoresUnknownEvents asserts unknown events are acknowledged rather
// than errored. A 4xx makes GitHub retry and eventually disable the webhook —
// for events we deliberately do not handle.
func TestWebhookIgnoresUnknownEvents(t *testing.T) {
	withWebhookSecret(t, "s3cr3t")

	const body = `{"action":"created"}`
	for _, event := range []string{"issue_comment", "push", "star", "workflow_run"} {
		t.Run(event, func(t *testing.T) {
			rec := postWebhook(t, event, body, signBody("s3cr3t", body))
			if rec.Code != http.StatusOK {
				t.Errorf("status = %d, want 200 for an ignored event", rec.Code)
			}
		})
	}
}

// TestTimelineTypeForPRAction pins the mapping. GitHub reports a merge and an
// abandonment with the SAME action ("closed"), distinguished only by the merged
// boolean — conflating them would record every abandoned PR as a merge.
func TestTimelineTypeForPRAction(t *testing.T) {
	tests := []struct {
		name   string
		action string
		merged bool
		want   structs.TimelineEntryType
		wantOK bool
	}{
		{name: "merged", action: "closed", merged: true, want: structs.TimelinePRMerged, wantOK: true},
		{name: "closed without merging", action: "closed", merged: false, want: structs.TimelinePRClosed, wantOK: true},
		{name: "reopened", action: "reopened", want: structs.TimelinePRReopened, wantOK: true},
		{name: "opened has no timeline meaning", action: "opened", wantOK: false},
		{name: "synchronize is a push, not a lifecycle change", action: "synchronize", wantOK: false},
		{name: "edited", action: "edited", wantOK: false},
		{name: "unknown", action: "converted_to_draft", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := timelineTypeForPRAction(tt.action, tt.merged)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Errorf("type = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestWebhookNeverEmitsStatusChange guards the design decision that a merged PR
// does not resolve an issue. Resolution stays a deliberate human or agent
// action, so no webhook path may produce a status_changed entry.
func TestWebhookNeverEmitsStatusChange(t *testing.T) {
	actions := []struct {
		action string
		merged bool
	}{
		{"closed", true}, {"closed", false}, {"reopened", false},
		{"opened", false}, {"synchronize", false},
	}

	for _, a := range actions {
		if got, ok := timelineTypeForPRAction(a.action, a.merged); ok && got == structs.TimelineStatusChanged {
			t.Errorf("action %q (merged=%v) produced a status change; webhooks must never alter issue status", a.action, a.merged)
		}
	}
}
