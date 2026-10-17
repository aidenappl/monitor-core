package alerts

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"syscall"
	"testing"

	"github.com/aidenappl/monitor-core/structs"
)

func TestValidateChannelConfig(t *testing.T) {
	tests := []struct {
		name, typ, config, wantErr string
	}{
		{"webhook ok", "webhook", `{"url":"https://example.com/hook"}`, ""},
		{"webhook missing url", "webhook", `{}`, "url is required"},
		{"webhook relative url", "webhook", `{"url":"/hook"}`, "url must be an absolute http:// or https:// URL"},
		{"webhook other scheme", "webhook", `{"url":"ftp://example.com/x"}`, "url must be an absolute http:// or https:// URL"},
		{"slack ok", "slack", `{"webhook_url":"https://hooks.slack.com/services/T/B/x"}`, ""},
		{"slack missing", "slack", `{"webhook_url":" "}`, "webhook_url is required"},
		{"pagerduty ok", "pagerduty", `{"routing_key":"abc"}`, ""},
		{"pagerduty missing", "pagerduty", `{}`, "routing_key is required for pagerduty channels"},
		{"email missing to", "email", `{"smtp_host":"smtp.example.com"}`, "to is required for email channels"},
		{"not an object", "webhook", `[]`, "config must be a JSON object"},
		{"unparseable", "webhook", `nope`, "config must be a JSON object"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateChannelConfig(&structs.NotificationChannel{Type: tt.typ, Config: tt.config})
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidChannelConfig) || err.Error() != tt.wantErr {
				t.Fatalf("err = %v, want ErrInvalidChannelConfig %q", err, tt.wantErr)
			}
		})
	}

	// The refusal never echoes the value — the config is the credential.
	err := ValidateChannelConfig(&structs.NotificationChannel{Type: "webhook", Config: `{"url":"secret-token-123"}`})
	if err == nil || strings.Contains(err.Error(), "secret-token-123") {
		t.Errorf("err = %v; must refuse without echoing the value", err)
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestDeliveryReason(t *testing.T) {
	secretURL := "https://hooks.slack.com/services/T000/B000/SECRETSECRET"
	transport := func(cause error) error {
		return notifyError("webhook", "request", fmt.Errorf("webhook request failed: %w",
			withoutURL(&url.Error{Op: "Post", URL: secretURL, Err: cause})))
	}
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"dns", transport(&net.OpError{Op: "dial", Err: &net.DNSError{Name: "example.invalid", Err: "no such host", IsNotFound: true}}),
			"DNS lookup failed for example.invalid: no such host"},
		{"refused", transport(&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}), "the destination refused the connection"},
		{"timeout", transport(timeoutErr{}), "the destination did not answer within 10 seconds"},
		{"status", &NotifyError{ChannelType: "webhook", Stage: "response", StatusCode: 404, Err: errors.New("x")},
			"the webhook destination answered HTTP 404"},
		{"scrubbed fallback", errors.New("failed posting to " + secretURL + " for ops@example.com"),
			"failed posting to <url> for <address>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DeliveryReason("webhook", tt.err)
			if got != tt.want {
				t.Errorf("DeliveryReason = %q, want %q", got, tt.want)
			}
			if strings.Contains(got, "SECRETSECRET") {
				t.Errorf("reason leaks the destination: %q", got)
			}
		})
	}
}

// TestSendTestReasonAgainstARealServer drives the real webhook sender: a 404
// from the destination becomes a reason naming the status and not the URL.
func TestSendTestReasonAgainstARealServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	ch := &structs.NotificationChannel{Type: "webhook", Config: `{"url":"` + srv.URL + `/secret-path"}`}
	err := NewNotifier().SendTest(ch)
	if err == nil {
		t.Fatal("SendTest succeeded against a 404")
	}
	reason := DeliveryReason(ch.Type, err)
	if reason != "the webhook destination answered HTTP 404" || strings.Contains(reason, "secret-path") {
		t.Errorf("reason = %q", reason)
	}
}
