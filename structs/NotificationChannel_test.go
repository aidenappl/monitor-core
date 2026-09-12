package structs

import (
	"encoding/json"
	"strings"
	"testing"
)

// The summary is what replaced serving the raw config, so its whole job is to be
// USEFUL ENOUGH to tell two channels apart and NEVER enough to reuse one. These
// tests are written as "the secret must not appear in the output", because that
// is the property, and asserting an exact string would pass just as happily on a
// summary that leaked a different field.
func TestSummariseChannelConfigNeverLeaksTheCredential(t *testing.T) {
	const slackSecret = "T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"
	const routingKey = "R0ABCDEFGHIJKLMNOP1234"
	const smtpPassword = "hunter2-smtp-password"

	tests := []struct {
		name        string
		channelType string
		config      string
		mustNotHave []string
		mustHave    string
	}{
		{
			name:        "slack shows the host, never the webhook path",
			channelType: "slack",
			config:      `{"webhook_url":"https://hooks.slack.com/services/` + slackSecret + `"}`,
			mustNotHave: []string{slackSecret, "B00000000", "services"},
			mustHave:    "hooks.slack.com",
		},
		{
			name:        "webhook drops a token in the query string",
			channelType: "webhook",
			config:      `{"url":"https://example.com/hook?token=SUPERSECRETTOKEN"}`,
			mustNotHave: []string{"SUPERSECRETTOKEN", "token", "/hook"},
			mustHave:    "example.com",
		},
		{
			name:        "webhook drops userinfo credentials",
			channelType: "webhook",
			config:      `{"url":"https://user:PASSWORD123@example.com/hook"}`,
			mustNotHave: []string{"PASSWORD123", "user:"},
			mustHave:    "example.com",
		},
		{
			name:        "pagerduty shows only the last four of the routing key",
			channelType: "pagerduty",
			config:      `{"routing_key":"` + routingKey + `"}`,
			mustNotHave: []string{routingKey, "R0ABCDEFGH"},
			mustHave:    "1234",
		},
		{
			name:        "email shows recipients but not the smtp password beside them",
			channelType: "email",
			config:      `{"to":"oncall@example.com","smtp_password":"` + smtpPassword + `"}`,
			mustNotHave: []string{smtpPassword},
			mustHave:    "oncall@example.com",
		},
		{
			name:        "an unrecognised shape falls back to a label, never the input",
			channelType: "webhook",
			config:      `{"some_unexpected_field":"` + smtpPassword + `"}`,
			mustNotHave: []string{smtpPassword, "some_unexpected_field"},
			mustHave:    "webhook",
		},
		{
			name:        "unparseable config leaks nothing",
			channelType: "slack",
			config:      `not json at all ` + slackSecret,
			mustNotHave: []string{slackSecret},
			mustHave:    "unreadable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SummariseChannelConfig(tc.channelType, tc.config)
			for _, secret := range tc.mustNotHave {
				if strings.Contains(got, secret) {
					t.Errorf("summary leaked %q\n got: %s", secret, got)
				}
			}
			if tc.mustHave != "" && !strings.Contains(got, tc.mustHave) {
				t.Errorf("summary = %q, expected it to contain %q", got, tc.mustHave)
			}
		})
	}
}

func TestSummariseChannelConfigEmpty(t *testing.T) {
	for _, config := range []string{"", "   ", "{}"} {
		if got := SummariseChannelConfig("webhook", config); got != "" {
			t.Errorf("SummariseChannelConfig(%q) = %q, want empty", config, got)
		}
	}
}

// The security property of this type is structural: Config carries plaintext
// credentials and is excluded from JSON, so no present or future handler can
// serialise it by forgetting to redact. A regression here is a one-character
// edit to a struct tag, invisible in review, and it re-opens the disclosure to
// every authenticated session in the zone.
func TestNotificationChannelNeverSerialisesItsConfig(t *testing.T) {
	const secret = "https://hooks.slack.com/services/T0/B0/SECRETSECRETSECRET"

	body, err := json.Marshal(NotificationChannel{
		ID:            "ch-1",
		Name:          "oncall",
		Type:          "slack",
		Config:        `{"webhook_url":"` + secret + `"}`,
		ConfigSummary: "hooks.slack.com",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	encoded := string(body)
	if strings.Contains(encoded, "SECRETSECRETSECRET") {
		t.Fatalf("the raw config reached the wire: %s", encoded)
	}
	if strings.Contains(encoded, `"config"`) {
		t.Errorf("a `config` key is present; it must be json:\"-\": %s", encoded)
	}
	if !strings.Contains(encoded, `"config_summary":"hooks.slack.com"`) {
		t.Errorf("the summary is missing, so the UI has nothing to identify the channel by: %s", encoded)
	}
}
