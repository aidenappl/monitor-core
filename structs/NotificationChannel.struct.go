package structs

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// NotificationChannel is one destination a firing alert can be sent to. Lives in
// MariaDB (monitor.notification_channels).
//
// It was `alerts.Channel` and moved here with the table (migration 120), for the
// import-cycle reason AlertRule.struct.go records.
type NotificationChannel struct {
	ID string `json:"id"`
	// Project is the tenant this destination belongs to (migration 128).
	//
	// A channel LOOKS zone-level and is not, and 128's header carries the whole
	// argument: channels are referenced by id out of alert_rules and
	// notification_policies, and both of those are project-scoped, so a shared
	// channel would be a cross-scope reference resolving either to "not found"
	// or to a send into another tenant's PagerDuty. Sharing costs one duplicate
	// row per project and buys an unambiguous graph.
	Project string `json:"project"`
	Name    string `json:"name"`
	Type    string `json:"type"`

	// Config is the DECRYPTED per-type payload — webhook URL, Slack webhook, SMTP
	// recipients, PagerDuty routing key — carried as raw JSON text because
	// alerts/notifier.go Unmarshals a different shape per Type.
	//
	// ⚠️ `json:"-"` IS THE WHOLE SECURITY PROPERTY OF THIS TYPE, NOT A STYLE
	// CHOICE. This field holds credentials in plaintext in memory, and
	// GET /v1/notification-channels serialises this struct on the ordinary /v1
	// subrouter — reachable by any authenticated session and any admin-scope API
	// key. It was `json:"config"` and every secret in a zone was readable by all
	// of them. A redaction applied in a handler would protect only the handler
	// that remembered; excluding it at the type protects every present and future
	// caller, including ones that embed this struct in a response nobody has
	// written yet.
	//
	// Anything that needs to DISPLAY a channel uses ConfigSummary. Anything that
	// needs to SEND through one (alerts/notifier.go) uses this.
	Config string `json:"-"`

	// ConfigSummary is the safe, lossy description rendered in the UI: enough to
	// tell two channels apart, never enough to reuse one. Derived at read time by
	// SummariseChannelConfig, never stored.
	ConfigSummary string `json:"config_summary"`

	CreatedAt time.Time `json:"created_at"`
}

// SummariseChannelConfig renders a channel's config as something safe to show.
//
// The UI's job is "which channel is this", not "what is its secret" — the
// notifications page only ever used config to print a one-line identifier, and
// it fell back to printing the WHOLE config when it could not find a friendlier
// field. So the summary is built here, server-side, and the raw value never
// leaves the process.
//
// FAILING CLOSED IS THE RULE. Every branch that cannot produce a known-safe
// string returns a generic label rather than any part of the input: a config
// that does not parse, or parses to a shape this function does not recognise,
// might be a secret in an unexpected position, and "unrecognised" is a better
// answer than a leak. Callers get "" for an empty config, never the input.
func SummariseChannelConfig(channelType, config string) string {
	trimmed := strings.TrimSpace(config)
	if trimmed == "" || trimmed == "{}" {
		return ""
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(trimmed), &parsed); err != nil {
		return "unreadable configuration"
	}

	str := func(key string) string {
		v, _ := parsed[key].(string)
		return strings.TrimSpace(v)
	}

	switch channelType {
	case "slack":
		// A Slack webhook URL is ITSELF the credential — its path is the secret,
		// so only the host may be shown.
		if host := hostOf(str("webhook_url")); host != "" {
			return host
		}
	case "webhook":
		// Same reasoning: a webhook URL frequently carries a token in its path or
		// query. Host only.
		for _, key := range []string{"url", "webhook_url"} {
			if host := hostOf(str(key)); host != "" {
				return host
			}
		}
	case "email":
		// Recipients are addressing, not authentication — safe, and the only
		// useful way to tell two email channels apart. An SMTP password may sit
		// beside them in the same object and is never read here.
		if to := str("to"); to != "" {
			return to
		}
		if host := str("smtp_host"); host != "" {
			return host
		}
	case "pagerduty":
		// The routing key IS the credential. Last four characters only, and only
		// when there is enough of it that four characters are not most of it.
		if key := str("routing_key"); len(key) >= 8 {
			return "routing key ••••" + key[len(key)-4:]
		}
		return "routing key set"
	}

	return fmt.Sprintf("%s configuration", channelType)
}

// hostOf returns just the host of an absolute URL, or "" for anything else.
// Deliberately drops path, query and userinfo — all three routinely carry the
// token that makes a webhook URL a credential.
func hostOf(raw string) string {
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return ""
	}
	return parsed.Host
}
