package alerts

import (
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
	"syscall"

	"github.com/aidenappl/monitor-core/structs"
)

// ErrInvalidChannelConfig marks a channel whose stored config can never deliver
// — a missing or malformed URL, no recipient, no routing key. It is the
// caller's to fix, so POST /v1/notification-channels/{id}/test answers it with
// a 400 naming the field; a config that is valid but whose delivery fails is a
// 200 carrying {ok:false, error} instead (NOTIFICATIONS-04).
var ErrInvalidChannelConfig = errors.New("invalid notification channel config")

// configError is a field-level config refusal. Its text names the config key
// and never echoes the value, which is the credential.
type configError struct{ msg string }

func (e configError) Error() string { return e.msg }
func (e configError) Unwrap() error { return ErrInvalidChannelConfig }

func badConfig(format string, args ...any) error {
	return configError{fmt.Sprintf(format, args...)}
}

// ValidateChannelConfig checks that a channel's config could deliver at all,
// without sending anything. The checks mirror what each notifier branch needs,
// so a config that passes here fails only at delivery time.
func ValidateChannelConfig(channel *structs.NotificationChannel) error {
	var fields map[string]any
	if err := json.Unmarshal([]byte(channel.Config), &fields); err != nil || fields == nil {
		return badConfig("config must be a JSON object")
	}
	str := func(key string) string {
		s, _ := fields[key].(string)
		return strings.TrimSpace(s)
	}

	switch channel.Type {
	case "webhook":
		return requireHTTPURL("url", str("url"))
	case "slack":
		return requireHTTPURL("webhook_url", str("webhook_url"))
	case "pagerduty":
		if str("routing_key") == "" {
			return badConfig("routing_key is required for pagerduty channels")
		}
	case "email":
		var cfg emailConfig
		_ = json.Unmarshal([]byte(channel.Config), &cfg)
		cfg = withServerSMTP(cfg)
		if strings.TrimSpace(cfg.To) == "" {
			return badConfig("to is required for email channels")
		}
		if cfg.SMTPHost == "" {
			return badConfig("smtp_host is required for email channels (or set MON_SMTP_* on the server)")
		}
	default:
		return badConfig("unsupported channel type: %s", channel.Type)
	}
	return nil
}

func requireHTTPURL(field, raw string) error {
	if raw == "" {
		return badConfig("%s is required", field)
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return badConfig("%s must be an absolute http:// or https:// URL", field)
	}
	return nil
}

// MAX_DELIVERY_REASON_LENGTH bounds the reason stored in last_test_error.
const MAX_DELIVERY_REASON_LENGTH = 500

var (
	urlInText   = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://\S+`)
	emailInText = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)
)

// DeliveryReason turns a failed delivery into one sentence a person can act on
// — "DNS lookup failed for example.invalid", "the webhook answered HTTP 404" —
// and never the destination itself. A channel's URL path, a Slack webhook and a
// PagerDuty routing key are credentials, so the reason is built from the
// failure's CLASS (and at most the host name, which is not secret); anything
// that falls through to raw error text has URLs and addresses stripped.
func DeliveryReason(channelType string, err error) string {
	if err == nil {
		return ""
	}
	label := channelType
	if label == "" {
		label = "destination"
	}

	var ne *NotifyError
	if errors.As(err, &ne) && ne.StatusCode != 0 {
		return fmt.Sprintf("the %s destination answered HTTP %d", label, ne.StatusCode)
	}

	var dnsErr *net.DNSError
	var certErr *x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	var invalidCert x509.CertificateInvalidError
	var netErr net.Error
	switch {
	case errors.As(err, &dnsErr):
		if dnsErr.IsNotFound {
			return fmt.Sprintf("DNS lookup failed for %s: no such host", dnsErr.Name)
		}
		return fmt.Sprintf("DNS lookup failed for %s", dnsErr.Name)
	case errors.Is(err, syscall.ECONNREFUSED):
		return "the destination refused the connection"
	case errors.As(err, &netErr) && netErr.Timeout():
		return "the destination did not answer within 10 seconds"
	case errors.As(err, &certErr), errors.As(err, &hostErr), errors.As(err, &invalidCert):
		return "the destination's TLS certificate could not be verified"
	}

	reason := urlInText.ReplaceAllString(err.Error(), "<url>")
	reason = emailInText.ReplaceAllString(reason, "<address>")
	if len(reason) > MAX_DELIVERY_REASON_LENGTH {
		reason = strings.ToValidUTF8(reason[:MAX_DELIVERY_REASON_LENGTH], "")
	}
	return reason
}
