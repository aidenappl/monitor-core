package alerts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html"
	"log"
	"net/http"
	"net/smtp"
	"strings"
	"time"
)

// Notifier dispatches notifications to different channel types
type Notifier struct {
	client *http.Client
}

// NewNotifier creates a new notifier
func NewNotifier() *Notifier {
	return &Notifier{
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// Send dispatches a notification to the appropriate channel type
func (n *Notifier) Send(channel *Channel, alertName, message string, value float64) error {
	switch channel.Type {
	case "webhook":
		return n.sendWebhook(channel, alertName, message, value)
	case "slack":
		return n.sendSlack(channel, alertName, message, value)
	case "email":
		return n.sendEmail(channel, alertName, message, value)
	case "pagerduty":
		return n.sendPagerDuty(channel, alertName, message, value, "trigger")
	default:
		return fmt.Errorf("unsupported channel type: %s", channel.Type)
	}
}

// SendResolved dispatches a resolved notification (relevant for PagerDuty auto-resolve)
func (n *Notifier) SendResolved(channel *Channel, alertName, message string, value float64) error {
	switch channel.Type {
	case "pagerduty":
		return n.sendPagerDuty(channel, alertName, message, value, "resolve")
	default:
		return n.Send(channel, alertName, message, value)
	}
}

// SendTest sends a test notification to verify channel configuration
func (n *Notifier) SendTest(channel *Channel) error {
	testAlert := "Test Alert"
	testMessage := "This is a test notification from Monitor. If you received this, your notification channel is configured correctly."
	testValue := 0.0

	switch channel.Type {
	case "pagerduty":
		return n.sendPagerDuty(channel, testAlert, testMessage, testValue, "trigger")
	default:
		return n.Send(channel, testAlert, testMessage, testValue)
	}
}

func (n *Notifier) sendWebhook(channel *Channel, alertName, message string, value float64) error {
	var config struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal([]byte(channel.Config), &config); err != nil {
		return fmt.Errorf("invalid webhook config: %w", err)
	}
	if config.URL == "" {
		return fmt.Errorf("webhook url is required")
	}

	payload := map[string]interface{}{
		"alert":     alertName,
		"message":   message,
		"value":     value,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook payload: %w", err)
	}

	resp, err := n.client.Post(config.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("webhook request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return nil
}

func (n *Notifier) sendSlack(channel *Channel, alertName, message string, value float64) error {
	var config struct {
		WebhookURL string `json:"webhook_url"`
	}
	if err := json.Unmarshal([]byte(channel.Config), &config); err != nil {
		return fmt.Errorf("invalid slack config: %w", err)
	}
	if config.WebhookURL == "" {
		return fmt.Errorf("slack webhook_url is required")
	}

	payload := map[string]interface{}{
		"blocks": []map[string]interface{}{
			{
				"type": "header",
				"text": map[string]string{
					"type": "plain_text",
					"text": fmt.Sprintf("Alert: %s", alertName),
				},
			},
			{
				"type": "section",
				"text": map[string]string{
					"type": "mrkdwn",
					"text": message,
				},
			},
			{
				"type": "context",
				"elements": []map[string]string{
					{
						"type": "mrkdwn",
						"text": fmt.Sprintf("Value: `%.2f` | %s", value, time.Now().UTC().Format(time.RFC3339)),
					},
				},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal slack payload: %w", err)
	}

	resp, err := n.client.Post(config.WebhookURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("slack request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("slack returned status %d", resp.StatusCode)
	}

	return nil
}

type emailConfig struct {
	SMTPHost     string `json:"smtp_host"`
	SMTPPort     string `json:"smtp_port"`
	SMTPUsername string `json:"smtp_username"`
	SMTPPassword string `json:"smtp_password"`
	FromEmail    string `json:"from_email"`
	FromName     string `json:"from_name"`
	To           string `json:"to"`
}

func (n *Notifier) sendEmail(channel *Channel, alertName, message string, value float64) error {
	var config emailConfig
	if err := json.Unmarshal([]byte(channel.Config), &config); err != nil {
		return fmt.Errorf("invalid email config: %w", err)
	}
	if config.SMTPHost == "" || config.To == "" {
		return fmt.Errorf("smtp_host and to are required for email channels")
	}
	if config.SMTPPort == "" {
		config.SMTPPort = "587"
	}
	if config.FromEmail == "" {
		config.FromEmail = config.SMTPUsername
	}

	subject := fmt.Sprintf("[Monitor] Alert: %s", alertName)
	htmlBody := renderAlertEmail(alertName, message, value)

	recipients := strings.Split(config.To, ",")
	for i := range recipients {
		recipients[i] = strings.TrimSpace(recipients[i])
	}

	from := config.FromEmail
	if config.FromName != "" {
		from = fmt.Sprintf("%s <%s>", config.FromName, config.FromEmail)
	}

	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n%s",
		from,
		strings.Join(recipients, ", "),
		subject,
		htmlBody,
	)

	addr := config.SMTPHost + ":" + config.SMTPPort
	var auth smtp.Auth
	if config.SMTPUsername != "" {
		auth = smtp.PlainAuth("", config.SMTPUsername, config.SMTPPassword, config.SMTPHost)
	}

	if err := smtp.SendMail(addr, auth, config.FromEmail, recipients, []byte(msg)); err != nil {
		return fmt.Errorf("email send failed: %w", err)
	}

	log.Printf("alert notifier: email sent to %s for alert %s", config.To, alertName)
	return nil
}

func (n *Notifier) sendPagerDuty(channel *Channel, alertName, message string, value float64, action string) error {
	var config struct {
		RoutingKey string `json:"routing_key"`
		Severity   string `json:"severity"`
	}
	if err := json.Unmarshal([]byte(channel.Config), &config); err != nil {
		return fmt.Errorf("invalid pagerduty config: %w", err)
	}
	if config.RoutingKey == "" {
		return fmt.Errorf("pagerduty routing_key is required")
	}
	if config.Severity == "" {
		config.Severity = "critical"
	}

	payload := map[string]interface{}{
		"routing_key":  config.RoutingKey,
		"event_action": action,
		"dedup_key":    fmt.Sprintf("monitor-alert-%s", alertName),
		"payload": map[string]interface{}{
			"summary":  message,
			"severity": config.Severity,
			"source":   "monitor.appleby.cloud",
			"custom_details": map[string]interface{}{
				"alert_name": alertName,
				"value":      value,
				"timestamp":  time.Now().UTC().Format(time.RFC3339),
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal pagerduty payload: %w", err)
	}

	req, err := http.NewRequest("POST", "https://events.pagerduty.com/v2/enqueue", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create pagerduty request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("pagerduty request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("pagerduty returned status %d", resp.StatusCode)
	}

	log.Printf("alert notifier: pagerduty %s sent for alert %s", action, alertName)
	return nil
}

func renderAlertEmail(alertName, message string, value float64) string {
	accentColor := "#3b82f6"
	iconEmoji := "ℹ️"
	if strings.Contains(message, "firing") {
		accentColor = "#ef4444"
		iconEmoji = "🔴"
	} else if strings.Contains(message, "resolved") {
		accentColor = "#22c55e"
		iconEmoji = "✅"
	}

	escaped := html.EscapeString(message)
	htmlBody := strings.ReplaceAll(escaped, "\n", "<br>")

	return fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0;padding:0;background-color:#111113;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<table width="100%%" cellpadding="0" cellspacing="0" style="background-color:#111113;padding:32px 16px;">
<tr><td align="center">
<table width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%%;">

<!-- Accent bar -->
<tr><td style="background:%s;height:4px;border-radius:8px 8px 0 0;"></td></tr>

<!-- Card -->
<tr><td style="background-color:#1a1a1e;padding:28px 32px;border-radius:0 0 8px 8px;">

<!-- Title -->
<table width="100%%" cellpadding="0" cellspacing="0">
<tr>
<td style="font-size:18px;font-weight:600;color:#e4e4e7;padding-bottom:16px;">
%s&nbsp;&nbsp;%s
</td>
</tr>
</table>

<!-- Body -->
<table width="100%%" cellpadding="0" cellspacing="0">
<tr>
<td style="font-size:14px;line-height:22px;color:#a1a1aa;padding-bottom:8px;">
%s
</td>
</tr>
<tr>
<td style="font-size:13px;color:#71717a;padding-bottom:24px;">
Value: %.2f
</td>
</tr>
</table>

<!-- Divider -->
<table width="100%%" cellpadding="0" cellspacing="0">
<tr><td style="border-top:1px solid #27272a;padding-top:16px;">
<span style="font-size:11px;color:#52525b;">Sent by Monitor &bull; Observability Platform</span>
</td></tr>
</table>

</td></tr>
</table>
</td></tr>
</table>
</body>
</html>`, accentColor, iconEmoji, alertName, htmlBody, value)
}
