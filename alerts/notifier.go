package alerts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
	default:
		return fmt.Errorf("unsupported channel type: %s", channel.Type)
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

func (n *Notifier) sendEmail(channel *Channel, alertName, message string, value float64) error {
	// Email sending is stubbed — integrate with SendGrid or similar when needed
	log.Printf("alert notifier: email notification stub — alert=%s message=%s value=%.2f", alertName, message, value)
	return nil
}
