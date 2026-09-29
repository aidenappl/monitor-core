package telemetry

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// The stdout line is written by this package rather than by the SDK, because
// the SDK's stdout switch is all-or-nothing and its redactor is unexported. So
// the line carries its own, smaller redaction: credential-named keys, and the
// credential shapes that realistically turn up in a Go error message. The event
// itself is still redacted by the SDK before it is shipped or spooled.

// MAX_LINE_VALUE bounds each value on a stdout line.
const MAX_LINE_VALUE = 400

// lineOmitKeys are left off the stdout line: multi-line, or already implied by
// the container the line is printed in.
var lineOmitKeys = map[string]bool{
	"stack_trace": true,
	"stacktrace":  true,
	"source_func": true,
	"mon_role":    true,
	"mon_zone":    true,
}

// lineSecretFragments mirrors the SDK's key rule: a key whose normalized form
// contains one of these holds a credential.
var lineSecretFragments = []string{
	"password", "passwd", "secret", "token", "apikey", "privatekey",
	"authorization", "cookie", "credential", "sessionid", "signature", "dsn",
	"encryptionkey", "signingkey", "email",
}

var lineShapes = []struct {
	re   *regexp.Regexp
	repl string
}{
	{regexp.MustCompile(`eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}`), "[REDACTED]"},
	{regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`), "$1 [REDACTED]"},
	{regexp.MustCompile(`(?i)(\b[a-z][a-z0-9+.-]*://[^:/@\s]+):[^@/\s]+@`), "$1:[REDACTED]@"},
	{regexp.MustCompile(`([A-Za-z0-9_.-]+):[^@\s/]+@(tcp|unix)\(`), "$1:[REDACTED]@$2("},
	{emailPattern, "[email]"},
}

// emailPattern matches an address anywhere in a string. Addresses turn up in
// free text the SDK's key rule cannot see — a MariaDB duplicate-key message, an
// SMTP refusal — so they are scrubbed from the event itself too.
var emailPattern = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)

// freeTextKeys are the event fields that hold prose written by something else —
// a driver, an IdP, an SMTP server.
var freeTextKeys = []string{"error", "error_message", "message", "detail"}

// scrubFreeText removes email addresses from the prose fields of an event
// before the SDK sees it.
func scrubFreeText(data map[string]any) {
	for _, key := range freeTextKeys {
		if s, ok := data[key].(string); ok && strings.Contains(s, "@") {
			data[key] = emailPattern.ReplaceAllString(s, "[email]")
		}
	}
}

// formatLine renders an event as one line: LEVEL name key=value …, keys sorted.
func formatLine(level, name string, data map[string]any) string {
	keys := make([]string, 0, len(data))
	for k := range data {
		if !lineOmitKeys[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(strings.ToUpper(level))
	b.WriteByte(' ')
	b.WriteString(name)
	for _, k := range keys {
		b.WriteByte(' ')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(lineValue(k, data[k]))
	}
	return b.String()
}

func lineValue(key string, v any) string {
	var s string
	switch t := v.(type) {
	case nil:
		return "null"
	case string:
		s = t
	case error:
		s = t.Error()
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprint(t)
	default:
		s = fmt.Sprint(t)
	}
	if secretKey(key) {
		return "[REDACTED]"
	}
	for _, shape := range lineShapes {
		s = shape.re.ReplaceAllString(s, shape.repl)
	}
	if len(s) > MAX_LINE_VALUE {
		s = s[:MAX_LINE_VALUE] + "…"
	}
	if s == "" || strings.ContainsAny(s, " \t\n\"=") {
		return strconv.Quote(s)
	}
	return s
}

func secretKey(key string) bool {
	normalized := strings.NewReplacer("_", "", "-", "", ".", "", " ", "").Replace(strings.ToLower(key))
	for _, fragment := range lineSecretFragments {
		if strings.Contains(normalized, fragment) {
			return true
		}
	}
	return false
}
