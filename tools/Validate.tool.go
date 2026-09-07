package tools

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// ValidateExternalURL checks that a URL is a valid HTTPS URL pointing to a
// public (non-internal) host. Use this for admin-configured SSO endpoints and
// any other user-supplied outbound URLs to prevent SSRF — an attacker who can
// set token_url/userinfo_url to an internal address could otherwise pivot the
// server into the private network or a cloud metadata endpoint.
func ValidateExternalURL(rawURL string) error {
	if rawURL == "" {
		return fmt.Errorf("URL is required")
	}

	parsed, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	if !strings.EqualFold(parsed.Scheme, "https") {
		return fmt.Errorf("URL must use HTTPS scheme, got %q", parsed.Scheme)
	}

	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("URL must include a hostname")
	}

	if host == "localhost" || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") {
		return fmt.Errorf("URL must not point to internal hosts")
	}

	ips, err := net.LookupHost(host)
	if err != nil {
		// Resolution failure is not proof of malice (offline dev, private DNS);
		// let it through — the HTTPS scheme + hostname checks still applied.
		return nil
	}

	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
			return fmt.Errorf("URL resolves to a private/internal IP address (%s)", ipStr)
		}
	}

	return nil
}

// ENDPOINT_URL_MAX_LENGTH matches the VARCHAR(255) that zones.ingest_url and
// zones.query_url are declared as. Checked in Go so an over-long value is
// refused with a message naming the field, rather than truncated silently under
// a non-strict sql_mode — a truncated URL is a URL that points somewhere else.
const ENDPOINT_URL_MAX_LENGTH = 255

// NormalizeEndpointURL validates a zone endpoint URL and returns the exact form
// to store.
//
// It is a WRAPPER around ValidateExternalURL, not a second guard. The SSRF rule
// — https only, no internal hostnames, no host that resolves into RFC1918 or
// loopback — has one definition in this package and this function delegates to
// it, because a zone's query URL is a target monitor-core makes outbound
// requests to (the reachability probe, and eventually the read fan-out) and is
// therefore exactly the input that guard exists for. Two copies of an SSRF rule
// is one copy that quietly gets weaker.
//
// What it ADDS is the structure a zone endpoint needs and an SSO endpoint does
// not:
//
//   - It is an ORIGIN (optionally with a path prefix), not a request URL. The
//     callers append "/health", "/ready" and eventually "/v1/…" to it, so a
//     stored query string or fragment would end up in the MIDDLE of the composed
//     URL and silently produce a request nobody wrote — "https://z/?a=1" plus
//     "/health" is not a health check, it is a 404 at best.
//   - Userinfo is refused outright. A credential in a URL would be stored in
//     plaintext in a registry row, logged by every proxy in the path, and
//     returned by the admin API to anyone who can list zones. There is no
//     scenario where the right answer is to allow it and hope.
//   - The trailing slash is stripped, so "https://z/" and "https://z" are one
//     value rather than two rows that differ only in how "//health" resolves.
//     This is the single normalization allowed, for the same reason CreateZone
//     trims whitespace and refuses to case-fold a slug: it cannot change which
//     endpoint the operator meant.
//
// The caller is expected to have trimmed surrounding whitespace already; this
// trims again anyway, because the one place it matters is the paste from a
// terminal that carries a newline and there is no version of this where a
// trailing "\n" was intended.
func NormalizeEndpointURL(rawURL string) (string, error) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return "", fmt.Errorf("URL is required")
	}
	if len(trimmed) > ENDPOINT_URL_MAX_LENGTH {
		return "", fmt.Errorf("URL must be at most %d characters, got %d", ENDPOINT_URL_MAX_LENGTH, len(trimmed))
	}

	parsed, err := url.ParseRequestURI(trimmed)
	if err != nil {
		return "", fmt.Errorf("invalid URL: %w", err)
	}
	if parsed.User != nil {
		return "", fmt.Errorf("URL must not embed credentials")
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return "", fmt.Errorf("URL must not carry a query string — it is an origin, and paths are appended to it")
	}
	if parsed.Fragment != "" {
		return "", fmt.Errorf("URL must not carry a fragment — it is an origin, and paths are appended to it")
	}

	// Rebuilt from the parsed parts rather than string-edited, so what is stored
	// is what was parsed. Scheme is lower-cased by url.Parse already;
	// ValidateExternalURL below is what insists it is https.
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	normalized := parsed.String()

	// Validated AFTER normalization, so the stored value is the value that was
	// checked. Validating the raw form and storing a rewritten one is how a guard
	// ends up vouching for a string nobody ever tested.
	if err := ValidateExternalURL(normalized); err != nil {
		return "", err
	}
	return normalized, nil
}
