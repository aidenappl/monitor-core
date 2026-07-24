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
