package issues

import "testing"

func TestNormalizeMessage(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "uuid collapsed",
			in:   "user 550e8400-e29b-41d4-a716-446655440000 not found",
			want: "user <UUID> not found",
		},
		{
			name: "number collapsed",
			in:   "failed after 42 retries",
			want: "failed after <N> retries",
		},
		{
			name: "url collapsed",
			in:   "GET https://api.example.com/v1/x failed",
			want: "GET <URL> failed",
		},
		{
			name: "hex collapsed",
			in:   "bad pointer 0xdeadbeef here",
			want: "bad pointer <HEX> here",
		},
		{
			name: "no dynamic parts unchanged",
			in:   "connection refused",
			want: "connection refused",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeMessage(tt.in); got != tt.want {
				t.Errorf("normalizeMessage(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestNormalizeMessageCollapsesVariants(t *testing.T) {
	// Two structurally identical errors differing only by a UUID and a number
	// must normalize to the same string.
	a := normalizeMessage("order 11111111-1111-1111-1111-111111111111 failed at attempt 3")
	b := normalizeMessage("order 22222222-2222-2222-2222-222222222222 failed at attempt 9")
	if a != b {
		t.Errorf("expected identical normalization, got %q vs %q", a, b)
	}
}

func TestGenerateFingerprint(t *testing.T) {
	// Stable: same inputs → same fingerprint.
	f1 := generateFingerprint("svc", "http_error", "boom", "/x")
	f2 := generateFingerprint("svc", "http_error", "boom", "/x")
	if f1 != f2 {
		t.Errorf("fingerprint not stable: %q vs %q", f1, f2)
	}
	if len(f1) != 64 {
		t.Errorf("expected 64-char sha256 hex, got %d chars", len(f1))
	}

	// UUID/number differences in the message collapse to the same fingerprint.
	fa := generateFingerprint("svc", "http_error", "request 550e8400-e29b-41d4-a716-446655440000 took 12 ms", "/x")
	fb := generateFingerprint("svc", "http_error", "request 6ba7b810-9dad-11d1-80b4-00c04fd430c8 took 88 ms", "/x")
	if fa != fb {
		t.Errorf("expected UUID/number variants to share a fingerprint, got %q vs %q", fa, fb)
	}

	// Different service → different fingerprint.
	if generateFingerprint("svc-a", "n", "m", "/p") == generateFingerprint("svc-b", "n", "m", "/p") {
		t.Errorf("expected different services to produce different fingerprints")
	}
}
