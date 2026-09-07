package tools

import "testing"

// TestValidateSlugFormat pins the format rule at its boundaries. The same rule
// is encoded a second time as a CHECK constraint in
// db/migrations/116_create_registry.sql, and the two have to agree — a case that
// changes here is a case that has to change there.
func TestValidateSlugFormat(t *testing.T) {
	tests := []struct {
		name  string
		slug  string
		valid bool
	}{
		{"simple", "trailblaze", true},
		{"digits and hyphens", "team-service-v2", true},
		{"minimum length", "abc", true},
		{"maximum length", "a1b-c2d-e3f-g4h-i5j-k6l-m7n-o8", true},

		{"empty", "", false},
		{"too short", "ab", false},
		{"too long", "a1b-c2d-e3f-g4h-i5j-k6l-m7n-o8x", false},
		{"uppercase", "Trailblaze", false},
		{"leading digit", "1abc", false},
		{"leading hyphen", "-abc", false},
		{"trailing hyphen", "abc-", false},
		{"underscore", "a_bc", false},
		{"dot", "a.bc", false},
		{"space", "a bc", false},
		{"non-ascii", "café-x", false},
		// Not normalized, so an untrimmed value is a failure and not a silent fix.
		{"untrimmed", " abc ", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSlug(tt.slug)
			if tt.valid && err != nil {
				t.Errorf("ValidateSlug(%q) = %v, want nil", tt.slug, err)
			}
			if !tt.valid && err == nil {
				t.Errorf("ValidateSlug(%q) = nil, want an error", tt.slug)
			}
		})
	}
}

// TestValidateSlugRejectsReserved is the guard on the silent failure this list
// exists for: the frontend resolves static route segments before dynamic ones,
// so a project slugged `settings` is shadowed by the app's own page and is
// unreachable with nothing logged anywhere.
func TestValidateSlugRejectsReserved(t *testing.T) {
	// The names the task set as the floor. Extra entries may be added to
	// reservedSlugs; none of these may be removed.
	required := []string{
		"monitor", "auth", "admin", "api", "login", "pending", "unauthorized",
		"settings", "errors", "live", "analytics", "dashboard", "alerts",
		"notifications", "performance", "health", "ready", "static", "_next",
		"favicon",
	}

	for _, slug := range required {
		if !IsReservedSlug(slug) {
			t.Errorf("IsReservedSlug(%q) = false, want true", slug)
		}
		// `_next` is already unreachable through the format rule, so only assert
		// that ValidateSlug refuses the ones the format rule would otherwise admit.
		if SlugPattern.MatchString(slug) && ValidateSlug(slug) == nil {
			t.Errorf("ValidateSlug(%q) = nil, want a reserved-name error", slug)
		}
	}
}

// TestValidateSlugAcceptsSeededDefaults keeps the two committed defaults valid.
// Both are minted by bootstrap.EnsureZoneAndProject at boot and validated there,
// so reserving either name would turn every fresh install into a boot failure.
func TestValidateSlugAcceptsSeededDefaults(t *testing.T) {
	for _, slug := range []string{"trailblaze", "default"} {
		if err := ValidateSlug(slug); err != nil {
			t.Errorf("ValidateSlug(%q) = %v, want nil — this is a seeded default", slug, err)
		}
	}
}
