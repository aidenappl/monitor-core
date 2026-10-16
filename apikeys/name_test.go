package apikeys

import (
	"errors"
	"strings"
	"testing"
)

// TestNormalizeName pins the length rule to the column (VARCHAR(255) utf8mb4,
// counted in characters): before it an over-long name reached MariaDB as errno
// 1406 and the rename answered 500 (ZONE-SETTINGS-01).
func TestNormalizeName(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr string
	}{
		{"trimmed", "  ingest  ", "ingest", ""},
		{"exactly the column", strings.Repeat("a", MAX_NAME_LENGTH), strings.Repeat("a", MAX_NAME_LENGTH), ""},
		{"multibyte at the limit counts characters", strings.Repeat("é", MAX_NAME_LENGTH), strings.Repeat("é", MAX_NAME_LENGTH), ""},
		{"one over", strings.Repeat("a", MAX_NAME_LENGTH+1), "", "name must be at most 255 characters"},
		{"multibyte over", strings.Repeat("🌿", MAX_NAME_LENGTH+1), "", "name must be at most 255 characters"},
		{"blank", "   ", "", "name is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeName(tt.in)
			if tt.wantErr == "" {
				if err != nil || got != tt.want {
					t.Fatalf("NormalizeName = (%q, %v), want %q", got, err, tt.want)
				}
				return
			}
			if !errors.Is(err, ErrInvalidKeyName) || err.Error() != tt.wantErr {
				t.Fatalf("NormalizeName err = %v, want ErrInvalidKeyName %q", err, tt.wantErr)
			}
		})
	}
}
