package routes

import (
	"strings"
	"testing"
)

func TestValidateCommentBody(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"ordinary", "looked into it, flaky upstream", ""},
		{"exactly the limit in ascii", strings.Repeat("a", MAX_COMMENT_RUNES), ""},
		// 10,000 four-byte runes is 40,000 bytes: the limit is characters, not bytes.
		{"exactly the limit in emoji", strings.Repeat("🔥", MAX_COMMENT_RUNES), ""},
		{"one over", strings.Repeat("a", MAX_COMMENT_RUNES+1), "body must be at most 10000 characters"},
		{"one over in multibyte", strings.Repeat("é", MAX_COMMENT_RUNES+1), "body must be at most 10000 characters"},
		{"empty", "", "body is required"},
		{"whitespace only", " \n\t ", "body is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCommentBody(tt.body)
			got := ""
			if err != nil {
				got = err.Error()
			}
			if got != tt.wantErr {
				t.Errorf("validateCommentBody() = %q, want %q", got, tt.wantErr)
			}
		})
	}
}
