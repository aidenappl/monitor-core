package tools

import "testing"

func TestHashAndCheckPassword(t *testing.T) {
	const pw = "correct-horse-7"

	hash, err := HashPassword(pw)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if len(hash) == 0 {
		t.Fatal("HashPassword returned empty hash")
	}
	if string(hash) == pw {
		t.Fatal("hash equals plaintext — not hashed")
	}

	if !CheckPassword(hash, pw) {
		t.Error("CheckPassword rejected the correct password")
	}
	if CheckPassword(hash, "wrong-password-1") {
		t.Error("CheckPassword accepted an incorrect password")
	}

	// Distinct hashes for the same input (bcrypt salts each call).
	hash2, err := HashPassword(pw)
	if err != nil {
		t.Fatalf("HashPassword (2): %v", err)
	}
	if string(hash) == string(hash2) {
		t.Error("two hashes of the same password are identical — salting broken")
	}
}

func TestValidatePassword(t *testing.T) {
	tests := []struct {
		name    string
		pw      string
		wantErr bool
	}{
		{"valid", "abcdef12", false},
		{"too short", "ab12", true},
		{"no digit", "abcdefgh", true},
		{"no letter", "12345678", true},
		{"empty", "", true},
		{"exactly min", "aaaaaaa1", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePassword(tt.pw)
			if tt.wantErr && err == nil {
				t.Errorf("ValidatePassword(%q) = nil, want error", tt.pw)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ValidatePassword(%q) = %v, want nil", tt.pw, err)
			}
		})
	}
}
