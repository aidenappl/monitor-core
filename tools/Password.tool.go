package tools

import (
	"fmt"
	"unicode"

	"golang.org/x/crypto/bcrypt"
)

// bcryptCost is fixed at 12 (OWASP-acceptable floor, matches OpenBucket). bcrypt
// truncates at 72 bytes; Monitor does not permit passphrases long enough for that
// to matter, so no SHA-256 pre-hash is applied.
const bcryptCost = 12

// minPasswordLength is the shortest password Monitor accepts.
const minPasswordLength = 8

// HashPassword returns the bcrypt hash (cost 12) of a plaintext password.
func HashPassword(password string) ([]byte, error) {
	return bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
}

// CheckPassword reports whether plaintext matches the stored bcrypt hash.
func CheckPassword(hashed []byte, password string) bool {
	return bcrypt.CompareHashAndPassword(hashed, []byte(password)) == nil
}

// dummyHash is a throwaway bcrypt hash (cost 12) computed once at startup. It
// exists only so DummyPasswordCheck can spend the same CPU as a real compare.
var dummyHash, _ = bcrypt.GenerateFromPassword([]byte("timing-equalizer"), bcryptCost)

// DummyPasswordCheck runs a bcrypt comparison against a throwaway hash to
// equalize response timing on the "no such account" login path. Without it, a
// missing account returns instantly while a real one pays the bcrypt cost — a
// timing oracle that lets an attacker enumerate registered emails. Always no-ops.
func DummyPasswordCheck(password string) {
	_ = bcrypt.CompareHashAndPassword(dummyHash, []byte(password))
}

// ValidatePassword enforces Monitor's minimum password policy: at least 8 chars,
// with at least one letter and one digit. Returns a user-safe error on failure.
func ValidatePassword(password string) error {
	if len(password) < minPasswordLength {
		return fmt.Errorf("password must be at least %d characters", minPasswordLength)
	}
	var hasLetter, hasDigit bool
	for _, r := range password {
		switch {
		case unicode.IsLetter(r):
			hasLetter = true
		case unicode.IsDigit(r):
			hasDigit = true
		}
	}
	if !hasLetter || !hasDigit {
		return fmt.Errorf("password must contain at least one letter and one digit")
	}
	return nil
}
