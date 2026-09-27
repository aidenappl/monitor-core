package query

import (
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// TestRotateRefreshTokenStampsUsedAt pins the conditional stamp the grace window
// rests on: the UPDATE that spends the old row sets replaced_by AND used_at in
// one statement, guarded by replaced_by IS NULL AND revoked_at IS NULL. A used_at
// written separately (or not at all) would leave a spent row with no spend time,
// which classifyRefresh treats as reuse — so concurrent tabs would go back to
// logging users out, and nothing would error.
func TestRotateRefreshTokenStampsUsedAt(t *testing.T) {
	usedAt := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	req := CreateRefreshTokenRequest{
		UserID:    7,
		TokenHash: make([]byte, 32),
		FamilyID:  make([]byte, 16),
		ExpiresAt: usedAt.Add(24 * time.Hour),
	}

	tests := []struct {
		name     string
		affected int64
		wantID   int64
		wantErr  error
	}{
		{name: "unclaimed row is stamped", affected: 1, wantID: 42},
		{name: "already-rotated row reports ErrTokenAlreadyRotated", affected: 0, wantErr: ErrTokenAlreadyRotated},
		// A concurrent RevokeFamily also leaves 0 rows: the revoked_at guard is what
		// stops a live successor being committed into a family logout just killed.
		{name: "concurrently revoked row reports ErrTokenAlreadyRotated", affected: 0, wantErr: ErrTokenAlreadyRotated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			mock.ExpectExec(regexp.QuoteMeta("INSERT INTO refresh_tokens")).
				WillReturnResult(sqlmock.NewResult(42, 1))
			mock.ExpectExec(regexp.QuoteMeta("UPDATE refresh_tokens SET replaced_by = ?, used_at = ? WHERE id = ? AND replaced_by IS NULL AND revoked_at IS NULL")).
				WithArgs(int64(42), usedAt, int64(9)).
				WillReturnResult(sqlmock.NewResult(0, tt.affected))

			gotID, err := RotateRefreshToken(mockDB, 9, usedAt, req)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("RotateRefreshToken: %v", err)
			}
			if gotID != tt.wantID {
				t.Errorf("id = %d, want %d", gotID, tt.wantID)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}
