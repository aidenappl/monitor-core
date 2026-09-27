package middleware

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aidenappl/monitor-core/jwt"
)

func TestDecodeFamilyID(t *testing.T) {
	tests := []struct {
		name   string
		fid    string
		wantOK bool
	}{
		{name: "16 hex bytes", fid: "00112233445566778899aabbccddeeff", wantOK: true},
		{name: "empty (pre-fid token)", fid: "", wantOK: false},
		{name: "not hex", fid: "zz112233445566778899aabbccddeeff", wantOK: false},
		{name: "wrong length", fid: "0011", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, ok := decodeFamilyID(tt.fid)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && len(b) != 16 {
				t.Errorf("len = %d, want 16", len(b))
			}
		})
	}
}

// TestSessionFamilyID never touches db.SQL: it only re-parses the access token.
func TestSessionFamilyID(t *testing.T) {
	const fid = "00112233445566778899aabbccddeeff"
	want := []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}

	mint := func(uid int64, fid string) string {
		tok, _, err := jwt.NewAccessToken(uid, "editor", fid)
		if err != nil {
			t.Fatalf("mint: %v", err)
		}
		return tok
	}

	tests := []struct {
		name   string
		bearer string
		cookie string
		wantOK bool
	}{
		{name: "bearer with fid", bearer: mint(42, fid), wantOK: true},
		{name: "cookie with fid", cookie: mint(42, fid), wantOK: true},
		{name: "invalid bearer falls through to cookie", bearer: "garbage", cookie: mint(42, fid), wantOK: true},
		{name: "legacy token without fid", cookie: mint(42, ""), wantOK: false},
		{name: "token for another user is ignored", cookie: mint(7, fid), wantOK: false},
		{name: "no token", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
			if tt.bearer != "" {
				r.Header.Set("Authorization", "Bearer "+tt.bearer)
			}
			if tt.cookie != "" {
				r.AddCookie(&http.Cookie{Name: "mon-access-token", Value: tt.cookie})
			}
			got, ok := SessionFamilyID(r, 42)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && !bytes.Equal(got, want) {
				t.Errorf("family = %x, want %x", got, want)
			}
		})
	}
}
