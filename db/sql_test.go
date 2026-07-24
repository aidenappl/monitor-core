package db

import "testing"

func TestEnsureDSNParams(t *testing.T) {
	const required = "charset=utf8mb4&parseTime=True&multiStatements=true"

	tests := []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "no existing params",
			dsn:  "monitor:pass@tcp(monitor-mariadb:3306)/monitor_auth",
			want: "monitor:pass@tcp(monitor-mariadb:3306)/monitor_auth?" + required,
		},
		{
			name: "existing params",
			dsn:  "monitor:pass@tcp(monitor-mariadb:3306)/monitor_auth?tls=true",
			want: "monitor:pass@tcp(monitor-mariadb:3306)/monitor_auth?tls=true&" + required,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ensureDSNParams(tt.dsn); got != tt.want {
				t.Errorf("ensureDSNParams(%q) = %q, want %q", tt.dsn, got, tt.want)
			}
		})
	}
}
