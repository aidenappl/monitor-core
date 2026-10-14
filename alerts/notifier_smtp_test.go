package alerts

import (
	"testing"

	"github.com/aidenappl/monitor-core/env"
)

func TestWithServerSMTP(t *testing.T) {
	orig := [5]string{env.SMTPHost, env.SMTPPort, env.SMTPUsername, env.SMTPPassword, env.SMTPFrom}
	t.Cleanup(func() {
		env.SMTPHost, env.SMTPPort, env.SMTPUsername, env.SMTPPassword, env.SMTPFrom = orig[0], orig[1], orig[2], orig[3], orig[4]
	})

	tests := []struct {
		name   string
		server [5]string // host, port, user, pass, from
		in     emailConfig
		want   emailConfig
	}{
		{"channel with only to uses the server",
			[5]string{"smtp.mailersend.net", "587", "u", "p", "alerts@appleby.cloud"},
			emailConfig{To: "a@b.c"},
			emailConfig{SMTPHost: "smtp.mailersend.net", SMTPPort: "587", SMTPUsername: "u", SMTPPassword: "p", FromEmail: "alerts@appleby.cloud", To: "a@b.c"}},
		{"channel's own from wins",
			[5]string{"h", "587", "u", "p", "alerts@appleby.cloud"},
			emailConfig{To: "a@b.c", FromEmail: "me@x.y"},
			emailConfig{SMTPHost: "h", SMTPPort: "587", SMTPUsername: "u", SMTPPassword: "p", FromEmail: "me@x.y", To: "a@b.c"}},
		{"channel with its own host is untouched",
			[5]string{"h", "587", "u", "p", "f"},
			emailConfig{SMTPHost: "own", SMTPUsername: "ou", To: "a@b.c"},
			emailConfig{SMTPHost: "own", SMTPUsername: "ou", To: "a@b.c"}},
		{"no server SMTP leaves the channel as it is",
			[5]string{},
			emailConfig{To: "a@b.c"},
			emailConfig{To: "a@b.c"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env.SMTPHost, env.SMTPPort, env.SMTPUsername, env.SMTPPassword, env.SMTPFrom = tt.server[0], tt.server[1], tt.server[2], tt.server[3], tt.server[4]
			if got := withServerSMTP(tt.in); got != tt.want {
				t.Fatalf("withServerSMTP\n got: %+v\nwant: %+v", got, tt.want)
			}
		})
	}
}
