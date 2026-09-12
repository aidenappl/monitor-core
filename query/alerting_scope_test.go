package query

import (
	"database/sql/driver"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/env"
)

// withCryptoKey pins env.CryptoKey for a test. env.Load() never runs under
// `go test`, so the var is empty and tools.Encrypt refuses outright — which
// would make a channel-create test fail for a reason unrelated to what it is
// asserting.
func withCryptoKey(t *testing.T) {
	t.Helper()
	previous := env.CryptoKey
	env.CryptoKey = "test-monitor-crypto-key-32-byte!"
	t.Cleanup(func() { env.CryptoKey = previous })
}

// THE FOUR ALERTING TABLES, AND THE PREDICATE THAT WAS MISSING FROM ALL OF THEM.
//
// alert_rules, notification_channels, notification_policies and service_groups
// shipped with no tenancy column at all — four of the seven migrations 127-133
// repair. The way that was eventually noticed was `grep "scope\."` returning zero
// hits across every one of their query files, months later.
//
// alert_rules has its own coverage in alert_rules_query_test.go, which also holds
// the empty-project refusal table for all four. This file is the other three,
// plus the two properties that only exist once a table is shared: that a MUTATION
// carries the project in its own WHERE rather than inheriting it from a read, and
// that the policy seeder is gated per PROJECT rather than per table.

// TestNotificationChannelReadsAreProjectScoped.
//
// This table is the one where the leak was not merely a listing. Since migration
// 126 the row carries an encrypted `config` — a Slack webhook URL, a PagerDuty
// routing key, SMTP recipients — which scanNotificationChannel DECRYPTS. The
// struct keeps it off the wire (`json:"-"`), but GetNotificationChannel is what
// POST /v1/notification-channels/{id}/test sends through, so an unscoped lookup
// let one tenant page another tenant's on-call rotation by id alone.
func TestNotificationChannelReadsAreProjectScoped(t *testing.T) {
	channelRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"id", "project", "name", "type", "config", "config_enc", "created_at"}).
			AddRow("c-1", "atlas", "ops", "slack", "{}", nil, testTime)
	}

	t.Run("list binds the project", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectQuery("WHERE monitor.notification_channels.project = \\?").
			WithArgs("atlas").WillReturnRows(channelRows())

		channels, err := ListNotificationChannels(mockDB, "atlas")
		if err != nil {
			t.Fatalf("ListNotificationChannels: %v", err)
		}
		if len(channels) != 1 || channels[0].Project != "atlas" {
			t.Fatalf("got %+v, want the project scanned back off the row", channels)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the list shows every tenant's destinations: %v", err)
		}
	})

	t.Run("get binds the project alongside the id", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectQuery("WHERE monitor.notification_channels.id = \\? AND monitor.notification_channels.project = \\?").
			WithArgs("c-1", "atlas").WillReturnRows(channelRows())

		if _, err := GetNotificationChannel(mockDB, "atlas", "c-1"); err != nil {
			t.Fatalf("GetNotificationChannel: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("an id from another project still resolves a credential: %v", err)
		}
	})

	t.Run("insert binds the project", func(t *testing.T) {
		withCryptoKey(t)

		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		// config is written NULL and config_enc holds the ciphertext (126), so
		// the encrypted value is AnyArg — it is nondeterministic by construction.
		mock.ExpectExec("INSERT INTO monitor.notification_channels").
			WithArgs(sqlmock.AnyArg(), "atlas", "ops", "slack", nil, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))

		ch, err := CreateNotificationChannel(mockDB, "atlas", CreateNotificationChannelRequest{
			Name: "ops", Type: "slack", Config: `{"webhook_url":"https://hooks.example.com/x"}`,
		})
		if err != nil {
			t.Fatalf("CreateNotificationChannel: %v", err)
		}
		if ch.Project != "atlas" {
			t.Errorf("Project = %q, want atlas on the returned channel", ch.Project)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the insert does not carry the project: %v", err)
		}
	})

	t.Run("delete scopes without reading first", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectExec("^DELETE FROM monitor.notification_channels WHERE id = \\? AND project = \\?$").
			WithArgs("c-1", "atlas").WillReturnResult(sqlmock.NewResult(0, 1))

		if _, err := DeleteNotificationChannel(mockDB, "atlas", "c-1"); err != nil {
			t.Fatalf("DeleteNotificationChannel: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the delete is not project-scoped: %v", err)
		}
	})
}

// TestServiceGroupReadsAreProjectScoped.
//
// A group's MEMBERS are service names, and a service name is unique only within
// one project's event stream — two tenants each running an `api` is the ordinary
// case (migration 130). So an unscoped group list does not merely show another
// tenant's grouping, it feeds alerts.matchServiceGroups a set that resolves
// against the wrong traffic and routes the alert by it.
func TestServiceGroupReadsAreProjectScoped(t *testing.T) {
	groupRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"id", "project", "name", "description", "services", "created_at", "updated_at"}).
			AddRow("g-1", "atlas", "payments", "", `["atlas-api"]`, testTime, testTime)
	}

	t.Run("list binds the project", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectQuery("WHERE monitor.service_groups.project = \\?").
			WithArgs("atlas").WillReturnRows(groupRows())

		groups, err := ListServiceGroups(mockDB, "atlas")
		if err != nil {
			t.Fatalf("ListServiceGroups: %v", err)
		}
		if len(groups) != 1 || groups[0].Project != "atlas" {
			t.Fatalf("got %+v, want the project scanned back off the row", groups)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the list shows every tenant's groups: %v", err)
		}
	})

	t.Run("insert binds the project", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectExec("INSERT INTO monitor.service_groups").
			WithArgs(sqlmock.AnyArg(), "atlas", "payments", "", `["atlas-api"]`, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))

		if _, err := CreateServiceGroup(mockDB, "atlas", CreateServiceGroupRequest{
			Name: "payments", Services: `["atlas-api"]`,
		}); err != nil {
			t.Fatalf("CreateServiceGroup: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the insert does not carry the project: %v", err)
		}
	})

	t.Run("update carries the project in its own WHERE", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectQuery("SELECT .* FROM monitor.service_groups WHERE").WillReturnRows(groupRows())
		mock.ExpectExec("^UPDATE monitor.service_groups SET name = \\? WHERE id = \\? AND project = \\?$").
			WithArgs("payments EU", "g-1", "atlas").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("SELECT .* FROM monitor.service_groups WHERE").WillReturnRows(groupRows())

		if _, err := UpdateServiceGroup(mockDB, "atlas", "g-1", UpdateServiceGroupRequest{Name: "payments EU"}); err != nil {
			t.Fatalf("UpdateServiceGroup: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the update leans on the preceding read for its tenancy: %v", err)
		}
	})

	t.Run("delete scopes without reading first", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectExec("^DELETE FROM monitor.service_groups WHERE id = \\? AND project = \\?$").
			WithArgs("g-1", "atlas").WillReturnResult(sqlmock.NewResult(0, 1))

		if _, err := DeleteServiceGroup(mockDB, "atlas", "g-1"); err != nil {
			t.Fatalf("DeleteServiceGroup: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the delete is not project-scoped: %v", err)
		}
	})
}

// TestNotificationPolicyMutationsCarryTheProjectThemselves.
//
// A policy is a ROUTING instruction, which makes an unscoped mutation worse than
// an unscoped read: retargeting another tenant's policy at channels you control
// redirects their alerts to you. The predicate lives in the UPDATE's and the
// DELETE's own WHERE rather than in the read that precedes them, so a future
// caller that skips or reorders the lookup still cannot reach across tenants.
func TestNotificationPolicyMutationsCarryTheProjectThemselves(t *testing.T) {
	policyRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{
			"id", "project", "name", "description", "position", "matchers", "channel_ids",
			"continue_matching", "repeat_interval_seconds", "enabled", "is_default",
			"created_at", "updated_at",
		}).AddRow("p-1", "atlas", "Payments", "", int64(2), "{}", "[]", false, int64(0), true, false, testTime, testTime)
	}

	t.Run("get binds the project alongside the id", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectQuery("WHERE monitor.notification_policies.id = \\? AND monitor.notification_policies.project = \\?").
			WithArgs("p-1", "atlas").WillReturnRows(policyRows())

		p, err := GetNotificationPolicy(mockDB, "atlas", "p-1")
		if err != nil {
			t.Fatalf("GetNotificationPolicy: %v", err)
		}
		if p == nil || p.Project != "atlas" {
			t.Fatalf("got %+v, want the project scanned back off the row", p)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("an id from another project still resolves: %v", err)
		}
	})

	t.Run("delete scopes its own statement", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		// The read is expected because DeleteNotificationPolicy needs it for the
		// is_default REFUSAL, not for the tenancy check — which is why the
		// DELETE below still names the project itself.
		mock.ExpectQuery("SELECT .* FROM monitor.notification_policies WHERE").WillReturnRows(policyRows())
		mock.ExpectExec("^DELETE FROM monitor.notification_policies WHERE id = \\? AND project = \\?$").
			WithArgs("p-1", "atlas").WillReturnResult(sqlmock.NewResult(0, 1))

		if err := DeleteNotificationPolicy(mockDB, "atlas", "p-1"); err != nil {
			t.Fatalf("DeleteNotificationPolicy: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the delete leans on the preceding read for its tenancy: %v", err)
		}
	})
}

// TestSeedDefaultPoliciesIsGatedPerProjectAndWritesThemDisabled is the SEEDING
// DECISION, pinned as behaviour.
//
// TWO PROPERTIES, AND EACH GUARDS A DIFFERENT FAILURE.
//
// The GATE is per project, not per table. Under migration 129 this table holds
// every project's routing order at once, so a table-wide count would let the
// first project set up permanently block every later one from being seeded —
// silently, with the later project simply booting with an empty routing table.
//
// The seeded rows are ENABLED = FALSE, and that is the fix for a live defect
// rather than a preference. They ship with empty channel_ids, and
// alerts.matchedChannelIDs treats "a policy matched" as reason to skip the
// rule's own notification_channel_ids fallback — so enabled, these four match
// EVERY alert by priority, suppress the per-rule channels an operator did
// configure, and route to nothing. Six enabled rules on the deployed instance
// were in exactly that state. Disabled, the router's `!policy.Enabled` continue
// skips them, the fallback stays reachable, and the four rows remain a template
// an operator fills in and turns on.
func TestSeedDefaultPoliciesIsGatedPerProjectAndWritesThemDisabled(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("^SELECT COUNT\\(\\*\\) FROM monitor.notification_policies WHERE project = \\?$").
		WithArgs("atlas").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(int64(0)))

	// Built rather than written out, because the assertion is one property
	// repeated four times and a hand-written 52-value list would bury it.
	// Column order is notificationPolicyInsertColumns:
	//   id, project, name, description, position, matchers, channel_ids,
	//   continue_matching, repeat_interval_seconds, enabled, is_default,
	//   created_at, updated_at
	seeds := []struct {
		name     string
		position int64
		matchers string
	}{
		{"Critical (P0) — All Channels", 1, `{"priority":"P0"}`},
		{"High (P1) — PagerDuty + Email", 2, `{"priority":"P1"}`},
		{"Medium (P2) — Email Only", 3, `{"priority":"P2"}`},
		{"Low (P3) — Web Only", 4, `{"priority":"P3"}`},
	}
	args := []driver.Value{}
	for _, sd := range seeds {
		args = append(args,
			sqlmock.AnyArg(), "atlas", sd.name, "", sd.position, sd.matchers,
			"[]",             // channel_ids — empty, which is why enabled must be false
			false,            // continue_matching
			int64(0),         // repeat_interval_seconds
			false,            // enabled — THE DECISION
			true,             // is_default
			sqlmock.AnyArg(), // created_at
			sqlmock.AnyArg(), // updated_at
		)
	}

	// One statement, four rows: the set is all-or-nothing, so two processes
	// booting together cannot interleave into a half-seeded list.
	mock.ExpectExec("^INSERT INTO monitor.notification_policies .* VALUES \\(.*\\),\\(.*\\),\\(.*\\),\\(.*\\)$").
		WithArgs(args...).
		WillReturnResult(sqlmock.NewResult(0, 4))

	if err := SeedDefaultNotificationPolicies(mockDB, "atlas"); err != nil {
		t.Fatalf("SeedDefaultNotificationPolicies: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the seeded defaults are no longer four disabled rows in one project-scoped statement: %v", err)
	}
}

// TestSeedDefaultPoliciesSkipsAProjectThatAlreadyHasSome is the other half of the
// per-project gate: a project whose policies were brought over by
// cutover.BackfillConfig must not be seeded on top of, and — unlike the old
// table-wide count — a DIFFERENT project having policies must not block this one.
func TestSeedDefaultPoliciesSkipsAProjectThatAlreadyHasSome(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	// The count is bound to THIS project, so rows belonging to another one are
	// not in it. No INSERT is expected: reaching one fails the call.
	mock.ExpectQuery("^SELECT COUNT\\(\\*\\) FROM monitor.notification_policies WHERE project = \\?$").
		WithArgs("atlas").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(int64(4)))

	if err := SeedDefaultNotificationPolicies(mockDB, "atlas"); err != nil {
		t.Fatalf("SeedDefaultNotificationPolicies: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the seeder wrote into a project that already had policies: %v", err)
	}
}
