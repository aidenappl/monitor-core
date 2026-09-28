package alerts

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/structs"
)

// These tests drive Evaluator.evaluateAll with an injected clock and fake
// stores, one pass per simulated tick, and assert on the WINDOWS the evaluator
// asked for — which is where every ING-9 failure lived. The old loop's bugs were
// all "this event fell between two lookbacks" or "this lookback read before the
// event was flushed", and neither shows up in a returned value unless the test
// controls both the clock and what is visible at each instant.

// testSettle is the settle delay these tests run with: the default
// FLUSH_INTERVAL of 5 s plus settleMargin.
const testSettle = 5*time.Second + settleMargin

// windowTestBase has a fractional second on purpose, so a window bound that
// inherited it from the clock would show up as a fractional bound.
var windowTestBase = time.Date(2026, 9, 27, 12, 0, 7, 250_000_000, time.UTC)

// aggCall is one successful aggregate the evaluator ran.
type aggCall struct {
	pass     int
	rule     string
	aggExpr  string
	from, to time.Time
	value    float64
}

// published is one notification the evaluator sent.
type published struct {
	rule   string
	status string
	value  float64
}

// evaluatorHarness wires an Evaluator to in-memory fakes. stored plays
// alert_states and outlives the Evaluator, so a test can "restart the process"
// by building a fresh one over the same stored states.
type evaluatorHarness struct {
	t     *testing.T
	clock time.Time
	e     *Evaluator

	rules   []structs.AlertRule
	listErr error
	onList  func()

	stored  map[string]*State
	readErr map[string]error
	reads   map[string]int
	writes  [][]*State
	// writeErr, when set, fails every INSERT; a failed one stores nothing.
	writeErr error

	value    func(rule *structs.AlertRule, aggExpr string, from, to time.Time) (float64, error)
	calls    []aggCall
	failures int

	notes  []published
	logs   []string
	passes int
}

func newHarness(t *testing.T, rules ...structs.AlertRule) *evaluatorHarness {
	t.Helper()
	h := &evaluatorHarness{
		t:       t,
		clock:   windowTestBase,
		rules:   rules,
		stored:  make(map[string]*State),
		readErr: make(map[string]error),
		reads:   make(map[string]int),
		value: func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) {
			return 0, nil
		},
	}
	h.e = h.newEvaluator()
	return h
}

// newEvaluator builds a fresh Evaluator — empty schedules, as after a restart —
// over the harness's shared stores.
func (h *evaluatorHarness) newEvaluator() *Evaluator {
	return &Evaluator{
		tick:   evaluatorTick,
		settle: testSettle,
		now:    func() time.Time { return h.clock },
		listRules: func() ([]structs.AlertRule, error) {
			if h.onList != nil {
				h.onList()
			}
			if h.listErr != nil {
				return nil, h.listErr
			}
			return append([]structs.AlertRule(nil), h.rules...), nil
		},
		listStates: func(_ context.Context, project string) (map[string]*State, error) {
			h.reads[project]++
			if err := h.readErr[project]; err != nil {
				return nil, err
			}
			out := make(map[string]*State)
			for id, s := range h.stored {
				if s.Project == project {
					cp := *s
					out[id] = &cp
				}
			}
			return out, nil
		},
		upsertStates: func(_ context.Context, states []*State) error {
			if h.writeErr != nil {
				return h.writeErr
			}
			batch := make([]*State, len(states))
			for i, s := range states {
				cp := *s
				batch[i] = &cp
				stored := *s
				// updated_at goes over the wire at whole seconds (clickhouse-go
				// binds a time.Time that way), so that is what a read returns.
				stored.UpdatedAt = stored.UpdatedAt.Truncate(time.Second)
				h.stored[s.RuleID] = &stored
			}
			h.writes = append(h.writes, batch)
			return nil
		},
		aggregate: func(ctx context.Context, rule *structs.AlertRule, aggExpr string, from, to time.Time) (float64, error) {
			if project, ok := scope.GetProject(ctx); !ok || project != rule.Project {
				h.t.Errorf("rule %s was aggregated with project %q, want its own %q", rule.ID, project, rule.Project)
			}
			v, err := h.value(rule, aggExpr, from, to)
			if err != nil {
				h.failures++
				return 0, err
			}
			h.calls = append(h.calls, aggCall{pass: h.passes, rule: rule.ID, aggExpr: aggExpr, from: from, to: to, value: v})
			return v, nil
		},
		publish: func(_ context.Context, rule *structs.AlertRule, state *State, status, _ string) {
			h.notes = append(h.notes, published{rule: rule.ID, status: status, value: state.Value})
		},
		logf: func(format string, args ...any) {
			h.logs = append(h.logs, fmt.Sprintf(format, args...))
		},
		lastEnd:      make(map[string]time.Time),
		pendingSince: make(map[string]time.Time),
		retryAt:      make(map[string]time.Time),
		inherited:    make(map[string]bool),
		unsaved:      make(map[string]*State),
	}
}

// evaluate runs one pass at the current clock.
func (h *evaluatorHarness) evaluate() {
	h.passes++
	h.e.evaluateAll(context.Background())
}

// tick advances the clock one evaluator tick and runs a pass.
func (h *evaluatorHarness) tick() {
	h.clock = h.clock.Add(evaluatorTick)
	h.evaluate()
}

func (h *evaluatorHarness) ticks(n int) {
	for i := 0; i < n; i++ {
		h.tick()
	}
}

func (h *evaluatorHarness) callsOf(ruleID string) []aggCall {
	var out []aggCall
	for _, c := range h.calls {
		if c.rule == ruleID {
			out = append(out, c)
		}
	}
	return out
}

func (h *evaluatorHarness) windowsOf(ruleID string) []window {
	var out []window
	for _, c := range h.callsOf(ruleID) {
		out = append(out, window{from: c.from, to: c.to})
	}
	return out
}

func (h *evaluatorHarness) logged(substr string) bool {
	for _, l := range h.logs {
		if strings.Contains(l, substr) {
			return true
		}
	}
	return false
}

func (h *evaluatorHarness) statuses() []string {
	out := []string{}
	for _, n := range h.notes {
		out = append(out, n.status)
	}
	return out
}

// newRule is a threshold count rule that fires on any event, with a cooldown
// long enough that no test sees a re-notification it did not ask for.
func newRule(id, project string, mutate ...func(*structs.AlertRule)) structs.AlertRule {
	r := structs.AlertRule{
		ID:                     id,
		Project:                project,
		Name:                   id,
		Type:                   "threshold",
		Metric:                 "count",
		Condition:              "gt",
		Threshold:              0,
		EvaluationIntervalSecs: 60,
		CooldownSeconds:        3600,
		Enabled:                true,
	}
	for _, m := range mutate {
		m(&r)
	}
	return r
}

// assertContiguous fails unless every window is interval long, has whole-second
// bounds, and starts exactly where the previous one ended.
func assertContiguous(t *testing.T, windows []window, interval time.Duration) {
	t.Helper()
	if len(windows) == 0 {
		t.Fatal("no windows were evaluated")
	}
	for i, w := range windows {
		if got := w.to.Sub(w.from); got != interval {
			t.Errorf("window %d [%s, %s) is %s long, want %s", i, w.from, w.to, got, interval)
		}
		if !w.from.Equal(w.from.Truncate(time.Second)) || !w.to.Equal(w.to.Truncate(time.Second)) {
			t.Errorf("window %d [%s, %s) has a fractional bound; the driver binds whole seconds", i, w.from, w.to)
		}
		if i == 0 {
			continue
		}
		switch prev := windows[i-1]; {
		case w.from.Before(prev.to):
			t.Errorf("window %d [%s, %s) overlaps the previous one, which ended %s — events double-counted", i, w.from, w.to, prev.to)
		case w.from.After(prev.to):
			t.Errorf("window %d [%s, %s) leaves a gap after the previous one, which ended %s — events never counted", i, w.from, w.to, prev.to)
		}
	}
}

// TestEveryEventIsCountedInExactlyOneWindow is the ING-9 regression test. Events
// arrive every 7 s and on every minute boundary, each flushed up to the settle
// delay late; the evaluator must count every one of them exactly once.
func TestEveryEventIsCountedInExactlyOneWindow(t *testing.T) {
	h := newHarness(t, newRule("r1", "atlas", func(r *structs.AlertRule) { r.Threshold = 1e9 }))

	type event struct{ ts, visibleAt time.Time }
	var events []event
	lateness := []time.Duration{0, 2 * time.Second, 5 * time.Second, testSettle}
	start := windowTestBase.Add(-3 * time.Minute).Truncate(time.Minute)
	for i := 0; i < 15*60/7; i++ {
		ts := start.Add(time.Duration(i) * 7 * time.Second)
		events = append(events, event{ts, ts.Add(lateness[i%len(lateness)])})
	}
	// Exactly on each boundary, as late as the settle delay allows: half-open
	// windows must put each in the window it starts, and only that one.
	for m := 0; m < 15; m++ {
		ts := start.Add(time.Duration(m) * time.Minute)
		events = append(events, event{ts, ts.Add(testSettle)})
	}
	in := func(ev event, from, to time.Time) bool { return !ev.ts.Before(from) && ev.ts.Before(to) }

	h.value = func(_ *structs.AlertRule, _ string, from, to time.Time) (float64, error) {
		n := 0
		for _, ev := range events {
			if in(ev, from, to) && !ev.visibleAt.After(h.clock) {
				n++
			}
		}
		return float64(n), nil
	}

	h.ticks(12 * 4)

	windows := h.windowsOf("r1")
	assertContiguous(t, windows, time.Minute)
	if len(windows) < 11 {
		t.Errorf("only %d windows in 12 minutes of ticks", len(windows))
	}

	var total float64
	for i, c := range h.callsOf("r1") {
		want := 0
		for _, ev := range events {
			if in(ev, c.from, c.to) {
				want++
			}
		}
		if int(c.value) != want {
			t.Errorf("window %d [%s, %s) counted %v events, want %d — it was read before its events were flushed", i, c.from, c.to, c.value, want)
		}
		total += c.value
	}

	first, last := windows[0].from, windows[len(windows)-1].to
	want := 0
	for _, ev := range events {
		if in(ev, first, last) {
			want++
		}
	}
	if int(total) != want {
		t.Errorf("counted %v events across [%s, %s), want %d — each exactly once", total, first, last, want)
	}
}

// TestAnEventFlushedWithinTheSettleDelayIsCounted pins the settle delay itself:
// the last instant of a window, landing a full settle delay later, is still
// counted — whatever the tick's phase against the window grid.
func TestAnEventFlushedWithinTheSettleDelayIsCounted(t *testing.T) {
	ts := time.Date(2026, 9, 27, 12, 1, 59, 999_000_000, time.UTC)
	visible := ts.Add(testSettle)

	for _, phase := range []time.Duration{0, 3 * time.Second, 7*time.Second + 500*time.Millisecond, 14 * time.Second} {
		t.Run(phase.String(), func(t *testing.T) {
			h := newHarness(t, newRule("r1", "atlas", func(r *structs.AlertRule) { r.Threshold = 1e9 }))
			h.clock = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC).Add(phase)
			h.value = func(_ *structs.AlertRule, _ string, from, to time.Time) (float64, error) {
				if !ts.Before(from) && ts.Before(to) && !visible.After(h.clock) {
					return 1, nil
				}
				return 0, nil
			}

			h.ticks(16)

			var counted float64
			for _, c := range h.callsOf("r1") {
				counted += c.value
			}
			if counted != 1 {
				t.Errorf("an event flushed %s after its window closed was counted %v times, want 1", testSettle, counted)
			}
		})
	}
}

// TestNowIsTakenBeforeTheRuleListing: the listing is a MariaDB round trip, and a
// `now` read after it let a slow read move which windows were due.
func TestNowIsTakenBeforeTheRuleListing(t *testing.T) {
	h := newHarness(t, newRule("r1", "atlas"))
	h.clock = time.Date(2026, 9, 27, 12, 0, 20, 0, time.UTC)
	listedAt := h.clock
	h.onList = func() { h.clock = h.clock.Add(50 * time.Second) }

	h.evaluate()

	want := window{
		from: time.Date(2026, 9, 27, 11, 59, 0, 0, time.UTC),
		to:   time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC),
	}
	if got := h.windowsOf("r1"); len(got) != 1 || !got[0].from.Equal(want.from) || !got[0].to.Equal(want.to) {
		t.Errorf("windows = %v, want exactly %v — the one due at the clock read before the listing", got, want)
	}
	if len(h.writes) != 1 || !h.writes[0][0].UpdatedAt.Equal(listedAt) {
		t.Errorf("state stamped %v, want the pre-listing clock %s", h.writes, listedAt)
	}
}

// TestIntervalIsClampedForZeroAndSubTickValues: the update path accepts 0, and a
// 1 s rule on a 15 s tick would need fifteen windows per tick and so jump on
// every one of them.
func TestIntervalIsClampedForZeroAndSubTickValues(t *testing.T) {
	cases := []struct {
		name string
		secs uint32
		want time.Duration
	}{
		{"0 reads as 60 s", 0, 60 * time.Second},
		{"1 s is floored at the tick", 1, evaluatorTick},
		{"14 s is floored at the tick", 14, evaluatorTick},
		{"120 s is kept", 120, 120 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rule := newRule("r1", "atlas", func(r *structs.AlertRule) { r.EvaluationIntervalSecs = c.secs })
			if got := effectiveInterval(&rule, evaluatorTick); got != c.want {
				t.Fatalf("effectiveInterval = %s, want %s", got, c.want)
			}

			h := newHarness(t, rule)
			h.ticks(40)

			windows := h.windowsOf("r1")
			assertContiguous(t, windows, c.want)
			if min := int(9 * time.Minute / c.want); len(windows) < min {
				t.Errorf("%d windows in 10 minutes, want at least %d", len(windows), min)
			}
			if h.logged("behind") {
				t.Errorf("a steady tick skipped windows: %q", h.logs)
			}
		})
	}
}

// assertCovered fails if the spans, taken together, leave a hole anywhere
// between the earliest start and the latest end. Unlike assertContiguous it
// allows overlap: a restart re-reads one window on purpose.
func assertCovered(t *testing.T, spans []window) {
	t.Helper()
	sort.Slice(spans, func(i, j int) bool { return spans[i].from.Before(spans[j].from) })
	end := spans[0].to
	for _, s := range spans[1:] {
		if s.from.After(end) {
			t.Errorf("[%s, %s) was never evaluated and never logged as skipped", end, s.from)
		}
		if s.to.After(end) {
			end = s.to
		}
	}
}

// loggedSkips returns every span a "windows behind" line reported skipping.
func (h *evaluatorHarness) loggedSkips() []window {
	var out []window
	for _, l := range h.logs {
		m := skippedSpan.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		from, err1 := time.Parse(time.RFC3339, m[1])
		to, err2 := time.Parse(time.RFC3339, m[2])
		if err1 != nil || err2 != nil {
			h.t.Fatalf("unparseable skip log %q", l)
		}
		out = append(out, window{from: from, to: to})
	}
	return out
}

// TestARestartResumesAtTheLastWindowAndNeverGaps: schedules are in memory, so
// a new process resumes an inherited rule from its saved state's updated_at —
// the old process's last tick. Its first window is the last one the old
// process read, or the one right after it, whatever the restart took: never a
// gap, never more than one window re-read, and the persisted firing status
// keeps the overlap from notifying again. Seeded from `now` alone, a restart
// that outlasted one interval gapped — nearly every restart, for a 15 s rule,
// since the new process's first pass is a tick after it starts.
func TestARestartResumesAtTheLastWindowAndNeverGaps(t *testing.T) {
	cases := []struct {
		interval uint32
		delays   []time.Duration
	}{
		{60, []time.Duration{0, 7 * time.Second, 15 * time.Second, 30 * time.Second, 45 * time.Second, 59 * time.Second,
			60 * time.Second, 75 * time.Second, 90 * time.Second, 2 * time.Minute, 3*time.Minute + 20*time.Second}},
		{15, []time.Duration{0, 15*time.Second + 2*time.Second, 15*time.Second + 5*time.Second, 15*time.Second + 10*time.Second,
			30 * time.Second, 45 * time.Second}},
	}
	for _, c := range cases {
		interval := time.Duration(c.interval) * time.Second
		for _, delay := range c.delays {
			t.Run(fmt.Sprintf("%s rule, %s restart", interval, delay), func(t *testing.T) {
				h := newHarness(t, newRule("r1", "atlas", func(r *structs.AlertRule) { r.EvaluationIntervalSecs = c.interval }))
				h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) { return 1, nil }

				h.ticks(20)
				if got := h.statuses(); !reflect.DeepEqual(got, []string{"firing"}) {
					t.Fatalf("before the restart notified %v, want one firing", got)
				}
				old := h.windowsOf("r1")
				lastOld := old[len(old)-1]

				h.clock = h.clock.Add(delay)
				h.e = h.newEvaluator()
				h.calls, h.notes = nil, nil
				h.evaluate()

				fresh := h.windowsOf("r1")
				if len(fresh) == 0 {
					t.Fatal("the first pass after a restart evaluated nothing")
				}
				switch first := fresh[0]; {
				case first.from.After(lastOld.to):
					t.Errorf("restart left a gap: old process ended at %s, new one starts at %s", lastOld.to, first.from)
				case first.from.Before(lastOld.from):
					t.Errorf("restart re-read more than one window: old process's last was [%s, %s), new one starts at %s", lastOld.from, lastOld.to, first.from)
				}
				if len(h.notes) != 0 {
					t.Errorf("the overlap re-notified %v; the persisted firing status should suppress it", h.statuses())
				}
				if h.logged("behind") {
					t.Errorf("a restart of %s logged a skip: %q", delay, h.logs)
				}

				h.ticks(8)
				assertContiguous(t, h.windowsOf("r1"), interval)
			})
		}
	}

	// Longer than five intervals: the backlog is too old to replay, but the gap
	// is a logged skip starting where the old process stopped, not a silence.
	t.Run("restart longer than the catch-up cap", func(t *testing.T) {
		h := newHarness(t, newRule("r1", "atlas"))
		h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) { return 1, nil }
		h.ticks(20)
		spans := h.windowsOf("r1")

		h.clock = h.clock.Add(10 * time.Minute)
		h.e = h.newEvaluator()
		h.notes = nil
		h.ticks(8)

		skips := h.loggedSkips()
		if len(skips) != 1 {
			t.Fatalf("a 10-minute restart logged %d skips, want 1: %q", len(skips), h.logs)
		}
		if last := spans[len(spans)-1]; skips[0].from.After(last.to) {
			t.Errorf("the logged skip starts at %s, after the old process stopped at %s", skips[0].from, last.to)
		}
		assertCovered(t, append(h.windowsOf("r1"), skips...))
		if len(h.notes) != 0 {
			t.Errorf("the restart re-notified %v; the persisted firing status should suppress it", h.statuses())
		}
	})

	// Only the first listing's rules are inherited. A rule enabled later — here,
	// one disabled for two minutes and switched back on — starts fresh from its
	// latest complete window even though it has a saved state, rather than
	// replaying the time it was off.
	t.Run("a rule re-enabled later is not inherited", func(t *testing.T) {
		rule := newRule("r1", "atlas")
		h := newHarness(t, rule)
		h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) { return 1, nil }
		h.ticks(8)

		h.rules = nil
		h.ticks(8)
		h.rules = []structs.AlertRule{rule}
		before := len(h.callsOf("r1"))
		h.tick()

		calls := h.callsOf("r1")[before:]
		if seeded := h.clock.Add(-testSettle).Truncate(time.Minute); len(calls) != 1 || !calls[0].to.Equal(seeded) {
			t.Errorf("re-enabled rule evaluated %v, want one fresh window ending %s", calls, seeded)
		}
	})
}

// TestAClickHouseErrorHoldsTheSchedule: a window whose aggregate fails is not
// skipped. lastEnd stays put, nothing is written for the rule, and the first
// attempt after recovery — at most one interval later — catches the held
// windows up.
func TestAClickHouseErrorHoldsTheSchedule(t *testing.T) {
	h := newHarness(t, newRule("r1", "atlas"))
	down := false
	h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) {
		if down {
			return 0, errors.New("clickhouse: connection refused")
		}
		return 1, nil
	}

	h.ticks(8)
	good := h.windowsOf("r1")
	writes := len(h.writes)

	down = true
	h.ticks(8)
	if h.failures == 0 {
		t.Fatal("no window was attempted during the outage")
	}
	if len(h.writes) != writes {
		t.Errorf("the outage wrote %d state batches; a failed window must write nothing", len(h.writes)-writes)
	}
	if held := h.e.lastEnd["r1"]; !held.Equal(good[len(good)-1].to) {
		t.Errorf("lastEnd moved to %s during the outage, want it held at %s", held, good[len(good)-1].to)
	}
	if !h.logged("holding its schedule") {
		t.Errorf("the failure was not logged: %q", h.logs)
	}

	down = false
	var caughtUp int
	for i := 0; i < 4 && caughtUp == 0; i++ {
		h.tick()
		for _, c := range h.callsOf("r1") {
			if c.pass == h.passes {
				caughtUp++
			}
		}
	}
	if caughtUp < 2 {
		t.Errorf("the first attempt after recovery evaluated %d windows, want the held backlog (>= 2)", caughtUp)
	}

	h.ticks(8)
	assertContiguous(t, h.windowsOf("r1"), time.Minute)
	if got := h.statuses(); !reflect.DeepEqual(got, []string{"firing"}) {
		t.Errorf("notifications = %v, want the one firing from before the outage", got)
	}
}

// TestAFailingRuleIsRetriedOncePerInterval: while a window keeps failing, the
// rule is attempted — and logs — once per interval, not once per tick, and the
// first attempt after recovery is never more than an interval late.
func TestAFailingRuleIsRetriedOncePerInterval(t *testing.T) {
	for _, secs := range []uint32{15, 60, 120} {
		interval := time.Duration(secs) * time.Second
		t.Run(interval.String(), func(t *testing.T) {
			h := newHarness(t, newRule("r1", "atlas", func(r *structs.AlertRule) { r.EvaluationIntervalSecs = secs }))
			down := false
			h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) {
				if down {
					return 0, errors.New("clickhouse: code 241, memory limit exceeded")
				}
				return 0, nil
			}
			h.ticks(int(2 * interval / evaluatorTick))

			down = true
			outage := 4 * time.Minute
			h.ticks(int(outage / evaluatorTick))
			// One attempt when the first held window comes due, then one per interval.
			if most := int(outage/interval) + 1; h.failures == 0 || h.failures > most {
				t.Errorf("%d failing attempts in a %s outage, want 1..%d — one per interval", h.failures, outage, most)
			}
			held := 0
			for _, l := range h.logs {
				if strings.Contains(l, "holding its schedule") {
					held++
				}
			}
			if held != h.failures {
				t.Errorf("logged %d failures for %d attempts", held, h.failures)
			}

			down = false
			calls := len(h.calls)
			h.ticks(int(interval / evaluatorTick))
			if len(h.calls) == calls {
				t.Errorf("no attempt within one interval of recovery")
			}
		})
	}

	// A ticker's clock reads are never exactly a tick apart. One read a few
	// milliseconds early must not push the retry of a 15 s rule a whole tick late.
	t.Run("jittered ticks", func(t *testing.T) {
		h := newHarness(t, newRule("r1", "atlas", func(r *structs.AlertRule) { r.EvaluationIntervalSecs = 15 }))
		h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) {
			return 0, errors.New("clickhouse: timeout")
		}
		const n = 16
		for i := 0; i < n; i++ {
			h.clock = h.clock.Add(evaluatorTick - 3*time.Millisecond)
			h.evaluate()
		}
		if h.failures != n {
			t.Errorf("%d attempts in %d slightly short ticks, want one per tick", h.failures, n)
		}
	})
}

var skippedSpan = regexp.MustCompile(`skipping \[(\S+), (\S+)\)`)

// TestAStalledRuleJumpsToTheLatestWindowAndLogsTheSkip: more than five windows
// behind, the evaluator does not replay stale data. It evaluates the latest
// complete window only and logs exactly what it skipped.
func TestAStalledRuleJumpsToTheLatestWindowAndLogsTheSkip(t *testing.T) {
	h := newHarness(t, newRule("r1", "atlas"))
	h.tick()
	behind := h.e.lastEnd["r1"]

	h.clock = h.clock.Add(10 * time.Minute)
	h.evaluate()

	var pass []aggCall
	for _, c := range h.callsOf("r1") {
		if c.pass == h.passes {
			pass = append(pass, c)
		}
	}
	latest := h.clock.Add(-testSettle).Truncate(time.Minute)
	if len(pass) != 1 || !pass[0].to.Equal(latest) || pass[0].to.Sub(pass[0].from) != time.Minute {
		t.Fatalf("after a 10-minute stall the pass evaluated %v, want only the latest complete window ending %s", pass, latest)
	}

	var spans [][]string
	for _, l := range h.logs {
		if m := skippedSpan.FindStringSubmatch(l); m != nil {
			spans = append(spans, m[1:])
		}
	}
	want := [][]string{{behind.Format(time.RFC3339), pass[0].from.Format(time.RFC3339)}}
	if !reflect.DeepEqual(spans, want) {
		t.Errorf("logged skips %v, want %v", spans, want)
	}

	h.ticks(8)
	assertContiguous(t, h.windowsOf("r1")[1:], time.Minute)
}

// TestAJumpRestartsTheForSecondsHold: the span a jump skips was never
// evaluated, so it must not count toward for_seconds. A hold started before a
// ten-minute stall would otherwise read as met on the first window after it.
func TestAJumpRestartsTheForSecondsHold(t *testing.T) {
	h := newHarness(t, newRule("r1", "atlas", func(r *structs.AlertRule) { r.ForSeconds = 120 }))
	h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) { return 1, nil }
	h.tick()
	if _, pending := h.e.pendingSince["r1"]; !pending {
		t.Fatal("setup: a firing rule with for_seconds 120 should be pending")
	}

	h.clock = h.clock.Add(10 * time.Minute)
	h.evaluate()
	if !h.logged("behind") {
		t.Fatalf("setup: a 10-minute stall should jump: %q", h.logs)
	}
	if len(h.notes) != 0 {
		t.Errorf("fired %v on the first window after a jump; the skipped span counted toward the hold", h.statuses())
	}
	windows := h.windowsOf("r1")
	if got, want := h.e.pendingSince["r1"], windows[len(windows)-1].to; !got.Equal(want) {
		t.Errorf("pendingSince = %s after the jump, want a fresh hold from %s", got, want)
	}

	h.ticks(8)
	if got := h.statuses(); !reflect.DeepEqual(got, []string{"firing"}) {
		t.Errorf("notified %v once the fresh hold was met, want one firing", got)
	}
}

// TestALongOutageLeavesNoSilentGap: across a 12-minute outage every window is
// either evaluated or inside a logged skip — nothing disappears unannounced —
// and no single tick replays more than five windows.
func TestALongOutageLeavesNoSilentGap(t *testing.T) {
	h := newHarness(t, newRule("r1", "atlas"))
	down := false
	h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) {
		if down {
			return 0, errors.New("clickhouse: timeout")
		}
		return 0, nil
	}

	h.ticks(8)
	down = true
	h.ticks(12 * 4)
	down = false
	h.ticks(12)

	tiles := h.windowsOf("r1")
	skips := 0
	for _, l := range h.logs {
		m := skippedSpan.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		from, err1 := time.Parse(time.RFC3339, m[1])
		to, err2 := time.Parse(time.RFC3339, m[2])
		if err1 != nil || err2 != nil {
			t.Fatalf("unparseable skip log %q", l)
		}
		tiles = append(tiles, window{from: from, to: to})
		skips++
	}
	if skips == 0 {
		t.Fatal("a 12-minute outage on a 60 s rule logged no skip")
	}
	sort.Slice(tiles, func(i, j int) bool { return tiles[i].from.Before(tiles[j].from) })
	for i := 1; i < len(tiles); i++ {
		if !tiles[i].from.Equal(tiles[i-1].to) {
			t.Errorf("evaluated windows and logged skips do not tile the timeline: [%s, %s) then [%s, %s)",
				tiles[i-1].from, tiles[i-1].to, tiles[i].from, tiles[i].to)
		}
	}

	perPass := make(map[int]int)
	for _, c := range h.callsOf("r1") {
		perPass[c.pass]++
	}
	for pass, n := range perPass {
		if n > maxCatchUpWindows {
			t.Errorf("pass %d evaluated %d windows, more than the catch-up cap of %d", pass, n, maxCatchUpWindows)
		}
	}
}

// TestCatchUpNotifiesOnceAndNeverSilently: overdue windows evaluated in one
// tick notify at most once, measured against the status persisted before the
// tick — never fire, resolve, fire. Usually that is where the final window
// leaves the rule; but a rule saved as ok whose catch-up fired and cleared is
// published as firing, with the value that fired, and saved as firing: read as
// its net change, ok → ok, it would send nothing and record nothing.
func TestCatchUpNotifiesOnceAndNeverSilently(t *testing.T) {
	cases := []struct {
		name       string
		start      string
		series     []float64
		wantNotes  []string
		wantValue  float64 // of the one notification, when there is one
		wantStatus string
	}{
		{"flapping ends firing", "ok", []float64{1, 0, 4}, []string{"firing"}, 4, "firing"},
		{"a blip that has already cleared", "ok", []float64{2, 3, 0}, []string{"firing"}, 3, "firing"},
		{"a single event that has already cleared", "ok", []float64{1, 0}, []string{"firing"}, 1, "firing"},
		{"a blip in the middle", "ok", []float64{0, 5, 0}, []string{"firing"}, 5, "firing"},
		{"firing, flaps, ends clear", "firing", []float64{0, 1, 0}, []string{"resolved"}, 0, "ok"},
		{"firing, flaps, ends firing", "firing", []float64{0, 0, 1}, []string{}, 0, "firing"},
		{"starts firing on the last window", "ok", []float64{0, 0, 1}, []string{"firing"}, 1, "firing"},
		{"quiet throughout", "ok", []float64{0, 0, 0}, []string{}, 0, "ok"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, newRule("r1", "atlas"))
			h.clock = time.Date(2026, 9, 27, 12, 0, 30, 0, time.UTC)

			startValue := 0.0
			if c.start == "firing" {
				startValue = 1
			}
			var series []float64
			next := 0
			h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) {
				if series == nil {
					return startValue, nil
				}
				v := series[next]
				next++
				return v, nil
			}

			h.evaluate()
			if got := h.stored["r1"].Status; got != c.start {
				t.Fatalf("setup left status %q, want %q", got, c.start)
			}
			h.notes = nil

			series = c.series
			h.clock = h.clock.Add(time.Duration(len(c.series)) * time.Minute)
			h.evaluate()

			if next != len(c.series) {
				t.Fatalf("the catch-up evaluated %d windows, want %d", next, len(c.series))
			}
			if got := h.statuses(); !reflect.DeepEqual(got, c.wantNotes) {
				t.Errorf("notified %v, want %v", got, c.wantNotes)
			}
			if len(h.notes) == 1 && h.notes[0].value != c.wantValue {
				t.Errorf("notified value %v, want %v", h.notes[0].value, c.wantValue)
			}
			if got := h.stored["r1"].Status; got != c.wantStatus {
				t.Errorf("persisted status %q, want %q", got, c.wantStatus)
			}
		})
	}
}

// TestAFiringWindowInsideACatchUpIsNeverSilent follows the fired-and-cleared
// catch-up through: a threshold-1 P0 rule whose one event lands in a window
// that is only read as part of a catch-up — after a ClickHouse outage, or a
// slow pass on a 15 s rule — sends one firing (with its value, and a log line
// naming the window), then resolves on the next window. The old net-transition
// reading sent nothing and logged nothing for either.
func TestAFiringWindowInsideACatchUpIsNeverSilent(t *testing.T) {
	p0 := func(secs uint32) structs.AlertRule {
		return newRule("p0", "atlas", func(r *structs.AlertRule) {
			r.Condition = "gte"
			r.Threshold = 1
			r.EvaluationIntervalSecs = secs
		})
	}
	// oneEventAt counts a single event at ts, failing while down is set.
	oneEventAt := func(ts *time.Time, down *bool) func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) {
		return func(_ *structs.AlertRule, _ string, from, to time.Time) (float64, error) {
			if *down {
				return 0, errors.New("clickhouse: code 241, memory limit exceeded")
			}
			if !ts.IsZero() && !ts.Before(from) && ts.Before(to) {
				return 1, nil
			}
			return 0, nil
		}
	}
	assertFiredThenResolved := func(t *testing.T, h *evaluatorHarness, catchUpPass int) {
		t.Helper()
		var inPass int
		for _, c := range h.callsOf("p0") {
			if c.pass == catchUpPass {
				inPass++
			}
		}
		if inPass < 2 {
			t.Fatalf("the catch-up pass evaluated %d windows; the scenario needs a multi-window catch-up", inPass)
		}
		if got := h.statuses(); !reflect.DeepEqual(got, []string{"firing", "resolved"}) {
			t.Fatalf("notified %v, want firing then resolved", got)
		}
		if h.notes[0].value != 1 {
			t.Errorf("fired on value %v, want the event's window (1)", h.notes[0].value)
		}
		if !h.logged("within one catch-up") {
			t.Errorf("the fired-and-cleared catch-up was not logged: %q", h.logs)
		}
		if got := h.stored["p0"].Status; got != "ok" {
			t.Errorf("persisted status %q after the resolve, want ok", got)
		}
	}

	t.Run("after a ClickHouse outage", func(t *testing.T) {
		var ts time.Time
		down := false
		h := newHarness(t, p0(60))
		h.value = oneEventAt(&ts, &down)
		h.ticks(8)

		ts = h.e.lastEnd["p0"].Add(30 * time.Second)
		down = true
		h.ticks(8)
		down = false

		catchUpPass := 0
		for i := 0; i < 8 && catchUpPass == 0; i++ {
			calls := len(h.calls)
			h.tick()
			if len(h.calls) > calls {
				catchUpPass = h.passes
			}
		}
		if got := h.statuses(); !reflect.DeepEqual(got, []string{"firing"}) {
			t.Fatalf("the catch-up notified %v, want one firing", got)
		}
		if got := h.stored["p0"].Status; got != "firing" {
			t.Errorf("the catch-up persisted %q, want firing until the next window resolves it", got)
		}
		h.ticks(4)
		assertFiredThenResolved(t, h, catchUpPass)
	})

	t.Run("a slow pass on a 15 s rule", func(t *testing.T) {
		var ts time.Time
		down := false
		h := newHarness(t, p0(15))
		h.value = oneEventAt(&ts, &down)
		h.ticks(8)

		ts = h.e.lastEnd["p0"].Add(5 * time.Second)
		h.clock = h.clock.Add(2 * evaluatorTick)
		h.evaluate()
		catchUpPass := h.passes
		h.ticks(2)
		assertFiredThenResolved(t, h, catchUpPass)
	})
}

// TestForSecondsFiresOnlyAfterTheHold is the pendingSince regression test. The
// hold's start used to be overwritten on every evaluation, so the elapsed time
// was always zero and a for_seconds rule could never fire.
func TestForSecondsFiresOnlyAfterTheHold(t *testing.T) {
	cases := []struct {
		name    string
		series  []float64
		firesAt int // window index of the one firing notification, or -1
	}{
		{"held two windows, fires on the third", []float64{1, 1, 1, 1}, 2},
		{"a quiet window restarts the hold", []float64{1, 0, 1, 1, 1}, 4},
		{"never held long enough", []float64{1, 1, 0, 1, 1, 0}, -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, newRule("r1", "atlas", func(r *structs.AlertRule) { r.ForSeconds = 120 }))
			h.clock = time.Date(2026, 9, 27, 12, 0, 30, 0, time.UTC)
			next := 0
			h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) {
				v := c.series[next]
				next++
				return v, nil
			}

			for i := range c.series {
				if i > 0 {
					h.clock = h.clock.Add(time.Minute)
				}
				h.evaluate()

				fired := len(h.notes) > 0
				if want := c.firesAt >= 0 && i >= c.firesAt; fired != want {
					t.Fatalf("after window %d fired=%v, want %v", i, fired, want)
				}
				if !fired && h.stored["r1"].Status != "ok" {
					t.Errorf("after window %d a pending rule was persisted as %q, want ok", i, h.stored["r1"].Status)
				}
				// The hold starts at the first firing window and stays there.
				if i == 1 && c.series[0] == 1 && c.series[1] == 1 {
					first := h.windowsOf("r1")[0].to
					if got := h.e.pendingSince["r1"]; !got.Equal(first) {
						t.Errorf("pendingSince = %s after two firing windows, want the first one's end %s", got, first)
					}
				}
			}
			if c.firesAt >= 0 && !reflect.DeepEqual(h.statuses(), []string{"firing"}) {
				t.Errorf("notified %v, want one firing", h.statuses())
			}
		})
	}

	t.Run("a catch-up counts toward the hold", func(t *testing.T) {
		h := newHarness(t, newRule("r1", "atlas", func(r *structs.AlertRule) { r.ForSeconds = 120 }))
		h.clock = time.Date(2026, 9, 27, 12, 0, 30, 0, time.UTC)
		firing := false
		h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) {
			if firing {
				return 1, nil
			}
			return 0, nil
		}
		h.evaluate()

		firing = true
		h.clock = h.clock.Add(3 * time.Minute)
		h.evaluate()
		if got := h.statuses(); !reflect.DeepEqual(got, []string{"firing"}) {
			t.Errorf("three firing windows in one catch-up notified %v, want one firing", got)
		}
	})
}

// TestRateChangeComparesAdjacentWindows: the previous window is
// [from-interval, from), so on the timer it is exactly the window evaluated
// just before, and the comparison never straddles a boundary.
func TestRateChangeComparesAdjacentWindows(t *testing.T) {
	h := newHarness(t, newRule("r1", "atlas", func(r *structs.AlertRule) {
		r.Type = "rate_change"
		r.Threshold = 50
	}))
	minute := func(hh, mm int) time.Time { return time.Date(2026, 9, 27, hh, mm, 0, 0, time.UTC) }
	counts := map[time.Time]float64{
		minute(11, 58): 10, minute(11, 59): 10, minute(12, 0): 10,
		minute(12, 1): 30, minute(12, 2): 30,
	}
	h.value = func(_ *structs.AlertRule, _ string, from, _ time.Time) (float64, error) {
		return counts[from], nil
	}

	h.clock = time.Date(2026, 9, 27, 12, 0, 30, 0, time.UTC)
	var perWindow [][]string
	for i := 0; i < 4; i++ {
		if i > 0 {
			h.clock = h.clock.Add(time.Minute)
		}
		h.evaluate()
		perWindow = append(perWindow, h.statuses())
	}

	calls := h.callsOf("r1")
	if len(calls) != 8 {
		t.Fatalf("%d aggregates for 4 rate_change windows, want 8 (current and previous each)", len(calls))
	}
	var current []window
	for k := 0; k < len(calls); k += 2 {
		cur, prev := calls[k], calls[k+1]
		if !prev.to.Equal(cur.from) || !prev.from.Equal(cur.from.Add(-time.Minute)) {
			t.Errorf("window %d: previous [%s, %s) is not [from-interval, from) of [%s, %s)", k/2, prev.from, prev.to, cur.from, cur.to)
		}
		if k > 0 {
			if before := calls[k-2]; !prev.from.Equal(before.from) || !prev.to.Equal(before.to) {
				t.Errorf("window %d compared against [%s, %s), want the window evaluated before it [%s, %s)", k/2, prev.from, prev.to, before.from, before.to)
			}
		}
		current = append(current, window{from: cur.from, to: cur.to})
	}
	assertContiguous(t, current, time.Minute)

	// 10→10, 10→10, 10→30 (+200 %, fires), 30→30 (0 %, resolves)
	want := [][]string{{}, {}, {"firing"}, {"firing", "resolved"}}
	if !reflect.DeepEqual(perWindow, want) {
		t.Errorf("notifications per window = %v, want %v", perWindow, want)
	}
	if h.notes[0].value != 200 {
		t.Errorf("fired on %v %%, want 200", h.notes[0].value)
	}
}

// TestAbsenceAcrossWindows: an absence rule counts each window on its own,
// ignoring the rule's metric, fires on the first empty window and resolves on
// the first non-empty one.
func TestAbsenceAcrossWindows(t *testing.T) {
	h := newHarness(t, newRule("r1", "atlas", func(r *structs.AlertRule) {
		r.Type = "absence"
		r.Metric = "avg"
		r.Field = "data.latency_ms"
	}))
	series := []float64{3, 0, 0, 2}
	next := 0
	h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) {
		v := series[next]
		next++
		return v, nil
	}

	h.clock = time.Date(2026, 9, 27, 12, 0, 30, 0, time.UTC)
	var perWindow [][]string
	for i := range series {
		if i > 0 {
			h.clock = h.clock.Add(time.Minute)
		}
		h.evaluate()
		perWindow = append(perWindow, h.statuses())
	}

	want := [][]string{{}, {"firing"}, {"firing"}, {"firing", "resolved"}}
	if !reflect.DeepEqual(perWindow, want) {
		t.Errorf("notifications per window = %v, want %v", perWindow, want)
	}
	for _, c := range h.callsOf("r1") {
		if c.aggExpr != "toFloat64(count())" {
			t.Errorf("absence aggregated %q, want a count whatever the metric", c.aggExpr)
		}
	}
	assertContiguous(t, h.windowsOf("r1"), time.Minute)
}

// TestADisabledRuleIsForgottenAndStartsFreshWhenReEnabled: while a rule is off,
// its schedule and pending hold are dropped, so a re-enable seeds one window
// back like a new rule — it neither replays nor logs a skip of its time off.
func TestADisabledRuleIsForgottenAndStartsFreshWhenReEnabled(t *testing.T) {
	rule := newRule("r1", "atlas", func(r *structs.AlertRule) { r.ForSeconds = 600 })
	h := newHarness(t, rule)
	h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) { return 1, nil }

	h.ticks(12)
	if _, pending := h.e.pendingSince["r1"]; !pending {
		t.Fatal("setup: a firing rule with for_seconds 600 should be pending")
	}

	h.rules = nil
	h.ticks(40)
	if _, kept := h.e.lastEnd["r1"]; kept {
		t.Error("a disabled rule's schedule was kept")
	}
	if _, kept := h.e.pendingSince["r1"]; kept {
		t.Error("a disabled rule's pending hold was kept")
	}

	h.rules = []structs.AlertRule{rule}
	before := len(h.callsOf("r1"))
	h.tick()

	calls := h.callsOf("r1")[before:]
	seeded := h.clock.Add(-testSettle).Truncate(time.Minute)
	if len(calls) != 1 || !calls[0].to.Equal(seeded) {
		t.Errorf("re-enabled rule evaluated %v, want one fresh window ending %s", calls, seeded)
	}
	if h.logged("behind") {
		t.Errorf("re-enabling logged a skip of the time the rule was off: %q", h.logs)
	}
	if got := h.e.pendingSince["r1"]; !got.Equal(seeded) {
		t.Errorf("pendingSince = %s after re-enable, want a hold starting fresh at %s", got, seeded)
	}
}

// TestAFailedRuleListingForgetsNothing: a MariaDB error is not "every rule was
// disabled". The schedules survive it and the next tick catches up.
func TestAFailedRuleListingForgetsNothing(t *testing.T) {
	h := newHarness(t, newRule("r1", "atlas"))
	h.ticks(8)
	held := h.e.lastEnd["r1"]

	h.listErr = errors.New("mariadb: bad connection")
	h.ticks(8)
	if got, ok := h.e.lastEnd["r1"]; !ok || !got.Equal(held) {
		t.Errorf("lastEnd = %s (kept %v) after failed listings, want it held at %s", got, ok, held)
	}

	h.listErr = nil
	h.ticks(4)
	assertContiguous(t, h.windowsOf("r1"), time.Minute)
}

// TestOneStateReadPerProjectAndOneInsertPerTick is ING-4: however many rules
// are due, a tick reads each project's states once and writes every evaluated
// state in one INSERT. A tick with nothing due touches neither.
func TestOneStateReadPerProjectAndOneInsertPerTick(t *testing.T) {
	rules := []structs.AlertRule{
		newRule("a1", "atlas"),
		newRule("a2", "atlas"),
		newRule("a3", "atlas", func(r *structs.AlertRule) { r.EvaluationIntervalSecs = 120 }),
		newRule("b1", "beta"),
		newRule("b2", "beta"),
	}
	project := make(map[string]string)
	for _, r := range rules {
		project[r.ID] = r.Project
	}
	h := newHarness(t, rules...)
	h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) { return 1, nil }

	inserts := 0
	for i := 0; i < 24; i++ {
		reads := map[string]int{"atlas": h.reads["atlas"], "beta": h.reads["beta"]}
		writes, calls := len(h.writes), len(h.calls)
		h.tick()

		due := make(map[string]bool)
		dueProjects := make(map[string]bool)
		for _, c := range h.calls[calls:] {
			due[c.rule] = true
			dueProjects[project[c.rule]] = true
		}
		for _, p := range []string{"atlas", "beta"} {
			want := 0
			if dueProjects[p] {
				want = 1
			}
			if got := h.reads[p] - reads[p]; got != want {
				t.Errorf("tick %d read %s's states %d times, want %d", i, p, got, want)
			}
		}

		got := len(h.writes) - writes
		if len(due) == 0 {
			if got != 0 {
				t.Errorf("tick %d evaluated nothing and still wrote %d batches", i, got)
			}
			continue
		}
		if got != 1 {
			t.Fatalf("tick %d issued %d INSERTs for %d evaluated rules, want 1", i, got, len(due))
		}
		inserts++
		batch := h.writes[len(h.writes)-1]
		if len(batch) != len(due) {
			t.Errorf("tick %d wrote %d states, want every evaluated rule's (%d)", i, len(batch), len(due))
		}
		for _, s := range batch {
			if s.Project != project[s.RuleID] || !s.UpdatedAt.Equal(h.clock) {
				t.Errorf("tick %d wrote %+v, want project %q and updated_at %s", i, s, project[s.RuleID], h.clock)
			}
		}
	}
	if inserts < 5 {
		t.Errorf("only %d ticks wrote states in 6 minutes", inserts)
	}
}

// TestAStateReadFailureSkipsOnlyThatProject: a failed ListAllStates is not "no
// states". Read that way, every firing rule in the project comes back as a fresh
// ok state and fires again. Its rules are skipped for the tick instead, with
// their windows held, while other projects evaluate normally.
func TestAStateReadFailureSkipsOnlyThatProject(t *testing.T) {
	h := newHarness(t, newRule("a1", "atlas"), newRule("b1", "beta"))
	h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) { return 1, nil }
	h.clock = time.Date(2026, 9, 27, 12, 0, 30, 0, time.UTC)

	h.evaluate()
	if len(h.notes) != 2 {
		t.Fatalf("setup notified %v, want both rules firing", h.notes)
	}
	h.notes = nil

	h.readErr["atlas"] = errors.New("clickhouse: timeout")
	held := h.e.lastEnd["a1"]
	calls := len(h.calls)
	h.clock = h.clock.Add(time.Minute)
	h.evaluate()

	for _, c := range h.calls[calls:] {
		if c.rule == "a1" {
			t.Errorf("a1 was evaluated although its project's states could not be read")
		}
	}
	if got := h.e.lastEnd["a1"]; !got.Equal(held) {
		t.Errorf("a1's lastEnd moved to %s, want it held at %s", got, held)
	}
	if batch := h.writes[len(h.writes)-1]; len(batch) != 1 || batch[0].RuleID != "b1" {
		t.Errorf("wrote %v, want only b1's state", batch)
	}
	if len(h.notes) != 0 {
		t.Errorf("notified %v; a failed state read must never re-fire a firing rule", h.statuses())
	}
	if !h.logged(`failed to read alert states for project "atlas"`) {
		t.Errorf("the failed read was not logged: %q", h.logs)
	}

	delete(h.readErr, "atlas")
	h.clock = h.clock.Add(time.Minute)
	h.evaluate()
	if a1 := h.windowsOf("a1"); len(a1) != 3 {
		t.Errorf("a1 has %d windows after recovery, want 3 (the held one caught up)", len(a1))
	}
	assertContiguous(t, h.windowsOf("a1"), time.Minute)
	if len(h.notes) != 0 {
		t.Errorf("recovery notified %v; the rule was firing throughout", h.statuses())
	}
}

// TestAFailedStateWriteNeverReplaysATransition: a tick's states are written in
// one INSERT after its notifications went out. When that INSERT fails, the
// states are kept in memory, stand in for the stale stored rows, and go out
// again with the next batch — so neither a firing nor a resolve is sent twice,
// and a deleted rule's state is not written back.
func TestAFailedStateWriteNeverReplaysATransition(t *testing.T) {
	writeFailure := errors.New("clickhouse: too many parts")

	t.Run("firing", func(t *testing.T) {
		h := newHarness(t, newRule("r1", "atlas"))
		h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) { return 1, nil }
		h.writeErr = writeFailure
		h.ticks(10)
		if _, stored := h.stored["r1"]; stored {
			t.Fatal("setup: a failed INSERT stored a state")
		}
		if !h.logged("keeping them in memory") {
			t.Errorf("the failed write was not logged: %q", h.logs)
		}

		h.writeErr = nil
		h.tick()
		h.ticks(8)
		if got := h.statuses(); !reflect.DeepEqual(got, []string{"firing"}) {
			t.Errorf("notified %v, want one firing — a failed write replayed it", got)
		}
		if got := h.stored["r1"]; got == nil || got.Status != "firing" || got.LastNotifiedAt == nil {
			t.Errorf("stored %+v after recovery, want firing with its notification time", got)
		}
	})

	t.Run("resolved", func(t *testing.T) {
		h := newHarness(t, newRule("r1", "atlas"))
		firing := true
		h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) {
			if firing {
				return 1, nil
			}
			return 0, nil
		}
		h.ticks(4)
		firing = false
		h.writeErr = writeFailure
		h.ticks(10)
		h.writeErr = nil
		h.ticks(8)
		if got := h.statuses(); !reflect.DeepEqual(got, []string{"firing", "resolved"}) {
			t.Errorf("notified %v, want firing then one resolved", got)
		}
		if got := h.stored["r1"].Status; got != "ok" {
			t.Errorf("stored status %q after recovery, want ok", got)
		}
	})

	t.Run("retried on a tick with nothing due", func(t *testing.T) {
		h := newHarness(t, newRule("r1", "atlas"))
		h.clock = time.Date(2026, 9, 27, 12, 0, 30, 0, time.UTC)
		h.value = func(*structs.AlertRule, string, time.Time, time.Time) (float64, error) { return 1, nil }
		h.writeErr = writeFailure
		h.evaluate()
		h.writeErr = nil

		calls, writes := len(h.calls), len(h.writes)
		h.tick()
		if len(h.calls) != calls {
			t.Fatal("setup: the next tick should have nothing due for a 60 s rule")
		}
		if len(h.writes) != writes+1 || len(h.writes[writes]) != 1 || h.writes[writes][0].RuleID != "r1" {
			t.Errorf("the retry wrote %v, want r1's unwritten state in one INSERT", h.writes[writes:])
		}
		if got := h.stored["r1"]; got == nil || got.Status != "firing" {
			t.Errorf("stored %+v, want the retried firing state", got)
		}
	})

	t.Run("a deleted rule is not written back", func(t *testing.T) {
		h := newHarness(t, newRule("r1", "atlas"))
		h.writeErr = writeFailure
		h.evaluate()
		h.writeErr = nil
		h.rules = nil
		h.ticks(4)
		if len(h.writes) != 0 {
			t.Errorf("wrote %v for a rule that is no longer listed", h.writes)
		}
	})
}

// statementConn is a driver.Conn that executes nothing and records the
// statements it is handed. QueryRow returns a row that scans nothing, so an
// aggregate reads as 0. Everything else panics, so a path that reaches
// ClickHouse some other way cannot slip past unnoticed.
type statementConn struct {
	mu      sync.Mutex
	queries []recordedStatement
	execs   []recordedStatement
}

type recordedStatement struct {
	sql  string
	args []any
}

func (c *statementConn) QueryRow(_ context.Context, query string, args ...any) driver.Row {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, recordedStatement{query, args})
	return zeroRow{}
}

func (c *statementConn) Exec(_ context.Context, query string, args ...any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.execs = append(c.execs, recordedStatement{query, args})
	return nil
}

func (c *statementConn) Contributors() []string                        { return nil }
func (c *statementConn) ServerVersion() (*driver.ServerVersion, error) { return nil, nil }
func (c *statementConn) Select(context.Context, any, string, ...any) error {
	panic("statementConn: unexpected Select")
}
func (c *statementConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	panic("statementConn: unexpected Query")
}
func (c *statementConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	panic("statementConn: unexpected PrepareBatch")
}
func (c *statementConn) AsyncInsert(context.Context, string, bool, ...any) error {
	panic("statementConn: unexpected AsyncInsert")
}
func (c *statementConn) Ping(context.Context) error { return nil }
func (c *statementConn) Stats() driver.Stats        { return driver.Stats{} }
func (c *statementConn) Close() error               { return nil }

type zeroRow struct{}

func (zeroRow) Err() error           { return nil }
func (zeroRow) Scan(...any) error    { return nil }
func (zeroRow) ScanStruct(any) error { return nil }

// withStatementConn swaps db.Conn for a recorder and pins db.Database.
func withStatementConn(t *testing.T) *statementConn {
	t.Helper()
	conn := &statementConn{}
	previousConn, previousDatabase := db.Conn, db.Database
	db.Conn, db.Database = conn, "monitor"
	t.Cleanup(func() { db.Conn, db.Database = previousConn, previousDatabase })
	return conn
}

// TestUpsertStatesIsOneMultiRowInsert pins the write half of ING-4 at the
// statement: one INSERT with a tuple per state and the args in column order.
func TestUpsertStatesIsOneMultiRowInsert(t *testing.T) {
	conn := withStatementConn(t)
	at := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	fired := at.Add(-time.Minute)
	states := []*State{
		{RuleID: "a1", Project: "atlas", Status: "firing", Value: 3, FiredAt: &fired, LastNotifiedAt: &fired, UpdatedAt: at},
		{RuleID: "a2", Project: "atlas", Status: "ok", Value: 0, UpdatedAt: at},
		{RuleID: "b1", Project: "beta", Status: "ok", Value: 1, UpdatedAt: at},
	}

	if err := UpsertStates(context.Background(), states); err != nil {
		t.Fatalf("UpsertStates: %v", err)
	}
	if len(conn.execs) != 1 {
		t.Fatalf("%d statements for 3 states, want one INSERT", len(conn.execs))
	}
	tuple := "(?, ?, ?, ?, ?, ?, ?, ?)"
	want := "INSERT INTO monitor.alert_states (rule_id, project, status, value, fired_at, resolved_at, last_notified_at, updated_at) VALUES " +
		tuple + ", " + tuple + ", " + tuple
	if got := conn.execs[0].sql; got != want {
		t.Errorf("statement =\n\t%s\nwant\n\t%s", got, want)
	}
	var noTime *time.Time
	wantArgs := []any{
		"a1", "atlas", "firing", 3.0, &fired, noTime, &fired, at,
		"a2", "atlas", "ok", 0.0, noTime, noTime, noTime, at,
		"b1", "beta", "ok", 1.0, noTime, noTime, noTime, at,
	}
	if got := conn.execs[0].args; !reflect.DeepEqual(got, wantArgs) {
		t.Errorf("args =\n\t%v\nwant\n\t%v", got, wantArgs)
	}

	if err := UpsertStates(context.Background(), nil); err != nil || len(conn.execs) != 1 {
		t.Errorf("an empty batch issued a statement (err %v)", err)
	}
}

// brokenStreamConn answers Query with a stream that yields no rows and then
// reports an error — a ClickHouse read that broke partway.
type brokenStreamConn struct {
	*statementConn
	err error
}

func (c brokenStreamConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return brokenRows{c.err}, nil
}

type brokenRows struct{ err error }

func (brokenRows) Next() bool                       { return false }
func (brokenRows) Scan(...any) error                { return nil }
func (brokenRows) ScanStruct(any) error             { return nil }
func (brokenRows) ColumnTypes() []driver.ColumnType { return nil }
func (brokenRows) Totals(...any) error              { return nil }
func (brokenRows) Columns() []string                { return nil }
func (brokenRows) Close() error                     { return nil }
func (r brokenRows) Err() error                     { return r.err }

// TestListAllStatesReportsABrokenStream: the evaluator's only state source
// must not turn a read that broke partway into a shorter map with a nil error.
// Every firing rule missing from it would be built as a fresh ok state and
// fire again — the "no states" reading evaluateAll refuses for an outright
// error, reached through a partial one.
func TestListAllStatesReportsABrokenStream(t *testing.T) {
	withDefaultProject(t, "default")
	broken := errors.New("clickhouse: unexpected EOF")
	conn := brokenStreamConn{statementConn: withStatementConn(t), err: broken}
	db.Conn = conn

	states, err := ListAllStates(context.Background(), "atlas")
	if !errors.Is(err, broken) {
		t.Errorf("ListAllStates = %v, %v; want the stream's error", states, err)
	}
}

// TestTimerAndTestEndpointEvaluateTheSameHalfOpenWindow runs both paths through
// the real queryAggForRange: the same statement, the same window length — the
// effective interval, so an interval of 0 is 60 s on both — and whole-second
// half-open bounds.
func TestTimerAndTestEndpointEvaluateTheSameHalfOpenWindow(t *testing.T) {
	withDefaultProject(t, "default")
	conn := withStatementConn(t)
	rule := newRule("r1", "atlas", func(r *structs.AlertRule) {
		r.EvaluationIntervalSecs = 0
		r.QueryFilters = `[{"field":"service","operator":"eq","value":"atlas-api"}]`
	})

	before := time.Now().UTC().Truncate(time.Second)
	if _, _, err := EvaluateRuleNow(scope.WithProject(context.Background(), "atlas"), &rule); err != nil {
		t.Fatalf("EvaluateRuleNow: %v", err)
	}
	after := time.Now().UTC()
	if len(conn.queries) != 1 {
		t.Fatalf("EvaluateRuleNow issued %d statements, want 1", len(conn.queries))
	}
	endpoint := conn.queries[0]
	from, to := endpoint.args[0].(time.Time), endpoint.args[1].(time.Time)
	if to.Sub(from) != defaultEvaluationInterval {
		t.Errorf("test endpoint window is %s, want the effective %s", to.Sub(from), defaultEvaluationInterval)
	}
	if !to.Equal(to.Truncate(time.Second)) || to.Before(before) || to.After(after) {
		t.Errorf("test endpoint window ends %s, want now (%s..%s) in whole seconds", to, before, after)
	}

	h := newHarness(t, rule)
	h.e.aggregate = queryAggForRange
	h.evaluate()
	if len(conn.queries) != 2 {
		t.Fatalf("the timer issued %d statements, want 1", len(conn.queries)-1)
	}
	timer := conn.queries[1]
	if timer.sql != endpoint.sql {
		t.Errorf("timer and test endpoint build different statements:\n\ttimer: %s\n\thttp:  %s", timer.sql, endpoint.sql)
	}
	if !strings.Contains(timer.sql, "timestamp >= ? AND timestamp < ?") {
		t.Errorf("the window is not half-open:\n\t%s", timer.sql)
	}
	tf, tt := timer.args[0].(time.Time), timer.args[1].(time.Time)
	if tt.Sub(tf) != defaultEvaluationInterval {
		t.Errorf("timer window is %s, want %s", tt.Sub(tf), defaultEvaluationInterval)
	}
	if !reflect.DeepEqual(timer.args[2:], endpoint.args[2:]) {
		t.Errorf("bound args after the window differ: timer %v, http %v", timer.args[2:], endpoint.args[2:])
	}
}
