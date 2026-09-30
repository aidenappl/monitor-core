package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/telemetry"
)

// CLOSED — TIMER-DRIVEN alert evaluation used to be ZONE-WIDE. Recorded
// 2026-09-06 as a known gap, narrowed the same day when its HTTP half turned out
// to be a live leak, and closed here by migration 127.
//
// WHAT IT WAS. evaluateRuleState is reached from two places, and they differed
// in the only thing that mattered — whether a request was on the other end:
//
//   - Evaluator.Run, on a 15-second timer, with a BACKGROUND context. There was
//     no request, no credential and therefore no project, so the aggregate ran
//     over the whole zone. A rule reading "more than 50 errors in a minute"
//     counted every tenant's errors, and fired for a project whose own traffic
//     was quiet.
//   - EvaluateRuleNow, from routes.HandleTestAlertRule, which passes the
//     request's context and IS scoped. That half was never a gap, it was an
//     ORACLE: a rule carries a caller-chosen aggregation, field and filter set,
//     so an admin key bound to project A could POST a rule and read back
//     max(data.amount) or a contains-filtered count over EVERY project's events,
//     one number at a time, with no row crossing the boundary to notice. It was
//     closed first, on its own.
//
// The consequence the old note recorded — that a rule's TEST value and its
// FIRING value legitimately disagreed, one scoped and one not — was a genuinely
// bad property to ship: the endpoint an operator uses to check a rule reported
// something other than what the rule does.
//
// WHAT CLOSED IT. alert_rules has a `project` column (migration 127), so the
// timer now has a project without needing a request: evaluateAll stamps each
// rule's OWN project onto the context before evaluating it, and every aggregate
// underneath is scoped by scope.ProjectPredicate exactly as the HTTP path
// already was. The two paths now build the same statement for the same rule,
// which alerts/evaluator_test.go pins directly.
//
// THE FALL-THROUGH IS GONE, and that is the part worth defending. buildAggQuery
// used to swallow scope.ErrNoProject and continue unscoped, which meant an
// unscoped aggregate was reachable by simply not having a project. It now
// propagates, so an unscoped aggregate is UNCONSTRUCTABLE — a future caller that
// forgets the project gets an error on the first evaluation instead of every
// tenant's numbers, which is the same property scope.ProjectPredicate's own
// header claims for the event reads.
//
// listEnabledRules stays UNSCOPED on purpose and is not a hole in this: the
// evaluator must run every project's rules, and the tenancy is applied per rule
// one layer down. query.ListEnabledAlertRules carries that argument in full.
//
// "project" also remains in structs.FilterColumns, so a rule can still narrow
// itself further with a query_filters entry — it can no longer WIDEN itself,
// because the predicate is ANDed on top.

// WINDOWS, NOT A WALL-CLOCK LOOKBACK (ING-9). The timer used to evaluate each
// rule over [now-interval, now], with `now` read AFTER the MariaDB rule listing,
// and to skip a rule whenever less than `interval` had passed since its last
// run. On a 15-second ticker that slipped a 60 s rule to a 75 s cadence, so about
// one event in five fell between two evaluations and was never counted, and an
// event still sitting in the batcher's buffer (up to FLUSH_INTERVAL) when its
// window was read was never counted either, because the next lookback had
// already moved past it. A single-event P0 rule could miss its one event.
//
// Each rule now walks a contiguous series of HALF-OPEN windows
// [lastEnd, lastEnd+interval), and a window is evaluated only once its end is
// older than the SETTLE delay — FLUSH_INTERVAL plus 3 s (the SDK's 1 s flush and
// slack), the Prometheus query_offset pattern — so every event lands in exactly
// one window and has been flushed before that window is read. The rules that
// make it safe, each pinned by a test in evaluator_schedule_test.go:
//
//   - The interval is max(evaluation_interval_seconds, tick), with 0 read as
//     60 s. The update path accepts 0, which would make zero-width windows (a
//     threshold never fires, an absence always does), and a 1 s rule would need
//     fifteen windows per tick.
//   - `now` is taken BEFORE the rule listing, so a slow MariaDB read cannot move
//     which windows are due.
//   - lastEnd lives in memory and is seeded on first sight, after the rule's
//     project states are read. A new or re-enabled rule starts at
//     floor(now-settle) - interval, with the floor taken on the interval grid:
//     its latest complete window. A rule this process INHERITED — one in its
//     first successful listing that already has a saved state — starts from
//     that state instead. Its updated_at is the old process's tick, so the seed
//     re-reads exactly the last window the old process evaluated, however long
//     the restart took (seeding from `now` alone gapped whenever the restart,
//     including the new process's first 15 s tick, outlasted one interval —
//     nearly always for a 15 s rule). Re-reading that ONE window reproduces the
//     saved status, which is what stops the overlap notifying twice; seeding
//     further back would re-read windows whose transitions were already sent.
//     A restart longer than five intervals becomes a logged jump (below), not
//     a silent gap.
//   - lastEnd advances only past a window whose aggregate succeeded, and only
//     after the project's states were read. A ClickHouse error holds it, and
//     the rule is retried once per interval, not every tick, so an outage
//     costs each rule one failing aggregate and one log line per interval. The
//     first attempt after recovery catches the held windows up.
//   - A rule more than five windows behind jumps to the latest complete window
//     and logs the span it skipped, rather than replaying stale data. Its
//     for_seconds hold restarts too: time nobody evaluated never counts toward
//     it.
//   - A catch-up of up to five windows evaluates each of them in order but
//     notifies AT MOST ONCE, measured against the status persisted before the
//     tick — never fire, resolve, fire within one tick about minutes-old data.
//     Usually that is the transition the final window leaves the rule in. The
//     exception is a rule saved as ok whose catch-up fired and then cleared
//     ([1, 0], [1, 1, 0]): its net change is ok → ok, and reading it that way
//     dropped the single-event P0 this scheme exists to catch without a
//     notification, a history row or a log line. That tick is published as
//     ok → firing, with the value of the last window that fired, and saved as
//     firing; the next tick's window resolves it.
//   - A disabled or deleted rule's lastEnd, pending hold, retry backoff and any
//     unwritten state are dropped, so a re-enable starts fresh instead of
//     replaying the time it was off.
//
// Window bounds are WHOLE SECONDS by construction — integer-second intervals on
// a grid anchored at the zero time — and that matters: clickhouse-go binds a
// time.Time at second precision, so a fractional bound would be truncated on the
// wire and two adjacent windows could overlap or gap by the fraction.
//
// for_seconds (pending → firing). A firing window on an ok rule whose
// for_seconds is set starts a hold at that window's end. The rule stays ok
// (persisted as ok) until a firing window ends at least for_seconds later, and
// any non-firing window drops the hold. The start is recorded ONLY on that
// ok→pending transition: it used to be overwritten on every evaluation, so the
// elapsed time was always zero and a for_seconds rule could never fire. The hold
// is measured in window time, not wall time, so catch-up windows advance it
// exactly as live ones do. It is in memory, so a restart restarts the hold —
// and because every push rolls monitor-core, a rule whose for_seconds is
// longer than the gap between deploys may not fire during a run of deploys.
// Persisting the hold needs a column on alert_states; that is left to the
// owner.
//
// ONE STATE READ PER PROJECT, ONE WRITE PER TICK (ING-4). The loop used to issue
// a SELECT … FINAL and a single-row INSERT into alert_states for every rule on
// every evaluation — a tiny part per rule per minute. It now reads a project's
// states once per tick, and only when one of its rules is due, and writes every
// evaluated rule's state in ONE multi-row INSERT. Every evaluated state is still
// rewritten, not only the changed ones: migration 007's project re-stamp
// depends on that. A failed state read skips that project's rules for the tick
// and holds their windows; treating it as "no states" would read every firing
// rule as ok and fire it again. A failed INSERT does not lose the tick either:
// its states are kept in memory, used in place of the stored rows for those
// rules, and written again with the next tick's batch, so a transient error
// cannot replay transitions the tick already notified.
//
// KNOWN LIMITS. Only alert_states survives a restart. A process that dies after
// publishing a tick but before its INSERT lands replays those transitions on
// the next start. An inherited rule's seed assumes the old process was caught
// up at its last write: if its last tick evaluated only part of a catch-up
// before a ClickHouse error and it then exited, the windows in between are
// skipped without a log line.

const (
	// evaluatorTick is how often Run wakes to look for windows that have become
	// due. It is also the floor on a rule's interval.
	evaluatorTick = 15 * time.Second

	// defaultEvaluationInterval stands in for an evaluation_interval_seconds of
	// 0. Create already defaults 0 to 60; update does not, so the evaluator has
	// to cope with it rather than build zero-width windows.
	defaultEvaluationInterval = 60 * time.Second

	// settleMargin is added to FLUSH_INTERVAL to give the settle delay: 1 s for
	// the SDK's own flush and 2 s of slack for the insert to land.
	settleMargin = 3 * time.Second

	// maxCatchUpWindows is how many overdue windows one tick evaluates before it
	// gives up on the backlog and jumps to the latest complete window.
	maxCatchUpWindows = 5
)

// window is one half-open evaluation window [from, to).
type window struct {
	from, to time.Time
}

// aggregateFunc runs one aggregate expression over [from, to) for a rule.
// queryAggForRange in production; the evaluator holds it as a field so the
// scheduling tests can count events without a ClickHouse.
type aggregateFunc func(ctx context.Context, rule *structs.AlertRule, aggExpr string, from, to time.Time) (float64, error)

// Evaluator periodically evaluates alert rules.
//
// Every dependency is a field so evaluator_schedule_test.go can drive the loop
// with an injected clock and fake stores. NewEvaluator wires the real ones.
// Everything here is touched only from Run's goroutine, so the maps need no
// lock.
type Evaluator struct {
	router   *Router
	alertHub *AlertHub

	tick   time.Duration
	settle time.Duration
	now    func() time.Time

	listRules    func() ([]structs.AlertRule, error)
	listStates   func(ctx context.Context, project string) (map[string]*State, error)
	upsertStates func(ctx context.Context, states []*State) error
	aggregate    aggregateFunc
	// publish performs a notification's side effects: history, the SSE hub and
	// the router. It is called only for the transition a tick notifies.
	publish func(ctx context.Context, rule *structs.AlertRule, state *State, status, msg string)
	// logf carries the evaluator's schedule narrative — which windows were
	// skipped, which rule is holding, which catch-up fired and cleared. In
	// production it goes to telemetry at DEBUG (see evaluatorTrace): every line
	// here accompanies a structured event that carries the same failure with
	// fields, and printing both would report one failure twice. Tests replace it
	// to assert on the narrative directly.
	logf func(format string, args ...any)

	// lastEnd is the end of the last window evaluated per rule — where that
	// rule's next window starts.
	lastEnd map[string]time.Time
	// pendingSince is the end of the window that started a rule's for_seconds
	// hold.
	pendingSince map[string]time.Time
	// retryAt is the earliest tick a rule whose window failed is tried again:
	// one attempt per interval while ClickHouse is failing, not one per tick.
	retryAt map[string]time.Time
	// booted is set by the first successful rule listing. inherited holds the
	// rules of that listing not yet seeded — the ones a previous process may
	// have been evaluating, whose saved state says where it stopped.
	booted    bool
	inherited map[string]bool
	// unsaved holds the states of a tick whose INSERT failed, by rule ID. They
	// stand in for the stored rows and are written again with the next batch.
	unsaved map[string]*State
}

// NewEvaluator creates a new alert evaluator wired to the real stores and clock.
func NewEvaluator(alertHub *AlertHub) *Evaluator {
	e := &Evaluator{
		router:       NewRouter(),
		alertHub:     alertHub,
		tick:         evaluatorTick,
		settle:       env.FlushInterval + settleMargin,
		now:          time.Now,
		listRules:    listEnabledRules,
		listStates:   ListAllStates,
		upsertStates: UpsertStates,
		aggregate:    queryAggForRange,
		logf:         evaluatorTrace,
		lastEnd:      make(map[string]time.Time),
		pendingSince: make(map[string]time.Time),
		retryAt:      make(map[string]time.Time),
		inherited:    make(map[string]bool),
		unsaved:      make(map[string]*State),
	}
	e.publish = e.publishTransition
	return e
}

// Run starts the evaluator loop, checking every tick for windows that are due.
func (e *Evaluator) Run(ctx context.Context) {
	ticker := time.NewTicker(e.tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.evaluateAll(ctx)
		}
	}
}

func (e *Evaluator) evaluateAll(ctx context.Context) {
	// Captured BEFORE the rule listing, not after it: the listing is a MariaDB
	// round trip, and a `now` taken after it let a slow read decide which windows
	// were due.
	now := e.now().UTC()

	rules, err := e.listRules()
	if err != nil {
		e.logf("alert evaluator: failed to list rules: %v", err)
		// Coalesced: a MariaDB outage fails every tick the same way.
		telemetry.ErrorCoalesced(ctx, "alert.rules.list.failed", "alert.rules.list.failed", err, map[string]any{
			"dependency": "mariadb",
			"reason":     "mariadb_read_failed",
			"outcome":    "no rule was evaluated this tick",
		})
		return
	}

	e.forgetRulesNotIn(rules)
	if !e.booted {
		// The first listing's rules are the ones a previous process may have been
		// evaluating right up to this one starting. A rule enabled later is new,
		// or re-enabled, and starts fresh.
		e.booted = true
		for i := range rules {
			e.inherited[rules[i].ID] = true
		}
	}

	// Read lazily, once per project, and only for a project with a rule due this
	// tick. A project whose read failed is remembered so its other rules are
	// skipped too, rather than retried rule by rule.
	states := make(map[string]map[string]*State)
	unreadable := make(map[string]bool)
	var evaluated []*State

	for i := range rules {
		rule := &rules[i]
		if unreadable[rule.Project] {
			continue
		}
		if retry, backingOff := e.retryAt[rule.ID]; backingOff && now.Before(retry) {
			continue
		}
		// A rule with no schedule yet is always due: seeding leaves at least its
		// latest complete window to evaluate. It is seeded below, once its saved
		// state has been read.
		_, scheduled := e.lastEnd[rule.ID]
		if scheduled && !e.hasDueWindow(rule, now) {
			continue
		}

		projectStates, read := states[rule.Project]
		if !read {
			projectStates, err = e.listStates(ctx, rule.Project)
			if err != nil {
				// NEVER read as "no states". Every firing rule would come back as
				// a fresh ok state and fire again. The windows are held, so the
				// next tick evaluates them.
				e.logf("alert evaluator: failed to read alert states for project %q; skipping its rules this tick: %v", rule.Project, err)
				// Coalesced per project: every rule in it is skipped for the
				// same reason, and the next tick fails the same way.
				telemetry.WarnCoalesced(ctx, "alert.state.read.failed:"+rule.Project, "alert.state.read.failed", err, map[string]any{
					"project":    rule.Project,
					"dependency": "clickhouse",
					"reason":     "state_read_failed",
					"outcome":    "the project's rules were held unevaluated; their windows are evaluated on a later tick",
				})
				unreadable[rule.Project] = true
				continue
			}
			if projectStates == nil {
				projectStates = make(map[string]*State)
			}
			states[rule.Project] = projectStates
		}

		persisted := projectStates[rule.ID]
		if s, ok := e.unsaved[rule.ID]; ok {
			// Newer than the stored row, which the failed INSERT never replaced.
			cp := *s
			persisted = &cp
		}
		if !scheduled {
			e.seed(rule, now, persisted)
		}
		windows := e.dueWindows(rule, now)
		if len(windows) == 0 {
			continue
		}

		// THE ONE LINE THAT CLOSES THE ZONE-WIDE GAP. Run's context is a
		// background one with no project, so without this every aggregate below
		// counted the whole zone. The rule's own column is the authority here —
		// there is no credential on this path to derive anything else from — and
		// it is NOT NULL (migration 127), so a rule that reached this loop always
		// carries one. Should it ever be empty, ProjectPredicate refuses and the
		// rule fails loudly rather than quietly counting every tenant.
		//
		// Stamped PER RULE, not once for the loop: two rules in one pass
		// routinely belong to different projects.
		ruleCtx := scope.WithProject(ctx, rule.Project)

		if state := e.evaluateRule(ruleCtx, rule, windows, persisted, now); state != nil {
			evaluated = append(evaluated, state)
		}
	}

	e.saveStates(ctx, evaluated)
}

// saveStates writes a tick's evaluated states in one INSERT, together with any
// an earlier tick failed to write whose rules were not evaluated again. On
// failure the whole batch is kept in e.unsaved: those rules' next windows start
// from it rather than from the stale stored rows, which is what stops them
// re-sending a transition this tick already published.
func (e *Evaluator) saveStates(ctx context.Context, evaluated []*State) {
	batch := evaluated
	if len(e.unsaved) > 0 {
		superseded := make(map[string]bool, len(evaluated))
		for _, s := range evaluated {
			superseded[s.RuleID] = true
		}
		var retry []string
		for id := range e.unsaved {
			if !superseded[id] {
				retry = append(retry, id)
			}
		}
		sort.Strings(retry)
		for _, id := range retry {
			batch = append(batch, e.unsaved[id])
		}
	}
	if len(batch) == 0 {
		return
	}

	if err := e.upsertStates(ctx, batch); err != nil {
		e.logf("alert evaluator: failed to write %d alert states; keeping them in memory and retrying with the next tick: %v", len(batch), err)
		// An error, not a warning: until this lands, a restart re-reads the
		// stored rows and re-evaluates from where they say, so a firing rule can
		// notify again.
		telemetry.ErrorCoalesced(ctx, "alert.state.write.failed", "alert.state.write.failed", err, map[string]any{
			"states":     len(batch),
			"dependency": "clickhouse",
			"reason":     "state_write_failed",
			"outcome":    "held in memory and written again with the next tick; a restart before then re-notifies",
		})
		e.unsaved = make(map[string]*State, len(batch))
		for _, s := range batch {
			e.unsaved[s.RuleID] = s
		}
		return
	}
	clear(e.unsaved)
}

// forgetRulesNotIn drops everything held in memory for a rule that is no
// longer enabled — its schedule, pending hold, retry backoff, inherited seed
// and unwritten state — so one that is re-enabled later starts from a fresh
// seed instead of replaying, or jumping over, the time it was off. Dropping the
// unwritten state also keeps a deleted rule's row from being written back.
func (e *Evaluator) forgetRulesNotIn(rules []structs.AlertRule) {
	enabled := make(map[string]bool, len(rules))
	for i := range rules {
		enabled[rules[i].ID] = true
	}
	for id := range e.lastEnd {
		if !enabled[id] {
			delete(e.lastEnd, id)
		}
	}
	for id := range e.pendingSince {
		if !enabled[id] {
			delete(e.pendingSince, id)
		}
	}
	for id := range e.retryAt {
		if !enabled[id] {
			delete(e.retryAt, id)
		}
	}
	for id := range e.inherited {
		if !enabled[id] {
			delete(e.inherited, id)
		}
	}
	for id := range e.unsaved {
		if !enabled[id] {
			delete(e.unsaved, id)
		}
	}
}

// effectiveInterval is the window length a rule is evaluated over: its
// evaluation_interval_seconds, with 0 read as 60 s, and never shorter than the
// tick. The test endpoint uses it too, so it reports the window the timer uses.
func effectiveInterval(rule *structs.AlertRule, tick time.Duration) time.Duration {
	interval := time.Duration(rule.EvaluationIntervalSecs) * time.Second
	if interval <= 0 {
		interval = defaultEvaluationInterval
	}
	if interval < tick {
		interval = tick
	}
	return interval
}

// hasDueWindow reports whether a rule already on a schedule has a complete
// window waiting — whether dueWindows would return anything — without reading
// its state first.
func (e *Evaluator) hasDueWindow(rule *structs.AlertRule, now time.Time) bool {
	next := e.lastEnd[rule.ID].Add(effectiveInterval(rule, e.tick))
	return !next.After(now.Add(-e.settle))
}

// seed puts a rule this process has not scheduled yet on the window grid.
// persisted is its saved state, or nil. See the header for why an inherited
// rule resumes from its saved state and nothing else does.
func (e *Evaluator) seed(rule *structs.AlertRule, now time.Time, persisted *State) {
	interval := effectiveInterval(rule, e.tick)

	// floor on the interval grid, minus one interval: the first window is the
	// latest complete one.
	last := now.Add(-e.settle).Truncate(interval).Add(-interval)

	if e.inherited[rule.ID] {
		delete(e.inherited, rule.ID)
		if persisted != nil && !persisted.UpdatedAt.IsZero() {
			// updated_at is the old process's `now`, but it went over the wire at
			// whole seconds, so that clock read anywhere in [U, U+1s). Taking the
			// top of that range can land one window later than the window the old
			// process read last — contiguous — but never earlier, which would
			// re-read a window whose transition was already sent.
			prevNow := persisted.UpdatedAt.Truncate(time.Second).Add(time.Second - time.Nanosecond)
			resume := prevNow.Add(-e.settle).Truncate(interval).Add(-interval)
			// Only ever further back: a saved clock ahead of this one (skew
			// between hosts) must not push the schedule into the future.
			if resume.Before(last) {
				last = resume
			}
		}
	}
	e.lastEnd[rule.ID] = last
}

// dueWindows returns the complete windows a rule has not yet evaluated, oldest
// first, jumping over a backlog longer than maxCatchUpWindows. The rule must
// have been seeded. See the header above for why each rule exists.
func (e *Evaluator) dueWindows(rule *structs.AlertRule, now time.Time) []window {
	interval := effectiveInterval(rule, e.tick)
	settled := now.Add(-e.settle)

	last, known := e.lastEnd[rule.ID]
	if !known {
		e.seed(rule, now, nil)
		last = e.lastEnd[rule.ID]
	}

	due := int(settled.Sub(last) / interval)
	if due <= 0 {
		return nil
	}
	if due > maxCatchUpWindows {
		latest := last.Add(time.Duration(due-1) * interval)
		e.logf("alert evaluator: rule %s (%s) is %d windows behind; skipping [%s, %s) unevaluated and resuming at the latest complete window",
			rule.ID, rule.Name, due, last.Format(time.RFC3339), latest.Format(time.RFC3339))
		// A span of this rule's history is being abandoned, so it is a warning
		// even though nothing failed here: an alert that should have fired in it
		// never will. Coalesced per rule — a long outage skips on many ticks.
		telemetry.WarnCoalesced(context.Background(), "alert.schedule.windows.skipped:"+rule.ID, "alert.schedule.windows.skipped", nil, map[string]any{
			"rule_id":      rule.ID,
			"project":      rule.Project,
			"windows":      due,
			"skipped_from": last.Format(time.RFC3339),
			"skipped_to":   latest.Format(time.RFC3339),
			"reason":       "beyond_catch_up_limit",
			"outcome":      "the span was never evaluated; evaluation resumed at the latest complete window",
		})
		// Recorded now, not after the window succeeds: the span is abandoned
		// either way, and holding it would log the same skip every tick.
		last = latest
		e.lastEnd[rule.ID] = latest
		due = 1
		// The skipped span was never evaluated, so it cannot count toward a
		// for_seconds hold: a hold started before it starts again after it.
		delete(e.pendingSince, rule.ID)
	}

	windows := make([]window, due)
	for i := range windows {
		from := last.Add(time.Duration(i) * interval)
		windows[i] = window{from: from, to: from.Add(interval)}
	}
	return windows
}

// EvaluateRuleNow evaluates a single rule and returns the current value and
// whether it is firing (for the test endpoint). It branches on rule.Type just
// like the live evaluator, over [now-interval, now) with the interval the timer
// uses, through the same statement builder.
//
// The context's project is used AS IT ARRIVES, and is deliberately not
// overwritten with rule.Project the way the timer does it. The two look
// interchangeable and are not: this is an HTTP path, and on an HTTP path the
// CREDENTIAL decides what may be read, never a field of the row being read. They
// agree today because routes.HandleTestAlertRule loads the rule through a
// project-scoped GetRule, so rule.Project is the caller's project by
// construction — and if a future caller ever loads a rule some other way, the
// credential still wins here rather than the row handing itself a wider scope.
func EvaluateRuleNow(ctx context.Context, rule *structs.AlertRule) (value float64, isFiring bool, err error) {
	// Whole seconds, because that is what the driver binds (see the header).
	now := time.Now().UTC().Truncate(time.Second)
	interval := effectiveInterval(rule, evaluatorTick)
	return evaluateRuleState(ctx, queryAggForRange, rule, now.Add(-interval), now)
}

// evaluateRule evaluates a rule over its due windows, oldest first, and returns
// the state to persist — nil when not even the first window could be evaluated,
// in which case nothing about the rule changed and nothing is written.
//
// persisted is the rule's row from this tick's state read, or nil for a rule
// that has none yet.
func (e *Evaluator) evaluateRule(ctx context.Context, rule *structs.AlertRule, windows []window, persisted *State, now time.Time) *State {
	state := persisted
	if state == nil {
		state = &State{
			RuleID:    rule.ID,
			Project:   rule.Project,
			Status:    "ok",
			UpdatedAt: now,
		}
	}

	// Re-stamped on the read path too, not only when the state is new. A row
	// written before migration 007 comes back with an empty project, and writing
	// it straight back would keep it invisible to every non-default project
	// forever — one evaluation is all it takes to repair, and this is where it
	// happens.
	state.Project = rule.Project

	wasFiring := state.Status == "firing"

	// fired is the last window this tick that left the rule firing, firedValue
	// that window's value, and final the last window evaluated at all.
	var fired, final window
	var firedValue float64
	firedThisTick := false

	evaluated := 0
	delete(e.retryAt, rule.ID)
	for _, w := range windows {
		value, isFiring, err := evaluateRuleState(ctx, e.aggregate, rule, w.from, w.to)
		if err != nil {
			// Once per interval, not once per tick: the retry and this line both
			// wait for retryAt. Half a tick early, so a clock read a few
			// milliseconds early on the retry's tick cannot push it a tick late.
			interval := effectiveInterval(rule, e.tick)
			e.retryAt[rule.ID] = now.Add(interval - e.tick/2)
			e.logf("alert evaluator: failed to query rule %s (%s) over [%s, %s); holding its schedule and retrying in %s: %v",
				rule.ID, rule.Name, w.from.Format(time.RFC3339), w.to.Format(time.RFC3339), interval, err)
			// Coalesced per rule: a broken rule — or a ClickHouse outage — fails
			// the same way every interval. The error carries the query kind and
			// ClickHouse's code (db.QueryError), never the statement.
			telemetry.ErrorCoalesced(ctx, "alert.evaluate.failed:"+rule.ID, "alert.evaluate.failed", err, map[string]any{
				"rule_id":     rule.ID,
				"project":     rule.Project,
				"rule_type":   rule.Type,
				"window_from": w.from.Format(time.RFC3339),
				"window_to":   w.to.Format(time.RFC3339),
				"retry_in_s":  int(interval / time.Second),
				"reason":      "rule_query_failed",
				"outcome":     "the rule's schedule is held at this window and retried; its state is unchanged",
			})
			break
		}
		e.applyWindow(rule, state, value, isFiring, w.to)
		if state.Status == "firing" {
			fired, firedValue, firedThisTick = w, value, true
		}
		final = w
		e.lastEnd[rule.ID] = w.to
		evaluated++
	}
	if evaluated == 0 {
		return nil
	}

	// A catch-up that fired and then cleared, on a rule saved as ok. Its net
	// change is ok → ok, which would publish nothing — and a single-event P0
	// caught only inside a catch-up would vanish without a notification, a
	// history row or a log line. It is published as ok → firing instead, with
	// the value that fired, and saved as firing; the next tick's window resolves
	// it, so the rule still never fires and resolves within one tick.
	if !wasFiring && state.Status != "firing" && firedThisTick {
		e.logf("alert evaluator: rule %s (%s) fired over [%s, %s) (value %.2f) and had cleared by [%s, %s) within one catch-up; notifying it as firing, the next window resolves it",
			rule.ID, rule.Name, fired.from.Format(time.RFC3339), fired.to.Format(time.RFC3339), firedValue,
			final.from.Format(time.RFC3339), final.to.Format(time.RFC3339))
		state.Status = "firing"
		state.Value = firedValue
	}

	// One notification per tick at most, for the transition the tick leaves the
	// rule in relative to what was persisted before it. In the usual one-window
	// tick that is simply that window's transition.
	isFiring := state.Status == "firing"
	switch {
	case isFiring && !wasFiring:
		state.FiredAt = &now
		e.onFiring(ctx, rule, state, now)

	case !isFiring && wasFiring:
		state.ResolvedAt = &now
		e.onResolved(ctx, rule, state, now)

	case isFiring && wasFiring:
		// Check cooldown for re-notification
		if state.LastNotifiedAt != nil {
			cooldown := time.Duration(rule.CooldownSeconds) * time.Second
			if now.Sub(*state.LastNotifiedAt) >= cooldown {
				e.onFiring(ctx, rule, state, now)
			}
		}
	}

	state.UpdatedAt = now
	return state
}

// applyWindow folds one window's result into the rule's status. at is the
// window's end, which is the clock the for_seconds hold is measured on.
func (e *Evaluator) applyWindow(rule *structs.AlertRule, state *State, value float64, isFiring bool, at time.Time) {
	state.Value = value

	if !isFiring {
		state.Status = "ok"
		delete(e.pendingSince, rule.ID)
		return
	}
	if state.Status == "firing" {
		return
	}

	if hold := time.Duration(rule.ForSeconds) * time.Second; hold > 0 {
		since, pending := e.pendingSince[rule.ID]
		if !pending {
			// ok → pending: the ONLY place the hold starts. Overwriting it on
			// every firing window is the bug that kept for_seconds rules from
			// ever firing.
			since = at
			e.pendingSince[rule.ID] = at
		}
		if at.Sub(since) < hold {
			return // pending, persisted as ok until the hold is met
		}
	}

	state.Status = "firing"
	delete(e.pendingSince, rule.ID)
}

func (e *Evaluator) onFiring(ctx context.Context, rule *structs.AlertRule, state *State, now time.Time) {
	state.LastNotifiedAt = &now
	msg := fmt.Sprintf("Alert '%s' is firing: value %.2f %s threshold %.2f", rule.Name, state.Value, rule.Condition, rule.Threshold)
	e.publish(ctx, rule, state, "firing", msg)
}

func (e *Evaluator) onResolved(ctx context.Context, rule *structs.AlertRule, state *State, now time.Time) {
	state.LastNotifiedAt = &now
	msg := fmt.Sprintf("Alert '%s' has resolved: value %.2f", rule.Name, state.Value)
	e.publish(ctx, rule, state, "resolved", msg)
}

// publishTransition is the production publish: history, the alert SSE hub, and
// the notification router.
func (e *Evaluator) publishTransition(ctx context.Context, rule *structs.AlertRule, state *State, status, msg string) {
	// The alert itself. Lean by policy: what fired, for which rule, at what
	// value — the rule's own definition is in MariaDB, not in every event.
	data := map[string]any{
		"rule_id":   rule.ID,
		"project":   rule.Project,
		"rule_type": rule.Type,
		"value":     state.Value,
		"priority":  rule.Priority,
	}
	if status == "firing" {
		telemetry.Info(ctx, "alert.fired", data)
	} else {
		telemetry.Info(ctx, "alert.resolved", data)
	}

	if err := RecordHistory(ctx, HistoryEntry{
		Project:  rule.Project,
		RuleID:   rule.ID,
		RuleName: rule.Name,
		Status:   status,
		Value:    state.Value,
		Message:  msg,
	}); err != nil {
		// The notification still goes out; only the audit row is missing, which
		// is why this is not a failed transition.
		telemetry.ErrorCoalesced(ctx, "alert.history.write.failed", "alert.history.write.failed", err, map[string]any{
			"rule_id":    rule.ID,
			"project":    rule.Project,
			"status":     status,
			"dependency": "clickhouse",
			"outcome":    "the alert was notified but its history row is missing",
		})
	}

	if e.alertHub != nil {
		e.alertHub.PublishStateChange(rule.Project, rule.ID, rule.Name, status, msg, state.Value)
	}

	alertCtx := BuildAlertContext(ctx, rule, status, state.Value, msg)
	// The destination count is Route's own business to report — it emits the
	// warning when it is zero, where it knows which channels resolved and which
	// did not.
	if _, err := e.router.Route(ctx, alertCtx, rule); err != nil {
		// Coalesced per rule. This is the alert not reaching anyone.
		telemetry.ErrorCoalesced(ctx, "alert.route.failed:"+rule.ID, "alert.route.failed", err, map[string]any{
			"rule_id":      rule.ID,
			"project":      rule.Project,
			"alert_status": status,
			"reason":       "routing_failed",
			"outcome":      "the alert was recorded but not delivered to any channel",
		})
	}
}

// evaluatorTrace is the production logf: the schedule narrative at debug, off
// unless MON_TELEMETRY_DEBUG is set. The failures it accompanies are reported as
// their own structured events at warn or error, so nothing is lost with it off.
func evaluatorTrace(format string, args ...any) {
	telemetry.Debug(context.Background(), "alert.evaluator.trace", map[string]any{
		"message": fmt.Sprintf(format, args...),
	})
}

// evaluateRuleState computes a rule's value over the half-open window
// [from, to) and whether it is firing, branching on rule.Type. The "value"
// recorded is type-dependent (see below). The timer passes the scheduled
// window; EvaluateRuleNow passes [now-interval, now). Both reach ClickHouse
// through aggregate — queryAggForRange in production — and so through the one
// statement builder the parity tests pin.
func evaluateRuleState(ctx context.Context, aggregate aggregateFunc, rule *structs.AlertRule, from, to time.Time) (value float64, isFiring bool, err error) {
	switch rule.Type {
	case "absence":
		// COUNT over the window ignoring metric/field; firing when nothing arrived.
		count, err := queryCountForRange(ctx, aggregate, rule, from, to)
		if err != nil {
			return 0, false, err
		}
		return count, count == 0, nil

	case "rate_change":
		// Percent change of the aggregate between the previous window and this
		// one. The previous window is [from-interval, from): the same length,
		// ending exactly where this one starts, so on the timer it is the window
		// evaluated just before.
		cur, err := queryValueForRange(ctx, aggregate, rule, from, to)
		if err != nil {
			return 0, false, err
		}
		prev, err := queryValueForRange(ctx, aggregate, rule, from.Add(-to.Sub(from)), from)
		if err != nil {
			return 0, false, err
		}
		var pct float64
		if prev == 0 {
			if cur > 0 {
				pct = 100
			} else {
				pct = 0
			}
		} else {
			pct = (cur - prev) / prev * 100
		}
		return pct, CheckCondition(pct, rule.Condition, rule.Threshold), nil

	default:
		// threshold (and empty/unknown): aggregate over the window vs threshold.
		v, err := queryValueForRange(ctx, aggregate, rule, from, to)
		if err != nil {
			return 0, false, err
		}
		return v, CheckCondition(v, rule.Condition, rule.Threshold), nil
	}
}

// parseRuleFilters decodes a rule's query_filters JSON array.
func parseRuleFilters(rule *structs.AlertRule) ([]structs.QueryFilter, error) {
	var filters []structs.QueryFilter
	if rule.QueryFilters != "" && rule.QueryFilters != "[]" {
		if err := json.Unmarshal([]byte(rule.QueryFilters), &filters); err != nil {
			return nil, fmt.Errorf("failed to parse query filters: %w", err)
		}
	}
	return filters, nil
}

// buildAggQuery assembles the statement queryAggForRange runs, and is split out
// of it for the same reason routes.subscriptionFilters is split out of its
// handler: the thing worth asserting on is the text, and it is unreachable
// through a function whose next act is to hit ClickHouse. A test can call it
// once as the timer reaches it and once as the test endpoint does, and compare
// the two statements — which is the regression guard on the closed gap above,
// and which no assertion about a returned float64 could express.
//
// aggExpr is a trusted, code-built expression; the rule's filters are bound.
//
// The window is HALF-OPEN, [from, to), on both paths. The timer's windows are
// contiguous — each starts where the last ended — so an inclusive upper bound
// would count an event stamped exactly on a boundary in two windows.
func buildAggQuery(ctx context.Context, rule *structs.AlertRule, aggExpr string, from, to time.Time) (string, []interface{}, error) {
	filters, err := parseRuleFilters(rule)
	if err != nil {
		return "", nil, err
	}

	sql := fmt.Sprintf("SELECT %s AS value FROM %s.events WHERE timestamp >= ? AND timestamp < ?", aggExpr, db.Database)
	args := []interface{}{from, to}

	// UNCONDITIONAL. There used to be an `errors.Is(err, scope.ErrNoProject)`
	// arm here that swallowed the error and continued zone-wide, because the
	// timer had no project to offer. It has one now (evaluateAll stamps
	// rule.Project), so the arm's only remaining effect would be to make an
	// unscoped aggregate reachable by forgetting to supply a project — which is
	// precisely the failure the sentinel exists to prevent. Propagating it makes
	// the unscoped read UNCONSTRUCTABLE rather than merely unusual.
	//
	// Attached BEFORE the rule's own filters, not after: these are positional
	// `?` placeholders, so the args have to be appended in the order their
	// placeholders appear in the text. Splicing this in below the loop would
	// slide every filter's binding one position along — valid SQL, no error,
	// wrong rows.
	predicate, scopeArgs, err := scope.ProjectPredicate(ctx)
	if err != nil {
		return "", nil, err
	}
	sql += " AND " + predicate
	args = append(args, scopeArgs...)

	for _, f := range filters {
		cond, condArgs, err := buildFilterCondition(f)
		if err != nil {
			return "", nil, err
		}
		sql += " AND " + cond
		args = append(args, condArgs...)
	}

	return sql, args, nil
}

// queryAggForRange runs an arbitrary aggregation expression over [from,to),
// applying the rule's query_filters. It is the production aggregateFunc.
//
// The tenancy predicate is ALWAYS attached — see the closed note at the top of
// this file. A context with no project produces an error here rather than a
// zone-wide read, on both the timer and the HTTP paths.
func queryAggForRange(ctx context.Context, rule *structs.AlertRule, aggExpr string, from, to time.Time) (float64, error) {
	sql, args, err := buildAggQuery(ctx, rule, aggExpr, from, to)
	if err != nil {
		return 0, err
	}

	var value float64
	if err := db.Conn.QueryRow(ctx, sql, args...).Scan(&value); err != nil {
		return 0, fmt.Errorf("query failed: %w", err)
	}
	return value, nil
}

// queryValueForRange runs the rule's configured metric aggregation over [from,to).
func queryValueForRange(ctx context.Context, aggregate aggregateFunc, rule *structs.AlertRule, from, to time.Time) (float64, error) {
	agg := structs.AggregationType(rule.Metric)
	if agg == "" {
		agg = structs.AggCount
	}
	aggExpr, err := buildAggExpr(agg, rule.Field)
	if err != nil {
		return 0, err
	}
	return aggregate(ctx, rule, aggExpr, from, to)
}

// queryCountForRange runs a COUNT over [from,to) regardless of the rule's metric
// (used by absence alerts, which only care whether any matching event arrived).
func queryCountForRange(ctx context.Context, aggregate aggregateFunc, rule *structs.AlertRule, from, to time.Time) (float64, error) {
	return aggregate(ctx, rule, "toFloat64(count())", from, to)
}

func buildAggExpr(agg structs.AggregationType, field string) (string, error) {
	switch agg {
	case structs.AggCount:
		return "toFloat64(count())", nil
	case structs.AggSum:
		col, err := numericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("toFloat64(sum(%s))", col), nil
	case structs.AggAvg:
		col, err := numericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("toFloat64(avg(%s))", col), nil
	case structs.AggMin:
		col, err := numericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("toFloat64(min(%s))", col), nil
	case structs.AggMax:
		col, err := numericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("toFloat64(max(%s))", col), nil
	default:
		return "toFloat64(count())", nil
	}
}

func numericFieldExpr(field string) (string, error) {
	if field == "" {
		return "", fmt.Errorf("field is required for this aggregation")
	}
	if len(field) > 5 && field[:5] == "data." {
		key := field[5:]
		if !structs.SafeIdentifierRegex.MatchString(key) {
			return "", fmt.Errorf("invalid data field name: %s", key)
		}
		return guardedDataNumber(key), nil
	}
	return "", fmt.Errorf("numeric aggregation only supported on data.* fields")
}

// guardedDataString and guardedDataNumber are this package's copies of
// services.dataStringExpr / services.dataNumberExpr: the JSON extract behind a
// `position(data, '"key"') > 0` test that skips the parse on rows whose text
// cannot hold the key. Results are unchanged — data is always json.Marshal
// output, which writes a SafeIdentifierRegex key verbatim — and the numeric form
// guards only the inner extract, so a missing key is still NULL (never 0) and
// still stays out of sum/avg/min/max and every numeric comparison. The key must
// already have passed structs.SafeIdentifierRegex. evaluator_test.go pins the
// same text services' tests pin, so the two copies cannot drift apart silently.
func guardedDataString(key string) string {
	return fmt.Sprintf("if(position(data, '\"%s\"') > 0, JSONExtractString(data, '%s'), '')", key, key)
}

func guardedDataNumber(key string) string {
	return fmt.Sprintf("toFloat64OrNull(if(position(data, '\"%s\"') > 0, JSONExtractRaw(data, '%s'), ''))", key, key)
}

func buildFilterCondition(f structs.QueryFilter) (string, []interface{}, error) {
	var fieldExpr string

	if len(f.Field) > 5 && f.Field[:5] == "data." {
		key := f.Field[5:]
		if !structs.SafeIdentifierRegex.MatchString(key) {
			return "", nil, fmt.Errorf("invalid data field name: %s", key)
		}
		switch f.Operator {
		case "lt", "gt", "lte", "gte":
			fieldExpr = guardedDataNumber(key)
		default:
			fieldExpr = guardedDataString(key)
		}
	} else if structs.FilterColumns[f.Field] {
		fieldExpr = f.Field
	} else {
		return "", nil, fmt.Errorf("invalid filter field: %s", f.Field)
	}

	switch f.Operator {
	case "eq", "":
		return fmt.Sprintf("%s = ?", fieldExpr), []interface{}{f.Value}, nil
	case "neq":
		return fmt.Sprintf("%s != ?", fieldExpr), []interface{}{f.Value}, nil
	case "lt":
		return fmt.Sprintf("%s < ?", fieldExpr), []interface{}{f.Value}, nil
	case "gt":
		return fmt.Sprintf("%s > ?", fieldExpr), []interface{}{f.Value}, nil
	case "lte":
		return fmt.Sprintf("%s <= ?", fieldExpr), []interface{}{f.Value}, nil
	case "gte":
		return fmt.Sprintf("%s >= ?", fieldExpr), []interface{}{f.Value}, nil
	case "contains":
		return fmt.Sprintf("%s LIKE ?", fieldExpr), []interface{}{fmt.Sprintf("%%%v%%", f.Value)}, nil
	default:
		return fmt.Sprintf("%s = ?", fieldExpr), []interface{}{f.Value}, nil
	}
}

// CheckCondition evaluates whether a value meets the alert condition against the threshold
func CheckCondition(value float64, condition string, threshold float64) bool {
	switch condition {
	case "gt":
		return value > threshold
	case "lt":
		return value < threshold
	case "gte":
		return value >= threshold
	case "lte":
		return value <= threshold
	case "eq":
		return value == threshold
	default:
		return false
	}
}

// listEnabledRules reads the rules the evaluator should run.
//
// It is a MariaDB read now (migration 119), so it no longer takes a context: the
// query layer's signature is db.Queryable-first and carries none, matching every
// other relational read in the repo. The wrapper is kept rather than inlined at
// the one call site because it is the seam that decides whether the evaluator
// sees every project's rules. It reads ALL of them, across every project, and
// evaluateAll scopes each one individually before it is evaluated — see the
// closed note above and query.ListEnabledAlertRules for why the LISTING itself
// must stay unscoped while the EVALUATION must not.
//
// The `FINAL` that used to be on this statement is gone with the engine that
// needed it. It was there because a ReplacingMergeTree can hold several versions
// of one rule until a background merge collapses them, so a read without it
// could return a rule as enabled after it had been disabled — a disabled rule
// that kept firing until ClickHouse got around to merging. There is one row per
// rule now.
func listEnabledRules() ([]structs.AlertRule, error) {
	return query.ListEnabledAlertRules(db.SQL)
}
