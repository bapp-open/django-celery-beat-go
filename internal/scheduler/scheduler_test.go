package scheduler

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bapp-open/django-celery-beat-go/internal/store"
	"github.com/bapp-open/django-celery-beat-go/internal/store/memstore"
)

func TestIntervalDispatchesOnSchedule(t *testing.T) {
	w := newWorld(t, "UTC")
	w.db.Put(intervalTask(1, "every-10s", 10, "seconds"))
	if err := w.runUntil(t0.Add(65 * time.Second)); err != nil {
		t.Fatal(err)
	}
	// first run at date_changed + 10s (ModelEntry: null last_run_at falls
	// back to date_changed), then every 10 s: 10, 20, ..., 60
	if got := w.broker.n("every-10s"); got != 6 {
		t.Fatalf("dispatched %d times, want 6", got)
	}
	row, _ := w.db.Get(1)
	if row.TotalRunCount != 6 || row.LastRunAt == nil || !row.LastRunAt.Equal(t0.Add(60*time.Second)) {
		t.Fatalf("row not updated like Python beat: count=%d last=%v", row.TotalRunCount, row.LastRunAt)
	}
	j := w.db.Journal()
	for i, r := range j {
		want := t0.Add(time.Duration(i+1) * 10 * time.Second)
		if !r.ScheduledFor.Equal(want) || r.PublishedAt == nil {
			t.Fatalf("journal row %d: scheduled %v published %v, want scheduled %v", i, r.ScheduledFor, r.PublishedAt, want)
		}
		if r.TaskID != TaskID(1, want) {
			t.Fatalf("task id is not deterministic: %s", r.TaskID)
		}
	}
	assertNoDuplicates(t, w.broker.ids())
	if w.reg.Get("gobeat_dispatch_total", "result", "published") != 6 {
		t.Fatal("dispatch metric not counted")
	}
}

func TestTaskIDIsStableAndDistinct(t *testing.T) {
	a := TaskID(7, t0)
	if a != TaskID(7, t0) || a == TaskID(8, t0) || a == TaskID(7, t0.Add(time.Microsecond)) {
		t.Fatal("task id must depend on exactly (periodic task id, scheduled time)")
	}
	if len(a) != 36 || a[14] != '5' {
		t.Fatalf("not a UUIDv5: %s", a)
	}
}

func TestCrontabDispatch(t *testing.T) {
	w := newWorld(t, "UTC")
	w.db.Put(cronTask(1, "hourly", "30", "*", "*", "*", "*", "Europe/Bucharest"))
	if err := w.runUntil(t0.Add(5*time.Hour + time.Minute)); err != nil {
		t.Fatal(err)
	}
	j := w.db.Journal()
	if len(j) != 5 {
		t.Fatalf("got %d runs, want 5", len(j))
	}
	for i, r := range j {
		want := t0.Add(30*time.Minute + time.Duration(i)*time.Hour)
		if !r.ClaimedAt.Equal(want) {
			t.Fatalf("run %d at %v, want %v", i, r.ClaimedAt, want)
		}
	}
}

func TestOneOffAndClocked(t *testing.T) {
	w := newWorld(t, "UTC")
	one := intervalTask(1, "one-off", 10, "seconds")
	one.OneOff = true
	at := t0.Add(42 * time.Second)
	clocked := store.Task{ID: 2, Name: "clocked", Task: "app.task", ClockedTime: &at, OneOff: true,
		Args: "[]", Kwargs: "{}", Enabled: true, DateChanged: t0}
	w.db.Put(one)
	w.db.Put(clocked)
	if err := w.runUntil(t0.Add(20 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if w.broker.n("one-off") != 1 || w.broker.n("clocked") != 1 {
		t.Fatalf("one-off ran %d times, clocked %d times; want exactly once each", w.broker.n("one-off"), w.broker.n("clocked"))
	}
	for _, id := range []int64{1, 2} {
		row, _ := w.db.Get(id)
		if row.Enabled || row.TotalRunCount != 0 || row.LastRunAt != nil {
			t.Fatalf("task %d not disabled like ModelEntry.is_due does: %+v", id, row)
		}
	}
	for _, r := range w.db.Journal() {
		if r.Name == "clocked" && !r.ClaimedAt.Equal(at) {
			t.Fatalf("clocked task ran at %v, want %v", r.ClaimedAt, at)
		}
	}
}

func TestStartTimeAndExpires(t *testing.T) {
	w := newWorld(t, "UTC")
	a := intervalTask(1, "starts-later", 10, "seconds")
	a.StartTime = ptr(t0.Add(95 * time.Second))
	b := intervalTask(2, "expires", 10, "seconds")
	b.Expires = ptr(t0.Add(35 * time.Second))
	w.db.Put(a)
	w.db.Put(b)
	if err := w.runUntil(t0.Add(130 * time.Second)); err != nil {
		t.Fatal(err)
	}
	j := w.db.Journal()
	var first time.Time
	for _, r := range j {
		if r.Name == "starts-later" && first.IsZero() {
			first = r.ClaimedAt
		}
	}
	if !first.Equal(t0.Add(95 * time.Second)) {
		t.Fatalf("first run at %v, want exactly start_time", first)
	}
	if w.broker.n("expires") != 3 {
		t.Fatalf("expiring task ran %d times, want 3 (10s, 20s, 30s)", w.broker.n("expires"))
	}
	if row, _ := w.db.Get(2); row.Enabled {
		t.Fatal("expired task was not disabled")
	}
}

func TestAdminEditsTakeEffect(t *testing.T) {
	w := newWorld(t, "UTC")
	w.db.Put(intervalTask(1, "edited", 10, "seconds"))
	step := 0
	err := w.runWith(w.opts, t0.Add(200*time.Second), func(now time.Time) {
		switch {
		case step == 0 && !now.Before(t0.Add(35*time.Second)):
			step++
			row, _ := w.db.Get(1)
			row.Args = `["v2"]`
			w.db.Put(row)
		case step == 1 && !now.Before(t0.Add(65*time.Second)):
			step++
			row, _ := w.db.Get(1)
			row.Enabled, row.LastRunAt = false, nil // PeriodicTask.save()
			w.db.Put(row)
		case step == 2 && !now.Before(t0.Add(125*time.Second)):
			step++
			row, _ := w.db.Get(1)
			row.Enabled, row.DateChanged = true, now
			w.db.Put(row)
		case step == 3 && !now.Before(t0.Add(165*time.Second)):
			step++
			w.db.Delete(1)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	var v1, v2 int
	for _, m := range w.broker.msgs {
		if strings.Contains(string(m.Body), "v2") {
			v2++
		} else {
			v1++
		}
	}
	// 10,20,30 with the old args; 40,50,60 new; disabled 65..125; then
	// 135..165 (interval counted from the re-enable), deleted at 165.
	if v1 != 3 || v2 != 3+4 {
		t.Fatalf("v1=%d v2=%d, want 3 and 7", v1, v2)
	}
	for _, r := range w.db.Journal() {
		if r.ClaimedAt.After(t0.Add(65*time.Second)) && r.ClaimedAt.Before(t0.Add(130*time.Second)) {
			t.Fatalf("ran at %v while disabled", r.ClaimedAt)
		}
	}
}

func TestUnsupportedTasksAreNamedAndSkipped(t *testing.T) {
	w := newWorld(t, "UTC")
	solar := store.Task{ID: 1, Name: "sunrise-job", Task: "app.task", Solar: true, Args: "[]", Kwargs: "{}", Enabled: true, DateChanged: t0}
	pickled := intervalTask(2, "pickled-job", 10, "seconds")
	pickled.Task = "app.pickled"
	badArgs := intervalTask(3, "bad-args", 10, "seconds")
	badArgs.Args = `{"not": "a list"`
	badCron := cronTask(4, "bad-cron", "61", "*", "*", "*", "*", "UTC")
	typed := intervalTask(5, "typed-json", 10, "seconds")
	typed.Kwargs = `{"when": {"__type__": "datetime", "__value__": "2024-01-01T00:00:00"}}`
	feb31 := cronTask(6, "feb-31", "0", "0", "*", "31", "2", "UTC")
	for _, x := range []store.Task{solar, pickled, badArgs, badCron, typed, feb31, intervalTask(9, "fine", 10, "seconds")} {
		w.db.Put(x)
	}
	if err := w.runUntil(t0.Add(11 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	out := w.log.String()
	for _, name := range []string{"sunrise-job", "pickled-job", "bad-args", "bad-cron", "typed-json", "feb-31"} {
		if w.broker.n(name) != 0 {
			t.Errorf("%s was dispatched", name)
		}
		if n := strings.Count(out, "name="+name+" "); n != 1 {
			t.Errorf("%s: want exactly one error naming the task across reloads, got %d", name, n)
		}
	}
	if !strings.Contains(out, "solar schedules are not supported") || !strings.Contains(out, `serializer \"pickle\"`) {
		t.Errorf("errors do not explain the reason:\n%s", out)
	}
	if w.broker.n("fine") != 66 {
		t.Errorf("healthy task ran %d times, want 66", w.broker.n("fine"))
	}
	if w.reg.Get("gobeat_tasks_refused") != 6 {
		t.Errorf("refused gauge = %v", w.reg.Get("gobeat_tasks_refused"))
	}
}

func TestRefusesToStartWithoutSettingsRow(t *testing.T) {
	w := newWorld(t, "UTC")
	w.db.SetSettings(nil, "")
	err := w.runUntil(t0.Add(time.Minute))
	var fatal *FatalError
	if !errors.As(err, &fatal) || !errors.Is(err, store.ErrNoSettings) {
		t.Fatalf("want a fatal ErrNoSettings, got %v", err)
	}
	if !strings.Contains(err.Error(), "gobeat_sync") {
		t.Fatalf("error does not tell the operator what to do: %v", err)
	}
}

func TestRefusesUnsupportedVersionsAndConfigurations(t *testing.T) {
	for name, mutate := range map[string]func(string) string{
		"celery 4":     func(s string) string { return strings.Replace(s, `"celery": "5.7.0"`, `"celery": "4.4.7"`, 1) },
		"dcb 2.5":      func(s string) string { return strings.Replace(s, `"2.9.0"`, `"2.5.0"`, 1) },
		"schema 2":     func(s string) string { return strings.Replace(s, `"schema": 1`, `"schema": 2`, 1) },
		"protocol 1":   func(s string) string { return strings.Replace(s, `"task_protocol": 2`, `"task_protocol": 1`, 1) },
		"naive/aware":  func(s string) string { return strings.Replace(s, `"tz_aware": true`, `"tz_aware": false`, 1) },
		"bad timezone": func(s string) string { return strings.Replace(s, `"timezone": "UTC"`, `"timezone": "Mars/Olympus"`, 1) },
		"local time": func(s string) string {
			return strings.Replace(strings.Replace(s, `"timezone": "UTC"`, `"timezone": null`, 1), `"enable_utc": true`, `"enable_utc": false`, 1)
		},
		"always eager": func(s string) string {
			return strings.Replace(s, `"task_protocol": 2`, `"task_protocol": 2, "task_always_eager": true`, 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t, "UTC")
			w.db.SetSettings([]byte(mutate(string(settingsDoc("UTC")))), "v1")
			err := w.runUntil(t0.Add(time.Minute))
			var fatal *FatalError
			if !errors.As(err, &fatal) {
				t.Fatalf("want a fatal error, got %v", err)
			}
		})
	}
}

func TestSettingsReloadOnVersionChange(t *testing.T) {
	w := newWorld(t, "UTC")
	w.db.Put(cronTask(1, "noon", "0", "12", "*", "*", "*", "UTC"))
	// The crontab above has TZ_AWARE zone UTC; an interval task shows the
	// app timezone change through the expires header instead.
	iv := intervalTask(2, "iv", 3600, "seconds")
	iv.ExpireSeconds = ptr(int64(60))
	w.db.Put(iv)
	changed := false
	err := w.runWith(w.opts, t0.Add(150*time.Minute), func(now time.Time) {
		if !changed && !now.Before(t0.Add(90*time.Minute)) {
			changed = true
			w.db.SetSettings(settingsDoc("Asia/Tokyo"), "v2")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(w.log.String(), "settings changed, reloading") {
		t.Fatal("settings change not detected")
	}
	if len(w.broker.msgs) != 2 {
		t.Fatalf("got %d messages", len(w.broker.msgs))
	}
	e1, _ := w.broker.msgs[0].Headers.Get("expires")
	e2, _ := w.broker.msgs[1].Headers.Get("expires")
	if !strings.HasSuffix(string(e1.(interface{ Runes() []rune }).Runes()), "+00:00") ||
		!strings.HasSuffix(string(e2.(interface{ Runes() []rune }).Runes()), "+09:00") {
		t.Fatalf("expires headers %v / %v do not reflect the timezone change", e1, e2)
	}
}

// --- failure injection ---------------------------------------------------------

func TestDatabaseDownAndBack(t *testing.T) {
	w := newWorld(t, "UTC")
	w.db.Put(intervalTask(1, "job", 10, "seconds"))
	state := 0
	err := w.runWith(w.opts, t0.Add(10*time.Minute), func(now time.Time) {
		switch {
		case state == 0 && !now.Before(t0.Add(95*time.Second)):
			state = 1
			w.db.SetDown(true)
		case state == 1 && !now.Before(t0.Add(5*time.Minute)):
			state = 2
			w.db.SetDown(false)
		}
	})
	if err != nil {
		t.Fatalf("a database outage must never be fatal: %v", err)
	}
	if !strings.Contains(w.log.String(), "database session failed, will retry") {
		t.Fatal("outage not logged")
	}
	assertNoDuplicates(t, w.broker.ids())
	var during, after []time.Time
	for _, r := range w.db.Journal() {
		if r.PublishedAt == nil {
			t.Fatalf("claimed run %s was never published", r.TaskID)
		}
		switch {
		case r.ClaimedAt.After(t0.Add(95*time.Second)) && r.ClaimedAt.Before(t0.Add(5*time.Minute)):
			during = append(during, r.ClaimedAt)
		case !r.ClaimedAt.Before(t0.Add(5 * time.Minute)):
			after = append(after, r.ClaimedAt)
		}
	}
	if len(during) != 0 {
		t.Fatalf("claimed %d runs while the database was down", len(during))
	}
	// catch-up-once: a single run for the whole outage, then every 10 s
	if len(after) < 3 || after[1].Sub(after[0]) != 10*time.Second {
		t.Fatalf("after the outage: %v", after)
	}
	// the simulation flips the database back on at a retry boundary, so
	// recovery is observed at most two capped (30 s) backoffs later
	if d := after[0].Sub(t0.Add(5 * time.Minute)); d > 61*time.Second {
		t.Fatalf("took %v to recover", d)
	}
}

func TestBrokerDownAndBackLosesNothing(t *testing.T) {
	w := newWorld(t, "UTC")
	w.db.Put(intervalTask(1, "job", 10, "seconds"))
	state := 0
	err := w.runWith(w.opts, t0.Add(10*time.Minute), func(now time.Time) {
		switch {
		case state == 0 && !now.Before(t0.Add(95*time.Second)):
			state = 1
			w.broker.setDown(true)
		case state == 1 && !now.Before(t0.Add(5*time.Minute)):
			state = 2
			w.broker.setDown(false)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	j := w.db.Journal()
	if len(j) != 60 {
		t.Fatalf("claimed %d runs, want 60: the schedule must keep advancing while the broker is down", len(j))
	}
	for _, r := range j {
		if r.PublishedAt == nil {
			t.Fatalf("run %s scheduled for %v was lost", r.TaskID, r.ScheduledFor)
		}
	}
	ids := w.broker.ids()
	assertNoDuplicates(t, ids)
	if len(ids) != 60 {
		t.Fatalf("published %d messages, want 60", len(ids))
	}
	// order is preserved for the backlog
	for i, r := range j {
		if ids[i] != r.TaskID {
			t.Fatalf("message %d out of order", i)
		}
	}
	if !strings.Contains(w.log.String(), "publish failed") {
		t.Fatal("failed publishes must be logged")
	}
}

func TestClaimedRunIsAbandonedLoudlyAfterMaxAge(t *testing.T) {
	w := newWorld(t, "UTC")
	w.opts.PublishMaxAge = 2 * time.Minute
	one := intervalTask(1, "job", 10, "seconds")
	one.OneOff = true
	w.db.Put(one)
	w.broker.setDown(true)
	if err := w.runUntil(t0.Add(10 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	j := w.db.Journal()
	if len(j) != 1 || j[0].AbandonedAt == nil || j[0].PublishedAt != nil {
		t.Fatalf("journal: %+v", j)
	}
	if !strings.Contains(w.log.String(), "ABANDONING claimed run") || w.reg.Get("gobeat_abandoned_total") != 1 {
		t.Fatal("abandoning a run must be logged and counted")
	}
}

func TestCrashBetweenClaimAndPublishIsRecovered(t *testing.T) {
	w := newWorld(t, "UTC")
	w.db.Put(intervalTask(1, "job", 10, "seconds"))
	// SIGKILL right after the first claim committed: the process dies
	// before it can publish.
	ctx, cancel := context.WithCancel(context.Background())
	o := w.opts
	killed := false
	o.Fault = func(point string) {
		if point == "after_claim" && !killed {
			killed = true
			cancel()
			panic("SIGKILL")
		}
	}
	func() {
		defer func() { _ = recover() }()
		_ = New(o).Run(ctx)
	}()
	cancel()
	if j := w.db.Journal(); len(j) != 1 || j[0].PublishedAt != nil || len(w.broker.ids()) != 0 {
		t.Fatalf("setup: want one claimed, unpublished run; journal=%+v", j)
	}
	lost := w.db.Journal()[0].TaskID

	// the next start publishes the orphan first
	w.db = reopen(w.db)
	w.opts.Opener = w.db
	if err := w.runUntil(t0.Add(60 * time.Second)); err != nil {
		t.Fatal(err)
	}
	ids := w.broker.ids()
	if len(ids) == 0 || ids[0] != lost {
		t.Fatalf("orphaned run %s was not published first: %v", lost, ids)
	}
	assertNoDuplicates(t, ids)
	if !strings.Contains(w.log.String(), "never published") {
		t.Fatal("recovery must be logged")
	}
}

// reopen simulates a process restart: sessions of the dead process are gone.
func reopen(db *memstore.DB) *memstore.DB {
	db.SetDown(true) // drops the dead session's lock
	db.SetDown(false)
	return db
}

func TestCrashAfterPublishRedeliversSameTaskID(t *testing.T) {
	w := newWorld(t, "UTC")
	w.db.Put(intervalTask(1, "job", 10, "seconds"))
	ctx, cancel := context.WithCancel(context.Background())
	o := w.opts
	o.Fault = func(point string) {
		if point == "after_publish" {
			cancel()
			panic("SIGKILL")
		}
	}
	func() {
		defer func() { _ = recover() }()
		_ = New(o).Run(ctx)
	}()
	cancel()
	first := w.broker.ids()
	if len(first) != 1 {
		t.Fatalf("setup: %v", first)
	}
	reopen(w.db)
	if err := w.runUntil(t0.Add(35 * time.Second)); err != nil {
		t.Fatal(err)
	}
	ids := w.broker.ids()
	// documented at-least-once edge: the same id, twice - never a new id
	if ids[1] != first[0] {
		t.Fatalf("redelivery must reuse the task id: %v", ids)
	}
	assertNoDuplicates(t, ids[1:])
}

func TestTwoInstancesOneLeaderNoDuplicates(t *testing.T) {
	w := newWorld(t, "UTC")
	w.db.RealTime = true
	w.db.Put(intervalTask(1, "fast", 20000, "microseconds"))
	// both instances run on the real clock against the same database
	run := func(ctx context.Context, done chan<- struct{}) {
		o := w.opts
		o.Clock = realClock{}
		o.StandbyInterval = 10 * time.Millisecond
		o.Pause = nil
		_ = New(o).Run(ctx)
		close(done)
	}
	w.db.Clock.Set(time.Now())
	row := intervalTask(1, "fast", 20000, "microseconds")
	row.DateChanged = time.Now().UTC().Truncate(time.Microsecond)
	w.db.Put(row)

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	doneA, doneB := make(chan struct{}), make(chan struct{})
	go run(ctxA, doneA)
	time.Sleep(50 * time.Millisecond)
	go run(ctxB, doneB)
	time.Sleep(400 * time.Millisecond)
	if !strings.Contains(w.log.String(), "standing by") {
		t.Fatal("second instance did not stand by")
	}
	before := len(w.broker.ids())
	cancelA() // the leader goes away; the standby must take over
	<-doneA
	time.Sleep(400 * time.Millisecond)
	cancelB()
	<-doneB
	ids := w.broker.ids()
	if before < 5 || len(ids) < before+5 {
		t.Fatalf("no failover: %d runs before, %d after", before, len(ids))
	}
	assertNoDuplicates(t, ids)
	if strings.Count(w.log.String(), "acquired leader lock") != 2 {
		t.Fatalf("want exactly two leadership acquisitions")
	}
	j := w.db.Journal()
	for i := 1; i < len(j); i++ {
		if d := j[i].ScheduledFor.Sub(j[i-1].ScheduledFor); d < 15*time.Millisecond {
			t.Fatalf("two dispatches for one slot: %v then %v", j[i-1].ScheduledFor, j[i].ScheduledFor)
		}
	}
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

func TestLostClaimRaceNeverPublishes(t *testing.T) {
	// Another writer (a second replica during a split brain, or an admin
	// save) changes last_run_at between our read and our claim.
	w := newWorld(t, "UTC")
	w.db.Put(intervalTask(1, "job", 10, "seconds"))
	var raced atomic.Bool
	w.db.Hook = func(op string) error {
		if op == "claim" && raced.CompareAndSwap(false, true) {
			go func() {}() // no-op; mutation must happen without the DB lock
		}
		return nil
	}
	stolen := false
	err := w.runWith(w.opts, t0.Add(45*time.Second), func(now time.Time) {
		if !stolen && now.Equal(t0.Add(5*time.Second)) {
			stolen = true
			// someone else already ran the 10s slot at 9.9s
			row, _ := w.db.Get(1)
			other := t0.Add(9900 * time.Millisecond)
			row.LastRunAt = &other
			w.db.PutQuiet([]store.Task{row}) // no marker change: we cannot know
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if w.reg.Get("gobeat_claim_conflicts_total") != 1 {
		t.Fatalf("conflicts = %v, want 1", w.reg.Get("gobeat_claim_conflicts_total"))
	}
	j := w.db.Journal()
	if len(j) == 0 || j[0].ScheduledFor.Before(t0.Add(19*time.Second)) {
		t.Fatalf("published the slot another writer had already taken: %+v", j)
	}
}

func TestClockJumpForwardCatchesUpOnce(t *testing.T) {
	w := newWorld(t, "UTC")
	w.db.Put(intervalTask(1, "job", 60, "seconds"))
	w.db.Put(cronTask(2, "cron", "*/5", "*", "*", "*", "*", "UTC"))
	jumped := false
	err := w.runWith(w.opts, t0.Add(4*time.Hour), func(now time.Time) {
		if !jumped && !now.Before(t0.Add(10*time.Minute+30*time.Second)) {
			jumped = true
			w.clk.Advance(3 * time.Hour) // NTP step / VM resume
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	assertNoDuplicates(t, w.broker.ids())
	var gapRuns int
	for _, r := range w.db.Journal() {
		if r.Name == "job" && r.ClaimedAt.After(t0.Add(11*time.Minute)) && r.ClaimedAt.Before(t0.Add(3*time.Hour+12*time.Minute)) {
			gapRuns++
		}
	}
	// exactly one catch-up run right after the jump, then the 60 s cadence
	if gapRuns != 2 {
		t.Fatalf("%d runs around the jump, want 2 (one catch-up, one regular)", gapRuns)
	}
}

func TestClockJumpForwardWithSkipPolicy(t *testing.T) {
	w := newWorld(t, "UTC")
	w.opts.MissedRunPolicy = Skip
	w.opts.MissedRunGrace = 30 * time.Second
	w.db.Put(intervalTask(1, "job", 60, "seconds"))
	jumped := false
	var jumpTo time.Time
	err := w.runWith(w.opts, t0.Add(4*time.Hour), func(now time.Time) {
		if !jumped && !now.Before(t0.Add(10*time.Minute+30*time.Second)) {
			jumped = true
			w.clk.Advance(3 * time.Hour)
			jumpTo = w.clk.Now()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range w.db.Journal() {
		if r.ClaimedAt.After(t0.Add(11*time.Minute)) && r.ClaimedAt.Before(jumpTo.Add(59*time.Second)) {
			t.Fatalf("run at %v: the missed run was not skipped", r.ClaimedAt)
		}
	}
	if w.reg.Get("gobeat_skipped_runs_total", "reason", "missed") != 1 {
		t.Fatal("skip not counted")
	}
	if !strings.Contains(w.log.String(), "skipping missed run") {
		t.Fatal("skip not logged")
	}
}

func TestClockJumpBackwardNeverDuplicates(t *testing.T) {
	w := newWorld(t, "UTC")
	w.db.Put(intervalTask(1, "job", 60, "seconds"))
	w.db.Put(cronTask(2, "cron", "*", "*", "*", "*", "*", "UTC"))
	jumped := false
	err := w.runWith(w.opts, t0.Add(40*time.Minute), func(now time.Time) {
		if !jumped && !now.Before(t0.Add(30*time.Minute+30*time.Second)) {
			jumped = true
			w.clk.Advance(-20 * time.Minute)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	assertNoDuplicates(t, w.broker.ids())
	seen := map[string]bool{}
	for _, r := range w.db.Journal() {
		k := r.Name + r.ScheduledFor.String()
		if seen[k] {
			t.Fatalf("slot %s dispatched twice after the clock went back", k)
		}
		seen[k] = true
	}
	// 30 runs before the jump (1:00..30:00); none until the clock passes
	// 31:00 again, then 31:00..40:00
	if n := w.broker.n("job"); n != 30+10 {
		t.Fatalf("interval task ran %d times, want 40", n)
	}
}

func TestDryRunWritesNothing(t *testing.T) {
	w := newWorld(t, "UTC")
	w.opts.DryRun = true
	w.db.Put(intervalTask(1, "job", 10, "seconds"))
	one := intervalTask(2, "one-off", 10, "seconds")
	one.OneOff = true
	w.db.Put(one)
	w.db.Hook = func(op string) error {
		switch op {
		case "claim", "skip", "mark_published", "mark_attempt", "abandon", "disable_expired", "disable_one_off", "prune", "lock":
			t.Errorf("dry run performed %q", op)
		}
		return nil
	}
	if err := w.runUntil(t0.Add(11 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if len(w.broker.ids()) != 0 || len(w.db.Journal()) != 0 {
		t.Fatal("dry run published or journaled")
	}
	out := w.log.String()
	if n := strings.Count(out, "msg=dispatch name=job "); n != 66 {
		t.Fatalf("logged %d would-be dispatches, want 66", n)
	}
	if n := strings.Count(out, "msg=dispatch name=one-off "); n != 1 {
		t.Fatalf("one-off logged %d times across reloads, want 1", n)
	}
	if row, _ := w.db.Get(1); row.LastRunAt != nil || row.TotalRunCount != 0 {
		t.Fatal("dry run modified the row")
	}
}

func TestIntervalsAreUnaffectedByDST(t *testing.T) {
	// App timezone with DST, autumn change (2024-10-27 01:00 UTC,
	// 04:00 EEST -> 03:00 EET). Since Celery 5.7 intervals are added in
	// elapsed time (celery.utils.time.add_seconds_to_datetime), so
	// nothing pauses or doubles in the repeated hour, and no entry can
	// hold up another one.
	w := newWorld(t, "Europe/Bucharest")
	start := time.Date(2024, 10, 26, 22, 30, 0, 0, time.UTC) // 01:30 EEST
	w.clk.Set(start)
	hourly := intervalTask(1, "hourly", 3600, "seconds")
	hourly.DateChanged = start
	fast := intervalTask(2, "fast", 10, "seconds")
	fast.DateChanged = start
	minutely := cronTask(3, "minutely", "*", "*", "*", "*", "*", "UTC")
	minutely.DateChanged = start
	// A crontab *in the DST zone* is wall-clock by definition: after
	// 03:59 EEST the next matching wall time is 04:00, which only exists
	// as EET - Celery (and so gobeat) skips the repeated hour.
	local := cronTask(4, "local-minutely", "*", "*", "*", "*", "*", "Europe/Bucharest")
	local.DateChanged = start
	w.db.Put(local)
	w.db.Put(hourly)
	w.db.Put(fast)
	w.db.Put(minutely)
	if err := w.runUntil(start.Add(5 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	prev := map[string]time.Time{}
	step := map[string]time.Duration{"hourly": time.Hour, "fast": 10 * time.Second, "minutely": time.Minute}
	for _, r := range w.db.Journal() {
		if r.Name == "local-minutely" {
			continue
		}
		if p, ok := prev[r.Name]; ok && r.ClaimedAt.Sub(p) != step[r.Name] {
			t.Fatalf("%s: %v -> %v, want a steady %v", r.Name, p, r.ClaimedAt, step[r.Name])
		}
		prev[r.Name] = r.ClaimedAt
	}
	if n := w.broker.n("local-minutely"); n != 240 {
		t.Fatalf("crontab in the DST zone ran %d times, want 240 (repeated hour skipped, as Celery does)", n)
	}
	if w.broker.n("hourly") != 5 || w.broker.n("fast") != 1800 || w.broker.n("minutely") != 300 {
		t.Fatalf("hourly %d, fast %d, minutely %d; want 5, 1800, 300", w.broker.n("hourly"), w.broker.n("fast"), w.broker.n("minutely"))
	}
}
