// Package scheduler is gobeat's tick loop: the equivalent of
// celery/beat.py: Scheduler.tick driven by
// django_celery_beat/schedulers.py: DatabaseScheduler, with the state kept
// in the database and every run claimed before it is published (see
// docs/DESIGN.md for the deliberate differences).
package scheduler

import (
	"container/heap"
	"context"
	crand "crypto/rand"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/bapp-open/django-celery-beat-go/internal/broker"
	"github.com/bapp-open/django-celery-beat-go/internal/clock"
	"github.com/bapp-open/django-celery-beat-go/internal/config"
	"github.com/bapp-open/django-celery-beat-go/internal/message"
	"github.com/bapp-open/django-celery-beat-go/internal/metrics"
	"github.com/bapp-open/django-celery-beat-go/internal/schedule"
	"github.com/bapp-open/django-celery-beat-go/internal/store"
)

// SyncMaxInterval is django_celery_beat/schedulers.py:
// SCHEDULE_SYNC_MAX_INTERVAL: the schedule is re-read at least this often.
const SyncMaxInterval = 300 * time.Second

// Missed-run policies.
const (
	CatchUpOnce = "catch_up_once"
	Skip        = "skip"
)

// Options configure a Scheduler.
type Options struct {
	Clock   clock.Clock
	Opener  store.Opener
	Log     *slog.Logger
	Metrics *metrics.Registry

	// NewPublisher builds the broker publisher for a settings document.
	NewPublisher func(*config.Settings) (broker.Publisher, error)
	// Override applies environment overrides to a freshly loaded
	// settings document.
	Override func(*config.Settings) error

	DryRun          bool
	MissedRunPolicy string        // CatchUpOnce (default) or Skip
	MissedRunGrace  time.Duration // lateness beyond which a run counts as missed
	PublishTimeout  time.Duration
	PublishMaxAge   time.Duration // give up on an unpublished claim after this
	Retention       time.Duration // keep published journal rows this long
	StandbyInterval time.Duration // how often a standby retries the lock

	Origin  string // `origin` message header
	ReplyTo string // reply_to message property

	AllowUnsupportedVersions bool

	// Pause waits between failed sessions (database down). The default
	// sleeps on the real clock; simulations advance their fake clock.
	Pause func(ctx context.Context, d time.Duration)
	// Fault is a test hook called at named points of a dispatch
	// ("after_claim", "after_publish"); nil in production.
	Fault func(point string)
	// Beat is called on every loop iteration (health).
	Beat func(state string)
}

// Scheduler runs the beat loop.
type Scheduler struct {
	o Options

	started bool // a session got past validation at least once
	pub     broker.Publisher
	pubVer  string

	// dry-run memory across reloads (Python keeps state in memory too)
	dryLast map[int64]dryState
}

type dryState struct {
	lastRunAt time.Time
	count     int64
	done      bool // one_off already "dispatched"
}

// New returns a scheduler.
func New(o Options) *Scheduler {
	if o.MissedRunPolicy == "" {
		o.MissedRunPolicy = CatchUpOnce
	}
	if o.MissedRunGrace <= 0 {
		o.MissedRunGrace = 60 * time.Second
	}
	if o.PublishTimeout <= 0 {
		o.PublishTimeout = 10 * time.Second
	}
	if o.PublishMaxAge <= 0 {
		o.PublishMaxAge = time.Hour
	}
	if o.Retention <= 0 {
		o.Retention = 24 * time.Hour
	}
	if o.StandbyInterval <= 0 {
		o.StandbyInterval = 5 * time.Second
	}
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	if o.Pause == nil {
		o.Pause = func(ctx context.Context, d time.Duration) {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
			case <-t.C:
			}
		}
	}
	if o.Fault == nil {
		o.Fault = func(string) {}
	}
	if o.Beat == nil {
		o.Beat = func(string) {}
	}
	return &Scheduler{o: o, dryLast: map[int64]dryState{}}
}

// FatalError is a condition gobeat refuses to run with (missing settings
// row, unknown schema, unsupported versions/configuration).
type FatalError struct{ Err error }

func (e *FatalError) Error() string { return e.Err.Error() }
func (e *FatalError) Unwrap() error { return e.Err }

var errSettingsChanged = errors.New("settings changed")

// Run blocks until ctx is cancelled (returns nil) or a fatal condition is
// met before the scheduler ever started (returns *FatalError). Once it has
// started, every failure - database or broker down, settings row removed,
// schema changed - is retried with backoff: gobeat never crash-loops.
func (s *Scheduler) Run(ctx context.Context) error {
	defer func() {
		if s.pub != nil {
			_ = s.pub.Close()
			s.pub = nil
		}
	}()
	failures := 0
	for {
		err := s.session(ctx)
		if ctx.Err() != nil {
			return nil
		}
		var fatal *FatalError
		switch {
		case errors.Is(err, errSettingsChanged):
			s.o.Log.Info("settings changed, reloading")
			s.o.Metrics.Add("gobeat_reloads_total", 1, "reason", "settings")
			failures = 0
			continue
		case errors.As(err, &fatal):
			if !s.started {
				return fatal
			}
			s.o.Log.Error("configuration problem, will retry", "error", err.Error())
			s.o.Metrics.Add("gobeat_errors_total", 1, "kind", "settings")
		default:
			s.o.Log.Error("database session failed, will retry", "error", fmt.Sprint(err))
			s.o.Metrics.Add("gobeat_errors_total", 1, "kind", "db")
		}
		s.o.Metrics.Set("gobeat_leader", 0)
		failures++
		s.o.Beat("retrying")
		s.o.Pause(ctx, backoff(failures, 500*time.Millisecond, 30*time.Second))
	}
}

func backoff(n int, base, max time.Duration) time.Duration {
	d := base
	for i := 1; i < n && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}

// entry is one scheduled periodic task (ModelEntry).
type entry struct {
	row       store.Task
	spec      schedule.Spec
	in        message.Input
	nextCheck time.Time
	idx       int
}

type entryHeap []*entry

func (h entryHeap) Len() int { return len(h) }
func (h entryHeap) Less(i, j int) bool {
	if !h[i].nextCheck.Equal(h[j].nextCheck) {
		return h[i].nextCheck.Before(h[j].nextCheck)
	}
	return h[i].row.ID < h[j].row.ID
}
func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].idx = i; h[j].idx = j }
func (h *entryHeap) Push(x any)   { e := x.(*entry); e.idx = len(*h); *h = append(*h, e) }
func (h *entryHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return e
}

type pendingItem struct {
	journalID int64
	name      string
	taskID    string
	claimedAt time.Time
	attempts  int
	msg       *message.Message
}

// run is the state of one leader session.
type run struct {
	s        *Scheduler
	st       store.Store
	settings *config.Settings
	version  string
	env      *schedule.Env
	builder  *message.Builder
	maxLoop  time.Duration

	heap       entryHeap
	refused    map[int64]string // task id -> error already logged
	pending    []*pendingItem
	brokerDown time.Time // do not try the broker before this instant
	brokerFail int

	lastMarker *time.Time
	lastFull   time.Time
	lastPoll   time.Time
	lastPrune  time.Time
	needReload bool
}

// supported major.minor versions (verified by tests/differential/matrix.sh)
var supportedVersions = map[string][]string{
	"celery":             {"5.4", "5.5", "5.6", "5.7"},
	"django_celery_beat": {"2.7", "2.8", "2.9"},
}

func checkVersions(v map[string]string) error {
	for pkg, ok := range supportedVersions {
		got := v[pkg]
		parts := strings.SplitN(got, ".", 3)
		mm := got
		if len(parts) >= 2 {
			mm = parts[0] + "." + parts[1]
		}
		found := false
		for _, s := range ok {
			found = found || s == mm
		}
		if !found {
			return fmt.Errorf("%s %s is not a supported version (supported: %s); set GOBEAT_ALLOW_UNSUPPORTED_VERSIONS=1 to run anyway", pkg, got, strings.Join(ok, ", "))
		}
	}
	return nil
}

func (s *Scheduler) session(ctx context.Context) error {
	st, err := s.o.Opener.Open(ctx)
	if err != nil {
		return err
	}
	defer st.Close()

	data, version, err := st.Settings(ctx)
	if errors.Is(err, store.ErrNoSettings) {
		return &FatalError{err}
	}
	if err != nil {
		return err
	}
	settings, err := config.ParseSettings(data)
	if err != nil {
		return &FatalError{err}
	}
	if s.o.Override != nil {
		if err := s.o.Override(settings); err != nil {
			return &FatalError{err}
		}
	}
	if !s.o.AllowUnsupportedVersions {
		if err := checkVersions(settings.Versions); err != nil {
			return &FatalError{err}
		}
	}
	if settings.UseTZ != settings.TZAware {
		return &FatalError{fmt.Errorf("USE_TZ=%v with DJANGO_CELERY_BEAT_TZ_AWARE=%v is not supported: Python beat itself mixes naive and aware datetimes in this combination", settings.UseTZ, settings.TZAware)}
	}
	if settings.TaskProtocol != 2 {
		return &FatalError{fmt.Errorf("task_protocol=%d is not supported (only protocol 2)", settings.TaskProtocol)}
	}
	if settings.TaskAlwaysEager {
		return &FatalError{errors.New("task_always_eager is set: Python beat would execute tasks inline, which gobeat cannot do")}
	}
	if err := st.CheckSchema(ctx, settings.UseTZ); err != nil {
		var se *store.SchemaError
		if errors.As(err, &se) {
			return &FatalError{err}
		}
		return err
	}
	builder, err := message.NewBuilder(settings, s.o.Origin, s.o.ReplyTo)
	if err != nil {
		return &FatalError{err}
	}
	if !s.o.DryRun && (s.pub == nil || s.pubVer != version) {
		if s.pub != nil {
			_ = s.pub.Close()
			s.pub = nil
		}
		pub, err := s.o.NewPublisher(settings)
		if err != nil {
			return &FatalError{err}
		}
		s.pub, s.pubVer = pub, version
	}
	if !s.started {
		for _, w := range settings.Warnings {
			s.o.Log.Warn("gobeat_sync warning", "warning", w)
		}
		if settings.TaskSendSentEvent {
			s.o.Log.Warn("task_send_sent_event is enabled; gobeat does not emit task-sent events")
		}
	}
	s.started = true

	r := &run{
		s: s, st: st, settings: settings, version: version, builder: builder,
		env: &schedule.Env{AppTZ: builder.AppTZ, TZAware: settings.TZAware, EnableUTC: settings.EnableUTC,
			CronDeadline: settings.BeatCronDeadline},
		maxLoop: time.Duration(settings.BeatMaxLoopInterval * float64(time.Second)),
		refused: map[int64]string{},
	}
	if r.maxLoop <= 0 {
		r.maxLoop = 5 * time.Second // DEFAULT_MAX_INTERVAL
	}

	// Leadership: a second replica stands by here.
	if !s.o.DryRun {
		announced := false
		for {
			ok, err := st.TryLock(ctx)
			if err != nil {
				return err
			}
			if ok {
				break
			}
			if !announced {
				s.o.Log.Info("another gobeat instance holds the leader lock; standing by")
				announced = true
			}
			s.o.Metrics.Set("gobeat_leader", 0)
			s.o.Beat("standby")
			if _, err := st.Wait(ctx, s.o.StandbyInterval); err != nil {
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		s.o.Metrics.Set("gobeat_leader", 1)
		s.o.Log.Info("acquired leader lock")
		if err := r.recover(ctx); err != nil {
			return err
		}
	}
	s.o.Log.Info("scheduler started", "dry_run", s.o.DryRun, "timezone", builder.AppTZ.String(),
		"max_loop_interval", r.maxLoop.String(), "settings_version", version,
		"missed_run_policy", s.o.MissedRunPolicy)
	if err := r.reload(ctx, "initial"); err != nil {
		return err
	}
	return r.loop(ctx)
}

// recover loads runs that were claimed but never confirmed by the broker
// (crash or SIGKILL between claim and publish).
func (r *run) recover(ctx context.Context) error {
	rows, err := r.st.Pending(ctx)
	if err != nil {
		return err
	}
	for _, p := range rows {
		m := new(message.Message)
		if err := json.Unmarshal(p.Message, m); err != nil {
			r.s.o.Log.Error("journal row is unreadable; abandoning it", "name", p.Name, "task_id", p.TaskID, "error", err.Error())
			r.s.o.Metrics.Add("gobeat_abandoned_total", 1)
			if err := r.st.Abandon(ctx, p.JournalID, r.s.o.Clock.Now(), "unreadable journal message: "+err.Error()); err != nil {
				return err
			}
			continue
		}
		r.pending = append(r.pending, &pendingItem{journalID: p.JournalID, name: p.Name, taskID: p.TaskID,
			claimedAt: p.ClaimedAt, attempts: p.Attempts, msg: m})
	}
	if len(r.pending) > 0 {
		r.s.o.Log.Warn("found claimed runs that were never published; publishing them now", "count", len(r.pending))
	}
	return nil
}

func (r *run) loop(ctx context.Context) error {
	o := &r.s.o
	notified := false
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		now := o.Clock.Now()
		o.Beat("leader")
		o.Metrics.Set("gobeat_last_tick_timestamp_seconds", float64(now.UnixMicro())/1e6)

		// DatabaseScheduler.schedule: reload when the marker moved or
		// after SCHEDULE_SYNC_MAX_INTERVAL. Python polls the marker on
		// every tick, i.e. at least every max_interval.
		if notified || now.Sub(r.lastPoll) >= r.maxLoop || now.Before(r.lastPoll) {
			st, err := r.st.State(ctx)
			if err != nil {
				return err
			}
			r.lastPoll = now
			if st.SettingsVersion == "" {
				o.Log.Error("the gobeat settings row disappeared; keeping the last known settings")
			} else if st.SettingsVersion != r.version {
				return errSettingsChanged
			}
			if !markerEqual(st.Marker, r.lastMarker) {
				o.Log.Info("schedule changed")
				if err := r.reload(ctx, "changed"); err != nil {
					return err
				}
			}
		}
		if now.Sub(r.lastFull) >= SyncMaxInterval || now.Before(r.lastFull) {
			if err := r.reload(ctx, "periodic"); err != nil {
				return err
			}
		}
		if err := r.flushPending(ctx, now); err != nil {
			return err
		}

		for len(r.heap) > 0 && !r.heap[0].nextCheck.After(now) && !r.needReload {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := r.process(ctx, r.heap[0], now); err != nil {
				return err
			}
			o.Beat("leader")
			now = o.Clock.Now()
		}
		if r.needReload {
			r.needReload = false
			if err := r.reload(ctx, "conflict"); err != nil {
				return err
			}
			continue
		}
		if !o.DryRun && now.Sub(r.lastPrune) >= time.Hour {
			r.lastPrune = now
			if err := r.st.Prune(ctx, now.Add(-o.Retention)); err != nil {
				return err
			}
		}

		wait := r.maxLoop
		if len(r.heap) > 0 {
			if d := r.heap[0].nextCheck.Sub(now); d < wait {
				wait = d
			}
		}
		if len(r.pending) > 0 {
			if d := r.brokerDown.Sub(now); d < wait {
				wait = d
			}
		}
		if d := r.lastFull.Add(SyncMaxInterval).Sub(now); d < wait {
			wait = d
		}
		if wait <= 0 {
			notified = false
			continue
		}
		var err error
		if notified, err = r.st.Wait(ctx, wait); err != nil {
			return err
		}
	}
}

func markerEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

// refuse logs (once per distinct error) that a task is not scheduled.
func (r *run) refuse(t *store.Task, err error) {
	msg := err.Error()
	if r.refused[t.ID] != msg {
		r.s.o.Log.Error("periodic task refused; it will not be scheduled", "name", t.Name, "task", t.Task,
			"periodic_task_id", t.ID, "error", msg)
		r.s.o.Metrics.Add("gobeat_errors_total", 1, "kind", "task")
	}
	r.refused[t.ID] = msg
}

func (r *run) buildEntry(t store.Task) (*entry, error) {
	e := &entry{row: t}
	e.spec = schedule.Spec{Enabled: t.Enabled, OneOff: t.OneOff, StartTime: t.StartTime, Expires: t.Expires,
		LastRunAt: t.LastRunAt, DateChanged: t.DateChanged, TotalRunCount: t.TotalRunCount}
	// PeriodicTask.scheduler: interval, crontab, solar, clocked - in that order.
	switch {
	case t.IntervalEvery != nil:
		period := ""
		if t.IntervalPeriod != nil {
			period = *t.IntervalPeriod
		}
		d, err := schedule.IntervalDuration(*t.IntervalEvery, period)
		if err != nil {
			return nil, err
		}
		e.spec.Kind, e.spec.Every = schedule.KindInterval, d
	case t.Cron != nil:
		tz := r.env.AppTZ
		if r.env.TZAware {
			var err error
			if tz, err = loadLocation(t.Cron.Timezone); err != nil {
				return nil, fmt.Errorf("crontab timezone %q: %w", t.Cron.Timezone, err)
			}
		}
		c, err := schedule.NewCrontab(t.Cron.Minute, t.Cron.Hour, t.Cron.DayOfWeek, t.Cron.DayOfMonth, t.Cron.MonthOfYear, tz)
		if err != nil {
			return nil, fmt.Errorf("invalid crontab %q %q %q %q %q: %w", t.Cron.Minute, t.Cron.Hour,
				t.Cron.DayOfMonth, t.Cron.MonthOfYear, t.Cron.DayOfWeek, err)
		}
		e.spec.Kind, e.spec.Cron = schedule.KindCrontab, c
	case t.Solar:
		return nil, fmt.Errorf("%w: solar schedules are not supported by gobeat", schedule.ErrUnsupported)
	case t.ClockedTime != nil:
		e.spec.Kind, e.spec.ClockedTime = schedule.KindClocked, *t.ClockedTime
	default:
		return nil, fmt.Errorf("%w: the task has no schedule", schedule.ErrUnsupported)
	}
	e.in = message.Input{ID: t.ID, Name: t.Name, Task: t.Task, Args: t.Args, Kwargs: t.Kwargs, Headers: t.Headers,
		Queue: t.Queue, Exchange: t.Exchange, RoutingKey: t.RoutingKey, Priority: t.Priority,
		Expires: t.Expires, ExpireSeconds: t.ExpireSeconds}
	return e, nil
}

func loadLocation(name string) (*time.Location, error) {
	if name == "UTC" {
		return time.UTC, nil
	}
	if name == "" || name == "Local" {
		return nil, errors.New("empty timezone")
	}
	return time.LoadLocation(name)
}

// reload is DatabaseScheduler.all_as_schedule + Scheduler.populate_heap.
func (r *run) reload(ctx context.Context, reason string) error {
	o := &r.s.o
	st, err := r.st.State(ctx)
	if err != nil {
		return err
	}
	tasks, err := r.st.Tasks(ctx)
	if err != nil {
		return err
	}
	now := o.Clock.Now()
	r.lastMarker, r.lastFull, r.lastPoll = st.Marker, now, now
	o.Metrics.Add("gobeat_reloads_total", 1, "reason", reason)

	r.heap = r.heap[:0]
	stillRefused := map[int64]string{}
	for i := range tasks {
		t := tasks[i]
		if o.DryRun {
			// Shadow mode never writes, so remember our own dispatches
			// like Python's in-memory entries do.
			if d, ok := r.s.dryLast[t.ID]; ok {
				if d.done {
					continue
				}
				if t.LastRunAt == nil || d.lastRunAt.After(*t.LastRunAt) {
					last := d.lastRunAt
					t.LastRunAt = &last
				}
				if d.count > t.TotalRunCount {
					t.TotalRunCount = d.count
				}
			}
		}
		e, err := r.buildEntry(t)
		if err == nil {
			// Validate the message now so that an unsupported task is
			// reported when it is loaded, not when it is due.
			_, err = r.builder.Build(e.in, "00000000-0000-0000-0000-000000000000", now)
		}
		var res schedule.Result
		if err == nil {
			res, err = e.spec.IsDue(r.env, now)
		}
		if err != nil {
			r.refuse(&t, err)
			stillRefused[t.ID] = r.refused[t.ID]
			continue
		}
		if res.Action != schedule.ActNone {
			if err := r.act(ctx, e, res.Action, now); err != nil {
				return err
			}
			continue
		}
		// populate_heap: due entries are checked immediately
		if res.Due {
			e.nextCheck = now
		} else {
			e.nextCheck = now.Add(seconds(res.Next))
		}
		r.heap = append(r.heap, e)
	}
	r.refused = stillRefused
	for i, e := range r.heap {
		e.idx = i
	}
	heap.Init(&r.heap)
	o.Metrics.Set("gobeat_tasks", float64(len(r.heap)))
	o.Metrics.Set("gobeat_tasks_refused", float64(len(r.refused)))
	o.Log.Debug("schedule loaded", "reason", reason, "tasks", len(r.heap), "refused", len(r.refused))
	return nil
}

// seconds converts Python's float seconds to a duration, saturating.
func seconds(s float64) time.Duration {
	if s >= schedule.NeverCheck || math.IsNaN(s) {
		return time.Duration(schedule.NeverCheck) * time.Second
	}
	if s <= 0 {
		return 0
	}
	// round up to a microsecond so we never wake before the instant
	return time.Duration(math.Ceil(s*1e6)) * time.Microsecond
}

func (r *run) setNext(e *entry, t time.Time) {
	e.nextCheck = t
	heap.Fix(&r.heap, e.idx)
}

func (r *run) remove(e *entry) { heap.Remove(&r.heap, e.idx) }

// act applies the side effects of ModelEntry.is_due.
func (r *run) act(ctx context.Context, e *entry, a schedule.Action, now time.Time) error {
	o := &r.s.o
	switch a {
	case schedule.ActDisableExpired:
		o.Log.Info("periodic task expired; disabling it", "name", e.row.Name, "periodic_task_id", e.row.ID, "dry_run", o.DryRun)
		if o.DryRun {
			return nil
		}
		return r.st.DisableExpired(ctx, e.row.ID, now)
	case schedule.ActDisableOneOff:
		o.Log.Info("one-off periodic task has run; disabling it", "name", e.row.Name, "periodic_task_id", e.row.ID, "dry_run", o.DryRun)
		if o.DryRun {
			return nil
		}
		return r.st.DisableOneOff(ctx, e.row.ID, now)
	}
	return nil
}

// TaskID derives the deterministic Celery task id for a run: UUIDv5 over
// "<periodic task id>:<scheduled instant in µs since the epoch>".
func TaskID(periodicTaskID int64, scheduled time.Time) string {
	// namespace: UUIDv5(NAMESPACE_DNS, "gobeat.dispatch")
	ns := [16]byte{0x6f, 0x1c, 0x53, 0x0a, 0x2f, 0x0e, 0x5b, 0x9d, 0x9a, 0x44, 0x67, 0x6f, 0x62, 0x65, 0x61, 0x74}
	h := sha1.New()
	h.Write(ns[:])
	fmt.Fprintf(h, "%d:%d", periodicTaskID, scheduled.UnixMicro())
	b := h.Sum(nil)[:16]
	b[6] = b[6]&0x0f | 0x50
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// process is one Scheduler.tick for the entry at the top of the heap.
func (r *run) process(ctx context.Context, e *entry, now time.Time) error {
	o := &r.s.o
	res, err := e.spec.IsDue(r.env, now)
	if err != nil {
		r.refuse(&e.row, err)
		r.remove(e)
		return nil
	}
	if res.Action != schedule.ActNone {
		r.remove(e)
		return r.act(ctx, e, res.Action, now)
	}
	if !res.Due {
		r.setNext(e, now.Add(seconds(res.Next)))
		return nil
	}

	seen := e.spec.LastRunAt
	lateness := now.Sub(res.DueAt)
	next := now.Add(seconds(res.Next))

	// Missed-run policy. Clocked tasks are exempt: they have exactly
	// one run and skipping it would mean never running it.
	if o.MissedRunPolicy == Skip && seen != nil && e.spec.Kind != schedule.KindClocked && lateness > o.MissedRunGrace {
		o.Log.Warn("skipping missed run (missed-run policy: skip)", "name", e.row.Name, "periodic_task_id", e.row.ID,
			"scheduled", res.DueAt.Format(time.RFC3339Nano), "late_seconds", lateness.Seconds(), "dry_run", o.DryRun)
		o.Metrics.Add("gobeat_skipped_runs_total", 1, "reason", "missed")
		if !o.DryRun {
			ok, err := r.st.Skip(ctx, e.row.ID, seen, now)
			if err != nil {
				return err
			}
			if !ok {
				return r.conflict(e)
			}
		}
		r.advance(e, now, false)
		// the schedule is now relative to `now`
		res2, err := e.spec.IsDue(r.env, now)
		if err == nil && !res2.Due {
			next = now.Add(seconds(res2.Next))
		}
		r.setNext(e, next)
		return nil
	}

	taskID := TaskID(e.row.ID, res.DueAt)
	msg, err := r.builder.Build(e.in, taskID, now)
	if err != nil {
		r.refuse(&e.row, err)
		r.remove(e)
		return nil
	}
	logDispatch := func(event string) {
		q := ""
		if msg.Queue != nil {
			q = msg.Queue.Name
		}
		o.Log.Info(event, "name", e.row.Name, "task", e.row.Task, "periodic_task_id", e.row.ID, "task_id", taskID,
			"scheduled", res.DueAt.Format("2006-01-02T15:04:05.000000Z07:00"), "late_seconds", lateness.Seconds(),
			"queue", q, "exchange", msg.Exchange, "routing_key", msg.RoutingKey, "dry_run", o.DryRun)
	}

	if o.DryRun {
		logDispatch("dispatch")
		o.Metrics.Add("gobeat_dispatch_total", 1, "result", "dry_run")
		r.advance(e, now, true)
		d := dryState{lastRunAt: now, count: e.spec.TotalRunCount, done: e.spec.OneOff}
		r.s.dryLast[e.row.ID] = d
		if e.spec.OneOff {
			r.remove(e)
			return nil
		}
		r.setNext(e, next)
		return nil
	}

	js, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encode journal message: %w", err)
	}
	// Claim before publish. These calls deliberately ignore cancellation:
	// a claim that has started is seen through to the journal.
	opCtx := context.WithoutCancel(ctx)
	ok, journalID, err := r.st.Claim(opCtx, store.Claim{PeriodicTaskID: e.row.ID, SeenLastRunAt: seen, Now: now,
		ScheduledFor: res.DueAt, TaskID: taskID, Name: e.row.Name, Task: e.row.Task, Message: js})
	if errors.Is(err, store.ErrDuplicateRun) {
		// This exact run is already in the journal (for example the
		// clock stepped back). Never publish it twice: move on.
		o.Log.Warn("run already dispatched; suppressing duplicate", "name", e.row.Name, "task_id", taskID)
		o.Metrics.Add("gobeat_skipped_runs_total", 1, "reason", "duplicate")
		ok, err := r.st.Skip(opCtx, e.row.ID, seen, now)
		if err != nil {
			return err
		}
		if !ok {
			return r.conflict(e)
		}
		r.advance(e, now, false)
		r.setNext(e, next)
		return nil
	}
	if err != nil {
		return err
	}
	if !ok {
		return r.conflict(e)
	}
	if seen != nil {
		o.Metrics.ObserveLateness(lateness.Seconds())
	}
	logDispatch("dispatch")
	o.Fault("after_claim")
	r.advance(e, now, true)
	item := &pendingItem{journalID: journalID, name: e.row.Name, taskID: taskID, claimedAt: now, msg: msg}
	if len(r.pending) > 0 || now.Before(r.brokerDown) {
		// keep order: older claims go first
		r.pending = append(r.pending, item)
		o.Metrics.Set("gobeat_pending_publishes", float64(len(r.pending)))
	} else if err := r.publish(opCtx, item, now); err != nil {
		return err
	}
	if e.spec.OneOff {
		// Python disables a one_off entry the next time is_due runs;
		// do it right away.
		r.setNext(e, now)
		return nil
	}
	r.setNext(e, next)
	return nil
}

// advance is ModelEntry.__next__: last_run_at = now, total_run_count += 1.
func (r *run) advance(e *entry, now time.Time, counted bool) {
	t := now
	e.spec.LastRunAt = &t
	e.row.LastRunAt = &t
	if counted {
		e.spec.TotalRunCount++
		e.row.TotalRunCount++
	}
}

func (r *run) conflict(e *entry) error {
	r.s.o.Log.Info("claim lost: the row changed under us; reloading", "name", e.row.Name, "periodic_task_id", e.row.ID)
	r.s.o.Metrics.Add("gobeat_claim_conflicts_total", 1)
	r.needReload = true
	return nil
}

// publish hands one claimed run to the broker. Broker failures never
// return an error (the run stays in the journal and is retried); only
// database failures do.
func (r *run) publish(ctx context.Context, it *pendingItem, now time.Time) error {
	o := &r.s.o
	pctx, cancel := context.WithTimeout(ctx, o.PublishTimeout)
	err := r.s.pub.Publish(pctx, it.msg)
	cancel()
	if err == nil {
		r.brokerFail = 0
		o.Metrics.Add("gobeat_dispatch_total", 1, "result", "published")
		o.Fault("after_publish")
		return r.st.MarkPublished(ctx, it.journalID, o.Clock.Now())
	}
	it.attempts++
	r.brokerFail++
	r.brokerDown = now.Add(backoff(r.brokerFail, time.Second, 60*time.Second))
	o.Metrics.Add("gobeat_errors_total", 1, "kind", "broker")
	o.Metrics.Add("gobeat_publish_retries_total", 1)
	o.Log.Warn("publish failed; the claimed run stays in the journal and will be retried", "name", it.name,
		"task_id", it.taskID, "attempt", it.attempts, "error", err.Error(),
		"retry_in", r.brokerDown.Sub(now).String())
	if merr := r.st.MarkAttempt(ctx, it.journalID, it.attempts, err.Error()); merr != nil {
		return merr
	}
	queued := false
	for _, p := range r.pending {
		queued = queued || p == it
	}
	if !queued {
		r.pending = append(r.pending, it)
	}
	o.Metrics.Set("gobeat_pending_publishes", float64(len(r.pending)))
	return nil
}

// flushPending retries claimed-but-unpublished runs, oldest first.
func (r *run) flushPending(ctx context.Context, now time.Time) error {
	o := &r.s.o
	if len(r.pending) == 0 || now.Before(r.brokerDown) {
		return nil
	}
	opCtx := context.WithoutCancel(ctx)
	for len(r.pending) > 0 {
		it := r.pending[0]
		if age := now.Sub(it.claimedAt); age > o.PublishMaxAge {
			o.Log.Error("ABANDONING claimed run: the broker did not accept it within GOBEAT_PUBLISH_MAX_AGE",
				"name", it.name, "task_id", it.taskID, "claimed_at", it.claimedAt.Format(time.RFC3339), "attempts", it.attempts)
			o.Metrics.Add("gobeat_abandoned_total", 1)
			if err := r.st.Abandon(opCtx, it.journalID, now, fmt.Sprintf("not published within %s", o.PublishMaxAge)); err != nil {
				return err
			}
			r.pending = r.pending[1:]
			continue
		}
		before := r.brokerFail
		if err := r.publish(opCtx, it, now); err != nil {
			return err
		}
		if r.brokerFail > before {
			break // still down; publish() set the next retry time
		}
		r.pending = r.pending[1:]
		o.Beat("leader")
	}
	o.Metrics.Set("gobeat_pending_publishes", float64(len(r.pending)))
	return nil
}

// NewReplyTo returns a random UUID for the reply_to property (Python uses
// Celery.thread_oid, a per-process UUID).
func NewReplyTo() string {
	var b [16]byte
	_, _ = crand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
