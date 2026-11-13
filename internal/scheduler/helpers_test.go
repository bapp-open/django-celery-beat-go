package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/bapp-open/django-celery-beat-go/internal/broker"
	"github.com/bapp-open/django-celery-beat-go/internal/clock"
	"github.com/bapp-open/django-celery-beat-go/internal/config"
	"github.com/bapp-open/django-celery-beat-go/internal/message"
	"github.com/bapp-open/django-celery-beat-go/internal/metrics"
	"github.com/bapp-open/django-celery-beat-go/internal/store"
	"github.com/bapp-open/django-celery-beat-go/internal/store/memstore"
)

const settingsJSON = `{
 "schema": 1,
 "versions": {"celery": "5.5.3", "kombu": "5.5.4", "django_celery_beat": "2.8.1"},
 "broker_url": "memory://", "broker_transport_options": {},
 "timezone": %q, "enable_utc": true, "use_tz": true, "tz_aware": true,
 "beat_max_loop_interval": 5, "beat_cron_starting_deadline": null,
 "task_default_queue": "celery", "task_default_exchange": "celery",
 "task_default_exchange_type": "direct", "task_default_routing_key": "celery",
 "task_default_delivery_mode": 2, "task_default_priority": null,
 "task_serializer": "json", "task_compression": null,
 "task_create_missing_queues": true, "task_queue_max_priority": null,
 "task_protocol": 2, "has_routes": false,
 "queues": {"celery": {"name": "celery", "routing_key": "celery", "durable": true,
   "queue_arguments": {}, "exchange": {"name": "celery", "type": "direct", "durable": true}}},
 "tasks": {"app.pickled": {"registered": true, "options": {"serializer": "pickle"}}},
 "warnings": []
}`

func settingsDoc(tz string) []byte { return []byte(fmt.Sprintf(settingsJSON, tz)) }

// fakeBroker records published messages and can be switched off.
type fakeBroker struct {
	mu       sync.Mutex
	down     bool
	failNext int
	msgs     []*message.Message
	keep     bool
	count    map[string]int // by periodic task name
	onPub    func(*message.Message)
}

func newBroker() *fakeBroker { return &fakeBroker{keep: true, count: map[string]int{}} }

func (b *fakeBroker) Publish(_ context.Context, m *message.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.down {
		return errors.New("fake broker: connection refused")
	}
	if b.failNext > 0 {
		b.failNext--
		return errors.New("fake broker: transient failure")
	}
	if b.keep {
		b.msgs = append(b.msgs, m)
	}
	b.count[m.Name]++
	if b.onPub != nil {
		b.onPub(m)
	}
	return nil
}
func (b *fakeBroker) Close() error { return nil }

func (b *fakeBroker) setDown(v bool) { b.mu.Lock(); b.down = v; b.mu.Unlock() }
func (b *fakeBroker) n(name string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count[name]
}
func (b *fakeBroker) ids() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.msgs))
	for i, m := range b.msgs {
		out[i] = m.TaskID
	}
	return out
}

// logBuf collects log lines for assertions.
type logBuf struct {
	mu sync.Mutex
	sb strings.Builder
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sb.Write(p)
}
func (l *logBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.sb.String() }

var t0 = time.Date(2024, 10, 20, 0, 0, 0, 0, time.UTC)

// world is one simulated deployment: fake clock, in-memory DB, fake broker.
type world struct {
	t      *testing.T
	clk    *clock.Fake
	db     *memstore.DB
	broker *fakeBroker
	log    *logBuf
	reg    *metrics.Registry
	opts   Options
}

func newWorld(t *testing.T, tz string) *world {
	w := &world{t: t, clk: clock.NewFake(t0), broker: newBroker(), log: &logBuf{}, reg: metrics.New()}
	w.db = memstore.New(w.clk, settingsDoc(tz), "v1")
	w.opts = Options{
		Clock: w.clk, Opener: w.db, Metrics: w.reg,
		Log:          slog.New(slog.NewTextHandler(w.log, &slog.HandlerOptions{Level: slog.LevelDebug})),
		NewPublisher: func(*config.Settings) (broker.Publisher, error) { return w.broker, nil },
		Origin:       "gen1@test", ReplyTo: "reply",
		// simulated pause: just let fake time pass
		Pause: func(_ context.Context, d time.Duration) { w.clk.Advance(d) },
	}
	return w
}

// runUntil runs a scheduler in simulated time until the fake clock reaches
// end (checked whenever the scheduler goes to sleep), then stops it.
func (w *world) runUntil(end time.Time) error {
	return w.runWith(w.opts, end, nil)
}

func (w *world) runWith(o Options, end time.Time, each func(now time.Time)) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := func() {
		now := w.clk.Now()
		if each != nil {
			each(now)
		}
		if !now.Before(end) {
			cancel()
		}
	}
	w.db.BeforeWait = stop
	pause := o.Pause
	o.Pause = func(ctx context.Context, d time.Duration) {
		stop()
		if ctx.Err() == nil {
			pause(ctx, d)
		}
	}
	return New(o).Run(ctx)
}

func ptr[T any](v T) *T { return &v }

func intervalTask(id int64, name string, every int64, period string) store.Task {
	return store.Task{ID: id, Name: name, Task: "app.task", IntervalEvery: &every, IntervalPeriod: &period,
		Args: "[]", Kwargs: "{}", Enabled: true, DateChanged: t0}
}

func cronTask(id int64, name, m, h, dow, dom, moy, tz string) store.Task {
	return store.Task{ID: id, Name: name, Task: "app.task",
		Cron: &store.CronRow{Minute: m, Hour: h, DayOfWeek: dow, DayOfMonth: dom, MonthOfYear: moy, Timezone: tz},
		Args: "[]", Kwargs: "{}", Enabled: true, DateChanged: t0}
}

func assertNoDuplicates(t *testing.T, ids []string) {
	t.Helper()
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("task id %s published twice", id)
		}
		seen[id] = true
	}
}
