package schedule

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/bapp-open/django-celery-beat-go/internal/pytime"
)

// NeverCheck is django_celery_beat/utils.py: NEVER_CHECK_TIMEOUT.
const NeverCheck = 100000000

// ErrUnsupported marks schedules gobeat refuses to evaluate rather than
// guess (solar schedules, non-positive intervals, ...).
var ErrUnsupported = errors.New("unsupported schedule")

// Kind is the schedule type of a periodic task.
type Kind int

// Schedule kinds, matching the PeriodicTask foreign keys.
const (
	KindInterval Kind = iota + 1
	KindCrontab
	KindClocked
)

// Env carries the app-level settings that influence due computation.
type Env struct {
	// AppTZ is app.timezone (celery/app/base.py: Celery.timezone).
	AppTZ *time.Location
	// TZAware is settings.DJANGO_CELERY_BEAT_TZ_AWARE.
	TZAware bool
	// EnableUTC is app.conf.enable_utc.
	EnableUTC bool
	// CronDeadline is app.conf.beat_cron_starting_deadline; only the
	// plain crontab (TZAware == false) consults it.
	CronDeadline *float64
}

// Action is a side effect ModelEntry.is_due performs on the model.
type Action int

// Actions.
const (
	ActNone Action = iota
	// ActDisableExpired: `expires` has passed; Python calls _disable()
	// (enabled=False, saved with no_changes=True).
	ActDisableExpired
	// ActDisableOneOff: a one_off task that already ran; Python sets
	// enabled=False, total_run_count=0 and saves (touching the change
	// marker).
	ActDisableOneOff
)

// Spec is the state of one PeriodicTask row needed to decide dueness.
type Spec struct {
	Kind Kind
	// Interval
	Every time.Duration
	// Crontab (TZ is the CrontabSchedule timezone when TZAware, else
	// app.timezone)
	Cron *Crontab
	// Clocked
	ClockedTime time.Time

	Enabled       bool
	OneOff        bool
	StartTime     *time.Time
	Expires       *time.Time
	LastRunAt     *time.Time
	DateChanged   time.Time
	TotalRunCount int64
}

// Result of IsDue.
type Result struct {
	// DueAt is the instant the run became due (only set when Due).
	DueAt  time.Time
	Due    bool
	Next   float64 // seconds until the next check, as returned by Python
	Action Action
}

// IntervalDuration converts IntervalSchedule(every, period) to a duration:
// timedelta(**{period: every}) in django_celery_beat/models.py.
func IntervalDuration(every int64, period string) (time.Duration, error) {
	var unit time.Duration
	switch period {
	case "days":
		unit = 24 * time.Hour
	case "hours":
		unit = time.Hour
	case "minutes":
		unit = time.Minute
	case "seconds":
		unit = time.Second
	case "microseconds":
		unit = time.Microsecond
	default:
		return 0, fmt.Errorf("%w: interval period %q", ErrUnsupported, period)
	}
	if every <= 0 {
		// Python would fire such an entry on every tick; refuse instead.
		return 0, fmt.Errorf("%w: interval every=%d is not positive", ErrUnsupported, every)
	}
	if every > int64(math.MaxInt64/int64(unit))/4 {
		return 0, fmt.Errorf("%w: interval every=%d %s is too large", ErrUnsupported, every, period)
	}
	return time.Duration(every) * unit, nil
}

// EffectiveLastRun ports the last_run_at initialisation in
// django_celery_beat/schedulers.py: ModelEntry.__init__: a null
// last_run_at falls back to date_changed, pushed 30 years into the past
// when start_time is set so that the first run happens at start_time.
func (s *Spec) EffectiveLastRun() time.Time {
	if s.LastRunAt != nil {
		return *s.LastRunAt
	}
	t := s.DateChanged
	if s.StartTime != nil {
		t = t.Add(-365 * 30 * 24 * time.Hour)
	}
	return t
}

// IsDue ports django_celery_beat/schedulers.py: ModelEntry.is_due and the
// schedule it delegates to. It is a pure function of the row and now.
func (s *Spec) IsDue(env *Env, now time.Time) (Result, error) {
	if !s.Enabled {
		return Result{Next: 5.0}, nil // "5 second delay for re-enable"
	}
	if s.StartTime != nil && now.Before(*s.StartTime) {
		// "The datetime is before the start date - don't run."
		var remaining time.Duration
		if s.Kind == KindCrontab {
			if !env.TZAware {
				// Python: naive.astimezone(None) minus a naive now raises
				// TypeError and takes beat down. Refuse the task instead.
				return Result{}, fmt.Errorf("%w: crontab with a future start_time under DJANGO_CELERY_BEAT_TZ_AWARE=False crashes Python beat", ErrUnsupported)
			}
			// CrontabSchedule.due_start_time: remaining_delta is fed the
			// start time expressed in app.timezone, and the final
			// subtraction is between two datetimes sharing that tzinfo,
			// i.e. a wall-clock difference.
			sw := pytime.InZone(*s.StartTime, env.AppTZ)
			d, err := s.Cron.remainingDelta(sw, env.AppTZ, now)
			if err != nil {
				return Result{}, err
			}
			remaining = d.apply(sw).Sub(pytime.InZone(now, env.AppTZ))
		} else {
			remaining = s.StartTime.Sub(now)
		}
		return Result{Next: math.Ceil(pytime.TotalSeconds(remaining))}, nil
	}
	if s.Expires != nil && !now.Before(*s.Expires) {
		return Result{Next: NeverCheck, Action: ActDisableExpired}, nil
	}
	if s.OneOff && s.TotalRunCount > 0 {
		return Result{Next: NeverCheck, Action: ActDisableOneOff}, nil
	}
	last := s.EffectiveLastRun()
	switch s.Kind {
	case KindInterval:
		// celery/schedules.py: schedule.is_due -> remaining(last, run_every,
		// now): wall-clock addition in app.timezone.
		end := pytime.InZone(last, env.AppTZ).Add(s.Every).UTC(env.AppTZ)
		if rem := end.Sub(now); rem > 0 {
			return Result{Next: pytime.TotalSeconds(rem)}, nil
		}
		return Result{Due: true, DueAt: end, Next: pytime.TotalSeconds(s.Every)}, nil
	case KindClocked:
		// django_celery_beat/clockedschedule.py: clocked.is_due
		if rem := s.ClockedTime.Sub(now); rem > 0 {
			return Result{Next: pytime.TotalSeconds(rem)}, nil
		}
		return Result{Due: true, DueAt: s.ClockedTime, Next: NeverCheck}, nil
	case KindCrontab:
		if !env.EnableUTC && s.Cron.TZ.String() == "UTC" {
			// celery/schedules.py: BaseSchedule.to_local converts UTC
			// datetimes to the *process* local zone when enable_utc is
			// off; that depends on the beat host's TZ and is not
			// reproducible here.
			return Result{}, fmt.Errorf("%w: enable_utc=False with a UTC crontab is evaluated in the beat process's local time by Python", ErrUnsupported)
		}
		var (
			due  bool
			next float64
			err  error
		)
		if env.TZAware {
			due, next, err = s.Cron.IsDueTZAware(last, now)
		} else {
			due, next, err = s.Cron.IsDuePlain(last, now, env.CronDeadline)
		}
		if err != nil {
			return Result{}, err
		}
		r := Result{Due: due, Next: next}
		if due {
			// the first matching minute after last_run_at
			rem, _ := s.Cron.Remaining(last, now)
			r.DueAt = now.Add(rem)
		}
		return r, nil
	}
	return Result{}, fmt.Errorf("%w: no schedule", ErrUnsupported)
}
