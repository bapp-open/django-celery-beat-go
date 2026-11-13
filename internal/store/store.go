// Package store is gobeat's view of the database: the django-celery-beat
// tables, the settings row and the dispatch journal.
package store

import (
	"context"
	"errors"
	"time"
)

// Errors.
var (
	// ErrNoSettings: the settings row is missing (run `manage.py gobeat_sync`).
	ErrNoSettings = errors.New("gobeat settings row not found: run `python manage.py migrate` and `python manage.py gobeat_sync` in the Django project first")
	// ErrDuplicateRun: the journal already holds this (task, scheduled time).
	ErrDuplicateRun = errors.New("run already journaled")
)

// CronRow is a CrontabSchedule row.
type CronRow struct {
	Minute, Hour, DayOfWeek, DayOfMonth, MonthOfYear string
	Timezone                                         string
}

// Task is one enabled PeriodicTask row joined with its schedule.
type Task struct {
	ID   int64
	Name string
	Task string

	IntervalEvery  *int64
	IntervalPeriod *string
	Cron           *CronRow
	ClockedTime    *time.Time
	Solar          bool

	Args, Kwargs  string
	Headers       *string
	Queue         *string
	Exchange      *string
	RoutingKey    *string
	Priority      *int64
	Expires       *time.Time
	ExpireSeconds *int64

	OneOff        bool
	StartTime     *time.Time
	Enabled       bool
	LastRunAt     *time.Time
	TotalRunCount int64
	DateChanged   time.Time
}

// Claim is one run to be claimed atomically with its journal row.
type Claim struct {
	PeriodicTaskID int64
	// SeenLastRunAt is the last_run_at value the decision was based on
	// (nil for NULL); the claim succeeds only if the row still has it.
	SeenLastRunAt *time.Time
	Now           time.Time
	ScheduledFor  time.Time
	TaskID        string
	Name          string
	Task          string
	Message       []byte // journal JSON (message.Message)
}

// Pending is a journal row that was claimed but not yet published.
type Pending struct {
	JournalID      int64
	PeriodicTaskID int64
	TaskID         string
	Name           string
	ClaimedAt      time.Time
	Attempts       int
	Message        []byte
}

// State is the cheap "did anything change" probe.
type State struct {
	Marker          *time.Time // PeriodicTasks.last_update (nil when absent)
	SettingsVersion string     // "" when the settings row is missing
}

// Store is everything the scheduler needs from the database. A Store is
// one database session: any error other than the sentinel ones means the
// session is unusable and the caller must Close it and open a new one.
type Store interface {
	// TryLock tries to take the leader lock for this session.
	TryLock(ctx context.Context) (bool, error)
	// Settings returns the settings document and its version hash.
	Settings(ctx context.Context) (data []byte, version string, err error)
	// CheckSchema verifies the django-celery-beat and django_gobeat
	// tables look like a schema this binary knows. useTZ is the Django
	// USE_TZ setting from the settings row.
	CheckSchema(ctx context.Context, useTZ bool) error
	// State returns the change marker and settings version.
	State(ctx context.Context) (State, error)
	// Tasks loads all enabled periodic tasks.
	Tasks(ctx context.Context) ([]Task, error)

	// Claim performs the compare-and-swap on last_run_at and writes the
	// journal row in the same statement. ok is false when the row
	// changed under us. ErrDuplicateRun is returned (with ok false) when
	// the journal already holds the task id.
	Claim(ctx context.Context, c Claim) (ok bool, journalID int64, err error)
	// Skip advances last_run_at with the same compare-and-swap but
	// without counting or journaling a run (missed-run policy "skip").
	Skip(ctx context.Context, id int64, seen *time.Time, now time.Time) (bool, error)
	// MarkPublished records that the broker accepted the message.
	MarkPublished(ctx context.Context, journalID int64, now time.Time) error
	// MarkAttempt records a failed publish attempt.
	MarkAttempt(ctx context.Context, journalID int64, attempts int, lastErr string) error
	// Abandon gives up on a journal row (loudly, by the caller).
	Abandon(ctx context.Context, journalID int64, now time.Time, reason string) error
	// Pending returns claimed-but-unpublished journal rows, oldest first.
	Pending(ctx context.Context) ([]Pending, error)
	// DisableExpired is ModelEntry._disable for an expired task.
	DisableExpired(ctx context.Context, id int64, now time.Time) error
	// DisableOneOff disables a one_off task that has run, resets its
	// run count and touches the change marker, as ModelEntry.is_due does.
	DisableOneOff(ctx context.Context, id int64, now time.Time) error
	// Prune deletes published/abandoned journal rows older than before.
	Prune(ctx context.Context, before time.Time) error

	// Wait blocks until the database signals a change (LISTEN/NOTIFY) or
	// d has elapsed; it is the scheduler's only way to sleep.
	Wait(ctx context.Context, d time.Duration) (notified bool, err error)
	// Close ends the session (releasing the leader lock).
	Close()
}

// Opener opens database sessions.
type Opener interface {
	Open(ctx context.Context) (Store, error)
}
