package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// NotifyChannel is the channel the django_gobeat triggers NOTIFY on.
const NotifyChannel = "gobeat"

// DefaultLockKey is the advisory lock key ("gobeat" in ASCII).
const DefaultLockKey int64 = 0x676f62656174

// knownMigrations are the django_celery_beat migrations of the supported
// releases (2.8.0 - 2.9.0 end at 0019). A database with a migration
// that is not in this list has a schema this binary has never seen.
var knownMigrations = map[string]bool{
	"0001_initial": true, "0002_auto_20161118_0346": true, "0003_auto_20161209_0049": true,
	"0004_auto_20170221_0000": true, "0005_add_solarschedule_events_choices": true,
	"0006_auto_20180210_1226": true, "0006_auto_20180322_0932": true, "0006_periodictask_priority": true,
	"0007_auto_20180521_0826": true, "0008_auto_20180914_1922": true, "0009_periodictask_headers": true,
	"0010_auto_20190429_0326": true, "0011_auto_20190508_0153": true, "0012_periodictask_expire_seconds": true,
	"0013_auto_20200609_0727": true, "0014_remove_clockedschedule_enabled": true,
	"0015_edit_solarschedule_events_choices": true, "0016_alter_crontabschedule_timezone": true,
	"0017_alter_crontabschedule_month_of_year": true, "0018_improve_crontab_helptext": true,
	"0019_alter_periodictasks_options": true,
}

// requiredMigration is the newest migration every supported release has.
const requiredMigration = "0019_alter_periodictasks_options"

// requiredColumns lists the columns gobeat reads or writes.
var requiredColumns = map[string][]string{
	"django_celery_beat_periodictask": {"id", "name", "task", "args", "kwargs", "headers", "queue", "exchange",
		"routing_key", "priority", "expires", "expire_seconds", "one_off", "start_time", "enabled", "last_run_at",
		"total_run_count", "date_changed", "interval_id", "crontab_id", "clocked_id", "solar_id"},
	"django_celery_beat_intervalschedule": {"id", "every", "period"},
	"django_celery_beat_crontabschedule":  {"id", "minute", "hour", "day_of_week", "day_of_month", "month_of_year", "timezone"},
	"django_celery_beat_clockedschedule":  {"id", "clocked_time"},
	"django_celery_beat_periodictasks":    {"ident", "last_update"},
	"django_gobeat_settings":              {"key", "version", "data"},
	"django_gobeat_dispatch": {"id", "periodic_task_id", "name", "task", "task_id", "scheduled_for", "claimed_at",
		"published_at", "abandoned_at", "attempts", "last_error", "message"},
}

// PostgresOpener opens sessions against one database.
type PostgresOpener struct {
	URL         string
	LockKey     int64
	SettingsKey string
	// ConnectTimeout bounds connection establishment.
	ConnectTimeout time.Duration
}

// Open implements Opener.
func (o *PostgresOpener) Open(ctx context.Context) (Store, error) {
	cfg, err := pgx.ParseConfig(o.URL)
	if err != nil {
		return nil, fmt.Errorf("DATABASE_URL: %w", err)
	}
	if cfg.RuntimeParams["application_name"] == "" {
		cfg.RuntimeParams["application_name"] = "gobeat"
	}
	// All instants are exchanged in UTC; `timestamp without time zone`
	// columns (USE_TZ=False) are read and written as UTC wall time, which
	// is how Python beat itself treats naive values (maybe_make_aware).
	cfg.RuntimeParams["timezone"] = "UTC"
	if o.ConnectTimeout > 0 {
		cfg.ConnectTimeout = o.ConnectTimeout
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	s := &pg{conn: conn, lockKey: o.LockKey, key: o.SettingsKey, ts: "timestamptz"}
	if s.key == "" {
		s.key = "default"
	}
	if s.lockKey == 0 {
		s.lockKey = DefaultLockKey
	}
	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		conn.Close(ctx)
		return nil, err
	}
	return s, nil
}

type pg struct {
	conn    *pgx.Conn
	lockKey int64
	key     string
	ts      string // "timestamptz" or "timestamp" (USE_TZ=False)
}

func (s *pg) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = s.conn.Close(ctx)
}

func (s *pg) TryLock(ctx context.Context) (bool, error) {
	var ok bool
	err := s.conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", s.lockKey).Scan(&ok)
	return ok, err
}

func isUndefinedTable(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "42P01"
}

func (s *pg) Settings(ctx context.Context) ([]byte, string, error) {
	var (
		version string
		data    string
	)
	err := s.conn.QueryRow(ctx, "SELECT version, data::text FROM django_gobeat_settings WHERE key = $1", s.key).Scan(&version, &data)
	if errors.Is(err, pgx.ErrNoRows) || isUndefinedTable(err) {
		return nil, "", ErrNoSettings
	}
	if err != nil {
		return nil, "", err
	}
	return []byte(data), version, nil
}

// SchemaError reports a schema this binary refuses to run against.
type SchemaError struct{ Msg string }

func (e *SchemaError) Error() string { return "unknown database schema: " + e.Msg }

func (s *pg) CheckSchema(ctx context.Context, useTZ bool) error {
	rows, err := s.conn.Query(ctx, "SELECT name FROM django_migrations WHERE app = 'django_celery_beat'")
	if err != nil {
		if isUndefinedTable(err) {
			return &SchemaError{"django_migrations table not found; is DATABASE_URL pointing at the Django database?"}
		}
		return err
	}
	applied, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		return &SchemaError{"django_celery_beat has no applied migrations; run `manage.py migrate`"}
	}
	hasRequired := false
	var unknown []string
	for _, name := range applied {
		if !knownMigrations[name] {
			unknown = append(unknown, name)
		}
		hasRequired = hasRequired || name == requiredMigration
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return &SchemaError{fmt.Sprintf("django_celery_beat migration(s) %s are not known to this gobeat build; upgrade gobeat before upgrading django-celery-beat", strings.Join(unknown, ", "))}
	}
	if !hasRequired {
		return &SchemaError{fmt.Sprintf("django_celery_beat migration %s is not applied; supported django-celery-beat versions are 2.8 and 2.9", requiredMigration)}
	}

	rows, err = s.conn.Query(ctx, `SELECT table_name, column_name, data_type FROM information_schema.columns
		WHERE table_schema = ANY (current_schemas(false)) AND table_name = ANY ($1)`, tableNames())
	if err != nil {
		return err
	}
	type col struct{ Table, Column, Type string }
	cols, err := pgx.CollectRows(rows, pgx.RowToStructByPos[col])
	if err != nil {
		return err
	}
	have := map[string]string{}
	for _, c := range cols {
		have[c.Table+"."+c.Column] = c.Type
	}
	var missing []string
	for table, names := range requiredColumns {
		for _, n := range names {
			if _, ok := have[table+"."+n]; !ok {
				missing = append(missing, table+"."+n)
			}
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		hint := ""
		for _, m := range missing {
			if strings.HasPrefix(m, "django_gobeat_") {
				hint = " (add 'django_gobeat' to INSTALLED_APPS and run `manage.py migrate`)"
			}
		}
		return &SchemaError{"missing column(s): " + strings.Join(missing, ", ") + hint}
	}
	wantType := "timestamp with time zone"
	s.ts = "timestamptz"
	if !useTZ {
		wantType = "timestamp without time zone"
		s.ts = "timestamp"
	}
	for _, c := range []string{"django_celery_beat_periodictask.last_run_at", "django_celery_beat_periodictask.start_time",
		"django_celery_beat_periodictask.expires", "django_celery_beat_periodictask.date_changed",
		"django_celery_beat_clockedschedule.clocked_time", "django_celery_beat_periodictasks.last_update",
		"django_gobeat_dispatch.scheduled_for", "django_gobeat_dispatch.claimed_at"} {
		if have[c] != wantType {
			return &SchemaError{fmt.Sprintf("column %s is %q but the settings row says USE_TZ=%v (expected %q); re-run gobeat_sync", c, have[c], useTZ, wantType)}
		}
	}
	return nil
}

func tableNames() []string {
	out := make([]string, 0, len(requiredColumns))
	for t := range requiredColumns {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func (s *pg) State(ctx context.Context) (State, error) {
	var (
		st      State
		version *string
	)
	err := s.conn.QueryRow(ctx, `SELECT
		(SELECT last_update FROM django_celery_beat_periodictasks WHERE ident = 1),
		(SELECT version FROM django_gobeat_settings WHERE key = $1)`, s.key).Scan(&st.Marker, &version)
	if err != nil {
		return st, err
	}
	if st.Marker != nil {
		t := st.Marker.UTC()
		st.Marker = &t
	}
	if version != nil {
		st.SettingsVersion = *version
	}
	return st, nil
}

const tasksSQL = `SELECT t.id, t.name, t.task, COALESCE(t.args, ''), COALESCE(t.kwargs, ''), t.headers,
	t.queue, t.exchange, t.routing_key, t.priority, t.expires, t.expire_seconds,
	t.one_off, t.start_time, t.enabled, t.last_run_at, t.total_run_count, t.date_changed,
	i.every, i.period,
	c.id, c.minute, c.hour, c.day_of_week, c.day_of_month, c.month_of_year, c.timezone,
	k.clocked_time, t.solar_id IS NOT NULL
FROM django_celery_beat_periodictask t
LEFT JOIN django_celery_beat_intervalschedule i ON i.id = t.interval_id
LEFT JOIN django_celery_beat_crontabschedule c ON c.id = t.crontab_id
LEFT JOIN django_celery_beat_clockedschedule k ON k.id = t.clocked_id
WHERE t.enabled
ORDER BY t.id`

func utcp(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func (s *pg) Tasks(ctx context.Context) ([]Task, error) {
	rows, err := s.conn.Query(ctx, tasksSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Task
	for rows.Next() {
		var (
			t                                       Task
			priority, expireSeconds                 *int64
			cronID                                  *int64
			minute, hour, dow, dom, moy, tz, period *string
		)
		if err := rows.Scan(&t.ID, &t.Name, &t.Task, &t.Args, &t.Kwargs, &t.Headers,
			&t.Queue, &t.Exchange, &t.RoutingKey, &priority, &t.Expires, &expireSeconds,
			&t.OneOff, &t.StartTime, &t.Enabled, &t.LastRunAt, &t.TotalRunCount, &t.DateChanged,
			&t.IntervalEvery, &period,
			&cronID, &minute, &hour, &dow, &dom, &moy, &tz,
			&t.ClockedTime, &t.Solar); err != nil {
			return nil, err
		}
		t.Priority, t.ExpireSeconds, t.IntervalPeriod = priority, expireSeconds, period
		if cronID != nil {
			d := func(p *string) string {
				if p == nil {
					return ""
				}
				return *p
			}
			t.Cron = &CronRow{d(minute), d(hour), d(dow), d(dom), d(moy), d(tz)}
		}
		t.Expires, t.StartTime, t.LastRunAt, t.ClockedTime = utcp(t.Expires), utcp(t.StartTime), utcp(t.LastRunAt), utcp(t.ClockedTime)
		t.DateChanged = t.DateChanged.UTC()
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *pg) Claim(ctx context.Context, c Claim) (bool, int64, error) {
	// One statement: the compare-and-swap and the journal insert commit
	// or fail together.
	q := fmt.Sprintf(`WITH claimed AS (
		UPDATE django_celery_beat_periodictask
		SET last_run_at = $1::%[1]s, total_run_count = total_run_count + 1, date_changed = $1::%[1]s
		WHERE id = $2 AND enabled AND last_run_at IS NOT DISTINCT FROM $3::%[1]s
		RETURNING id)
	INSERT INTO django_gobeat_dispatch
		(periodic_task_id, name, task, task_id, scheduled_for, claimed_at, attempts, last_error, message)
	SELECT id, $4, $5, $6, $7::%[1]s, $1::%[1]s, 0, '', $8::jsonb FROM claimed
	RETURNING id`, s.ts)
	var id int64
	err := s.conn.QueryRow(ctx, q, c.Now, c.PeriodicTaskID, c.SeenLastRunAt, c.Name, c.Task, c.TaskID,
		c.ScheduledFor, string(c.Message)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, 0, nil
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" { // unique_violation on task_id
		return false, 0, ErrDuplicateRun
	}
	if err != nil {
		return false, 0, err
	}
	return true, id, nil
}

func (s *pg) Skip(ctx context.Context, id int64, seen *time.Time, now time.Time) (bool, error) {
	q := fmt.Sprintf(`UPDATE django_celery_beat_periodictask SET last_run_at = $1::%[1]s, date_changed = $1::%[1]s
		WHERE id = $2 AND enabled AND last_run_at IS NOT DISTINCT FROM $3::%[1]s`, s.ts)
	tag, err := s.conn.Exec(ctx, q, now, id, seen)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *pg) MarkPublished(ctx context.Context, journalID int64, now time.Time) error {
	_, err := s.conn.Exec(ctx, fmt.Sprintf(`UPDATE django_gobeat_dispatch
		SET published_at = $1::%s, attempts = attempts + 1, last_error = '' WHERE id = $2`, s.ts), now, journalID)
	return err
}

func (s *pg) MarkAttempt(ctx context.Context, journalID int64, attempts int, lastErr string) error {
	_, err := s.conn.Exec(ctx, `UPDATE django_gobeat_dispatch SET attempts = $1, last_error = $2 WHERE id = $3`,
		attempts, truncate(lastErr, 2000), journalID)
	return err
}

func (s *pg) Abandon(ctx context.Context, journalID int64, now time.Time, reason string) error {
	_, err := s.conn.Exec(ctx, fmt.Sprintf(`UPDATE django_gobeat_dispatch
		SET abandoned_at = $1::%s, last_error = $2 WHERE id = $3`, s.ts), now, truncate(reason, 2000), journalID)
	return err
}

func (s *pg) Pending(ctx context.Context) ([]Pending, error) {
	rows, err := s.conn.Query(ctx, `SELECT id, periodic_task_id, task_id, name, claimed_at, attempts, message::text
		FROM django_gobeat_dispatch WHERE published_at IS NULL AND abandoned_at IS NULL ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Pending
	for rows.Next() {
		var (
			p   Pending
			msg string
		)
		if err := rows.Scan(&p.JournalID, &p.PeriodicTaskID, &p.TaskID, &p.Name, &p.ClaimedAt, &p.Attempts, &msg); err != nil {
			return nil, err
		}
		p.ClaimedAt = p.ClaimedAt.UTC()
		p.Message = []byte(msg)
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *pg) DisableExpired(ctx context.Context, id int64, now time.Time) error {
	// ModelEntry._disable: enabled=False; PeriodicTask.save() nulls
	// last_run_at for a disabled task; no_changes=True leaves the change
	// marker alone.
	_, err := s.conn.Exec(ctx, fmt.Sprintf(`UPDATE django_celery_beat_periodictask
		SET enabled = false, last_run_at = NULL, date_changed = $1::%s WHERE id = $2 AND enabled`, s.ts), now, id)
	return err
}

func (s *pg) DisableOneOff(ctx context.Context, id int64, now time.Time) error {
	// ModelEntry.is_due "ONE OFF TASK": enabled=False, total_run_count=0,
	// no_changes=False -> PeriodicTasks.update_changed().
	batch := &pgx.Batch{}
	batch.Queue(fmt.Sprintf(`UPDATE django_celery_beat_periodictask
		SET enabled = false, total_run_count = 0, last_run_at = NULL, date_changed = $1::%s
		WHERE id = $2 AND enabled`, s.ts), now, id)
	batch.Queue(fmt.Sprintf(`INSERT INTO django_celery_beat_periodictasks (ident, last_update) VALUES (1, $1::%[1]s)
		ON CONFLICT (ident) DO UPDATE SET last_update = EXCLUDED.last_update`, s.ts), now)
	return s.conn.SendBatch(ctx, batch).Close()
}

func (s *pg) Prune(ctx context.Context, before time.Time) error {
	_, err := s.conn.Exec(ctx, fmt.Sprintf(`DELETE FROM django_gobeat_dispatch
		WHERE (published_at IS NOT NULL AND published_at < $1::%[1]s)
		   OR (abandoned_at IS NOT NULL AND abandoned_at < $1::%[1]s)`, s.ts), before)
	return err
}

func (s *pg) Wait(ctx context.Context, d time.Duration) (bool, error) {
	if d <= 0 {
		return false, ctx.Err()
	}
	wctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	_, err := s.conn.WaitForNotification(wctx)
	if err == nil {
		// Collapse a burst of notifications into one wake-up.
		for {
			dctx, dcancel := context.WithTimeout(ctx, time.Millisecond)
			_, derr := s.conn.WaitForNotification(dctx)
			dcancel()
			if derr != nil {
				break
			}
		}
		if s.conn.IsClosed() {
			return true, errors.New("database connection closed")
		}
		return true, ctx.Err()
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err) {
		if s.conn.IsClosed() {
			return false, errors.New("database connection closed")
		}
		return false, nil
	}
	return false, err
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
