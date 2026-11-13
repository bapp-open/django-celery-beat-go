// Package memstore is an in-memory store.Store used by the scheduler's
// fake-clock tests and simulations. It models the tables, the leader lock
// and the dispatch journal, and can inject failures.
package memstore

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/bapp-open/django-celery-beat-go/internal/clock"
	"github.com/bapp-open/django-celery-beat-go/internal/store"
)

// JournalRow is a django_gobeat_dispatch row.
type JournalRow struct {
	ID             int64
	PeriodicTaskID int64
	Name           string
	TaskID         string
	ScheduledFor   time.Time
	ClaimedAt      time.Time
	PublishedAt    *time.Time
	AbandonedAt    *time.Time
	Attempts       int
	LastError      string
	Message        []byte
}

// ErrInjected is the error returned by injected failures.
var ErrInjected = errors.New("memstore: injected database failure")

// DB is the shared "database". All methods are safe for concurrent use.
type DB struct {
	mu sync.Mutex

	Clock *clock.Fake // advanced by Wait in simulation mode
	// RealTime makes Wait sleep on the wall clock instead of advancing
	// the fake clock (needed when several schedulers share the DB).
	RealTime bool
	// KeepJournal false drops journal rows once published (soak runs).
	KeepJournal bool

	tasks    map[int64]*store.Task
	marker   *time.Time
	settings []byte
	version  string
	journal  map[int64]*JournalRow
	byTaskID map[string]struct{}
	nextJID  int64
	owner    *session
	notify   chan struct{}

	down bool
	// Hook, when set, is called before every operation with its name; a
	// non-nil error fails the operation.
	Hook func(op string) error
	// BeforeWait is called at the start of every Wait (simulation
	// drivers use it to stop the run or mutate the schedule).
	BeforeWait func()
}

// New returns an empty database holding the given settings document.
func New(c *clock.Fake, settings []byte, version string) *DB {
	return &DB{Clock: c, KeepJournal: true, tasks: map[int64]*store.Task{}, settings: settings, version: version,
		journal: map[int64]*JournalRow{}, byTaskID: map[string]struct{}{}, notify: make(chan struct{}, 1)}
}

// Open implements store.Opener.
func (db *DB) Open(ctx context.Context) (store.Store, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.down {
		return nil, ErrInjected
	}
	if db.Hook != nil {
		if err := db.Hook("open"); err != nil {
			return nil, err
		}
	}
	return &session{db: db}, nil
}

// SetDown makes the database unreachable: open sessions fail on their
// next operation (and lose the leader lock), new ones cannot be opened.
func (db *DB) SetDown(down bool) {
	db.mu.Lock()
	db.down = down
	if down {
		db.owner = nil
	}
	db.mu.Unlock()
}

func (db *DB) touch() {
	select {
	case db.notify <- struct{}{}:
	default:
	}
}

// Put inserts or replaces a periodic task row and touches the change
// marker, like PeriodicTask.save().
func (db *DB) Put(t store.Task) {
	db.mu.Lock()
	cp := t
	db.tasks[t.ID] = &cp
	db.bump()
	db.mu.Unlock()
	db.touch()
}

// PutQuiet inserts rows without touching the marker (bulk seeding).
func (db *DB) PutQuiet(ts []store.Task) {
	db.mu.Lock()
	for i := range ts {
		cp := ts[i]
		db.tasks[cp.ID] = &cp
	}
	db.mu.Unlock()
}

func (db *DB) bump() {
	now := db.Clock.Now()
	if db.marker != nil && !now.After(*db.marker) {
		now = db.marker.Add(time.Microsecond)
	}
	db.marker = &now
}

// Delete removes a row, like PeriodicTask.delete().
func (db *DB) Delete(id int64) {
	db.mu.Lock()
	delete(db.tasks, id)
	db.bump()
	db.mu.Unlock()
	db.touch()
}

// Get returns a copy of a row.
func (db *DB) Get(id int64) (store.Task, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	t, ok := db.tasks[id]
	if !ok {
		return store.Task{}, false
	}
	return *t, true
}

// SetSettings replaces the settings document.
func (db *DB) SetSettings(data []byte, version string) {
	db.mu.Lock()
	db.settings, db.version = data, version
	db.mu.Unlock()
	db.touch()
}

// Journal returns a snapshot of the journal ordered by id.
func (db *DB) Journal() []JournalRow {
	db.mu.Lock()
	defer db.mu.Unlock()
	out := make([]JournalRow, 0, len(db.journal))
	for _, r := range db.journal {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Leader reports whether some session holds the leader lock.
func (db *DB) Leader() bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.owner != nil
}

type session struct {
	db     *DB
	closed bool
}

// begin locks the DB and applies failure injection.
func (s *session) begin(op string) error {
	s.db.mu.Lock()
	if s.closed {
		return errors.New("memstore: session closed")
	}
	if s.db.down {
		return ErrInjected
	}
	if s.db.Hook != nil {
		if err := s.db.Hook(op); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) Close() {
	s.db.mu.Lock()
	s.closed = true
	if s.db.owner == s {
		s.db.owner = nil
	}
	s.db.mu.Unlock()
}

func (s *session) TryLock(context.Context) (bool, error) {
	defer s.db.mu.Unlock()
	if err := s.begin("lock"); err != nil {
		return false, err
	}
	if s.db.owner == nil || s.db.owner == s {
		s.db.owner = s
		return true, nil
	}
	return false, nil
}

func (s *session) Settings(context.Context) ([]byte, string, error) {
	defer s.db.mu.Unlock()
	if err := s.begin("settings"); err != nil {
		return nil, "", err
	}
	if s.db.settings == nil {
		return nil, "", store.ErrNoSettings
	}
	return s.db.settings, s.db.version, nil
}

func (s *session) CheckSchema(context.Context, bool) error {
	defer s.db.mu.Unlock()
	return s.begin("schema")
}

func (s *session) State(context.Context) (store.State, error) {
	defer s.db.mu.Unlock()
	if err := s.begin("state"); err != nil {
		return store.State{}, err
	}
	st := store.State{SettingsVersion: s.db.version}
	if s.db.settings == nil {
		st.SettingsVersion = ""
	}
	if s.db.marker != nil {
		m := *s.db.marker
		st.Marker = &m
	}
	return st, nil
}

func (s *session) Tasks(context.Context) ([]store.Task, error) {
	defer s.db.mu.Unlock()
	if err := s.begin("tasks"); err != nil {
		return nil, err
	}
	out := make([]store.Task, 0, len(s.db.tasks))
	for _, t := range s.db.tasks {
		if t.Enabled {
			out = append(out, *t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func sameTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

func (s *session) Claim(_ context.Context, c store.Claim) (bool, int64, error) {
	defer s.db.mu.Unlock()
	if err := s.begin("claim"); err != nil {
		return false, 0, err
	}
	t, ok := s.db.tasks[c.PeriodicTaskID]
	if !ok || !t.Enabled || !sameTime(t.LastRunAt, c.SeenLastRunAt) {
		return false, 0, nil
	}
	if _, dup := s.db.byTaskID[c.TaskID]; dup {
		return false, 0, store.ErrDuplicateRun
	}
	now := c.Now
	t.LastRunAt = &now
	t.TotalRunCount++
	t.DateChanged = now
	s.db.nextJID++
	s.db.journal[s.db.nextJID] = &JournalRow{ID: s.db.nextJID, PeriodicTaskID: c.PeriodicTaskID, Name: c.Name,
		TaskID: c.TaskID, ScheduledFor: c.ScheduledFor, ClaimedAt: c.Now, Message: c.Message}
	if s.db.KeepJournal {
		s.db.byTaskID[c.TaskID] = struct{}{}
	}
	return true, s.db.nextJID, nil
}

func (s *session) Skip(_ context.Context, id int64, seen *time.Time, now time.Time) (bool, error) {
	defer s.db.mu.Unlock()
	if err := s.begin("skip"); err != nil {
		return false, err
	}
	t, ok := s.db.tasks[id]
	if !ok || !t.Enabled || !sameTime(t.LastRunAt, seen) {
		return false, nil
	}
	t.LastRunAt = &now
	t.DateChanged = now
	return true, nil
}

func (s *session) MarkPublished(_ context.Context, id int64, now time.Time) error {
	defer s.db.mu.Unlock()
	if err := s.begin("mark_published"); err != nil {
		return err
	}
	if r := s.db.journal[id]; r != nil {
		r.PublishedAt = &now
		r.Attempts++
		if !s.db.KeepJournal {
			delete(s.db.journal, id)
		}
	}
	return nil
}

func (s *session) MarkAttempt(_ context.Context, id int64, attempts int, lastErr string) error {
	defer s.db.mu.Unlock()
	if err := s.begin("mark_attempt"); err != nil {
		return err
	}
	if r := s.db.journal[id]; r != nil {
		r.Attempts, r.LastError = attempts, lastErr
	}
	return nil
}

func (s *session) Abandon(_ context.Context, id int64, now time.Time, reason string) error {
	defer s.db.mu.Unlock()
	if err := s.begin("abandon"); err != nil {
		return err
	}
	if r := s.db.journal[id]; r != nil {
		r.AbandonedAt, r.LastError = &now, reason
	}
	return nil
}

func (s *session) Pending(context.Context) ([]store.Pending, error) {
	defer s.db.mu.Unlock()
	if err := s.begin("pending"); err != nil {
		return nil, err
	}
	var out []store.Pending
	for _, r := range s.db.journal {
		if r.PublishedAt == nil && r.AbandonedAt == nil {
			out = append(out, store.Pending{JournalID: r.ID, PeriodicTaskID: r.PeriodicTaskID, TaskID: r.TaskID,
				Name: r.Name, ClaimedAt: r.ClaimedAt, Attempts: r.Attempts, Message: r.Message})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].JournalID < out[j].JournalID })
	return out, nil
}

func (s *session) DisableExpired(_ context.Context, id int64, now time.Time) error {
	defer s.db.mu.Unlock()
	if err := s.begin("disable_expired"); err != nil {
		return err
	}
	if t := s.db.tasks[id]; t != nil && t.Enabled {
		t.Enabled, t.LastRunAt, t.DateChanged = false, nil, now
	}
	return nil
}

func (s *session) DisableOneOff(_ context.Context, id int64, now time.Time) error {
	defer s.db.mu.Unlock()
	if err := s.begin("disable_one_off"); err != nil {
		return err
	}
	if t := s.db.tasks[id]; t != nil && t.Enabled {
		t.Enabled, t.LastRunAt, t.DateChanged, t.TotalRunCount = false, nil, now, 0
	}
	s.db.bump()
	return nil
}

func (s *session) Prune(_ context.Context, before time.Time) error {
	defer s.db.mu.Unlock()
	if err := s.begin("prune"); err != nil {
		return err
	}
	for id, r := range s.db.journal {
		if (r.PublishedAt != nil && r.PublishedAt.Before(before)) || (r.AbandonedAt != nil && r.AbandonedAt.Before(before)) {
			delete(s.db.journal, id)
			delete(s.db.byTaskID, r.TaskID)
		}
	}
	return nil
}

func (s *session) Wait(ctx context.Context, d time.Duration) (bool, error) {
	if s.db.BeforeWait != nil {
		s.db.BeforeWait()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := s.begin("wait"); err != nil {
		s.db.mu.Unlock()
		return false, err
	}
	real := s.db.RealTime
	s.db.mu.Unlock()
	if !real {
		// simulation: time passes instantly; a pending notification
		// (schedule change) is delivered without advancing the clock
		select {
		case <-s.db.notify:
			return true, nil
		default:
		}
		s.db.Clock.Advance(d)
		return false, nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-s.db.notify:
		return true, nil
	case <-t.C:
		return false, nil
	}
}
