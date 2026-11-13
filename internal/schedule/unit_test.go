package schedule

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestParseFieldExamples(t *testing.T) {
	// the examples in the celery.schedules.crontab_parser docstring
	for _, c := range []struct {
		spec     string
		max, min int
		want     []int
	}{
		{"*/15", 60, 0, []int{0, 15, 30, 45}},
		{"*/4", 24, 0, []int{0, 4, 8, 12, 16, 20}},
		{"*", 7, 0, []int{0, 1, 2, 3, 4, 5, 6}},
		{"*/3", 31, 1, []int{1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31}},
		{"*/2", 12, 1, []int{1, 3, 5, 7, 9, 11}},
		{"2-12/2", 12, 1, []int{2, 4, 6, 8, 10, 12}},
		{"1,13,30-45,50-59/2", 60, 0, []int{1, 13, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 50, 52, 54, 56, 58}},
		{"mon-fri", 7, 0, []int{1, 2, 3, 4, 5}},
		{"fri-mon", 7, 0, []int{0, 1, 5, 6}}, // wraps around
		{"sunday", 7, 0, []int{0}},
		{"jan-mar", 12, 1, []int{1, 2, 3}},
		{"22-2", 24, 0, []int{0, 1, 2, 22, 23}},
		{"007", 60, 0, []int{7}},
	} {
		got, err := ParseField(c.spec, c.max, c.min)
		if err != nil || !reflect.DeepEqual(got, c.want) {
			t.Errorf("ParseField(%q) = %v, %v; want %v", c.spec, got, err, c.want)
		}
	}
	for _, c := range []struct {
		spec     string
		max, min int
		want     error
	}{
		{"", 60, 0, ErrParse}, {"1,,2", 60, 0, ErrParse}, {"*/", 60, 0, ErrParse},
		{"60", 60, 0, ErrValue}, {"0", 31, 1, ErrValue}, {"7", 7, 0, ErrValue},
		{"*/0", 60, 0, ErrValue}, {"abc", 60, 0, ErrValue}, {"-1", 60, 0, ErrParse},
		{"1.5", 60, 0, ErrValue}, {"L", 31, 1, ErrValue},
	} {
		if _, err := ParseField(c.spec, c.max, c.min); !errors.Is(err, c.want) {
			t.Errorf("ParseField(%q) error = %v, want %v", c.spec, err, c.want)
		}
	}
}

func at(s string) time.Time {
	v, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return v
}

func nextRun(t *testing.T, c *Crontab, last string) string {
	t.Helper()
	l := at(last)
	rem, err := c.Remaining(l, l)
	if err != nil {
		t.Fatal(err)
	}
	return l.Add(rem).In(c.TZ).Format("Mon 2006-01-02 15:04 MST")
}

func TestCrontabDayOfWeekAndDayOfMonthMustBothMatch(t *testing.T) {
	// cron(8) runs when EITHER day field matches; Celery requires BOTH
	// (celery/schedules.py: crontab._delta_to_next).
	c, err := NewCrontab("0", "0", "mon", "13", "*", time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if got := nextRun(t, c, "2024-01-01T00:00:00Z"); got != "Mon 2024-05-13 00:00 UTC" {
		t.Errorf("next Monday the 13th: %s", got)
	}
}

func TestCrontabDSTGapAndOverlap(t *testing.T) {
	ny, _ := time.LoadLocation("America/New_York")
	c, _ := NewCrontab("30", "2", "*", "*", "*", ny)
	// 02:30 does not exist on 2024-03-10: Python resolves the wall time
	// with the pre-transition offset, i.e. it fires at 03:30 EDT.
	if got := nextRun(t, c, "2024-03-09T12:00:00Z"); got != "Sun 2024-03-10 03:30 EDT" {
		t.Errorf("gap: %s", got)
	}
	c, _ = NewCrontab("30", "1", "*", "*", "*", ny)
	// 01:30 happens twice on 2024-11-03: it fires once, at the first one.
	if got := nextRun(t, c, "2024-11-02T12:00:00Z"); got != "Sun 2024-11-03 01:30 EDT" {
		t.Errorf("overlap: %s", got)
	}
	if got := nextRun(t, c, "2024-11-03T05:30:00Z"); got != "Mon 2024-11-04 01:30 EST" {
		t.Errorf("after overlap: %s", got)
	}
}

func TestFirstRunWithNullLastRunAt(t *testing.T) {
	env := &Env{AppTZ: time.UTC, TZAware: true, EnableUTC: true}
	changed := at("2024-06-01T10:00:00Z")
	s := &Spec{Kind: KindInterval, Every: time.Hour, Enabled: true, DateChanged: changed}
	// ModelEntry.__init__: last_run_at defaults to date_changed
	if r, _ := s.IsDue(env, at("2024-06-01T10:59:59Z")); r.Due || r.Next != 1 {
		t.Errorf("before first interval: %+v", r)
	}
	if r, _ := s.IsDue(env, at("2024-06-01T11:00:00Z")); !r.Due || !r.DueAt.Equal(at("2024-06-01T11:00:00Z")) {
		t.Errorf("at first interval: %+v", r)
	}
	// with start_time the reference moves 30 years back: due at start_time
	start := at("2024-06-01T10:30:00Z")
	s.StartTime = &start
	if r, _ := s.IsDue(env, at("2024-06-01T10:29:59.5Z")); r.Due || r.Next != 1 {
		t.Errorf("before start_time: %+v (delay is ceil'ed seconds)", r)
	}
	if r, _ := s.IsDue(env, start); !r.Due {
		t.Errorf("at start_time: %+v", r)
	}
	if got := s.EffectiveLastRun(); !got.Equal(changed.Add(-365 * 30 * 24 * time.Hour)) {
		t.Errorf("effective last run %v", got)
	}
}

func TestEntrySideEffectsAndRefusals(t *testing.T) {
	env := &Env{AppTZ: time.UTC, TZAware: true, EnableUTC: true}
	now := at("2024-06-01T10:00:00Z")
	base := Spec{Kind: KindInterval, Every: time.Minute, Enabled: true, DateChanged: now.Add(-time.Hour)}

	s := base
	s.Enabled = false
	if r, _ := s.IsDue(env, now); r.Due || r.Next != 5 {
		t.Errorf("disabled: %+v", r)
	}
	s = base
	s.Expires = &now
	if r, _ := s.IsDue(env, now); r.Action != ActDisableExpired || r.Next != NeverCheck {
		t.Errorf("expired: %+v", r)
	}
	s = base
	s.OneOff, s.TotalRunCount = true, 1
	if r, _ := s.IsDue(env, now); r.Action != ActDisableOneOff || r.Due {
		t.Errorf("one-off done: %+v", r)
	}
	cron, _ := NewCrontab("0", "4", "*", "*", "*", time.UTC)
	s = Spec{Kind: KindCrontab, Cron: cron, Enabled: true, DateChanged: now}
	if _, err := s.IsDue(&Env{AppTZ: time.UTC, TZAware: true, EnableUTC: false}, now); !errors.Is(err, ErrUnsupported) {
		t.Errorf("enable_utc=False with a UTC crontab must be refused, got %v", err)
	}
	feb31, _ := NewCrontab("0", "0", "*", "31", "2", time.UTC)
	s = Spec{Kind: KindCrontab, Cron: feb31, Enabled: true, DateChanged: now}
	if _, err := s.IsDue(env, now); !errors.Is(err, ErrRollover) {
		t.Errorf("February 31st: %v", err)
	}
	for _, bad := range []struct {
		every  int64
		period string
	}{{0, "seconds"}, {-5, "minutes"}, {1, "weeks"}, {1 << 62, "days"}} {
		if _, err := IntervalDuration(bad.every, bad.period); !errors.Is(err, ErrUnsupported) {
			t.Errorf("IntervalDuration(%d, %q) = %v", bad.every, bad.period, err)
		}
	}
}

func TestCronStartingDeadline(t *testing.T) {
	c, _ := NewCrontab("0", "*", "*", "*", "*", time.UTC)
	last, now := at("2024-06-01T10:00:00Z"), at("2024-06-01T13:20:00Z")
	due, _, _ := c.IsDuePlain(last, now, nil)
	if !due {
		t.Error("no deadline: missed runs are due")
	}
	short, long := 600.0, 3600.0
	if due, next, _ := c.IsDuePlain(last, now, &short); due || next != 2400 {
		t.Errorf("deadline passed 20 min ago: due=%v next=%v", due, next)
	}
	if due, _, _ := c.IsDuePlain(last, now, &long); !due {
		t.Error("within deadline: due")
	}
}

func FuzzParseField(f *testing.F) {
	for _, s := range []string{"*", "*/5", "1-5", "1-5/2", "mon-fri", "jan", "0,15,30", "", "*/0", "٣", "1__0", "-", "5-1"} {
		f.Add(s, uint8(0))
	}
	fields := [][2]int{{60, 0}, {24, 0}, {7, 0}, {31, 1}, {12, 1}}
	f.Fuzz(func(t *testing.T, spec string, which uint8) {
		fl := fields[int(which)%len(fields)]
		vals, err := ParseField(spec, fl[0], fl[1])
		if err != nil {
			if !errors.Is(err, ErrParse) && !errors.Is(err, ErrValue) {
				t.Fatalf("unclassified error %v", err)
			}
			return
		}
		if len(vals) == 0 {
			t.Fatalf("%q parsed to an empty set", spec)
		}
		for i, v := range vals {
			if v < fl[1] || v >= fl[0]+fl[1] || (i > 0 && vals[i-1] >= v) {
				t.Fatalf("%q -> %v: out of range or unsorted", spec, vals)
			}
		}
	})
}

func FuzzCrontabIsDue(f *testing.F) {
	f.Add("*/5", "*", "*", "*", "*", int64(1700000000), int64(3600))
	f.Add("0", "0", "mon", "13", "*", int64(1710000000), int64(86400*40))
	f.Add("30", "2", "*", "29", "2", int64(1720000000), int64(-60))
	ny, _ := time.LoadLocation("America/New_York")
	f.Fuzz(func(t *testing.T, m, h, dow, dom, moy string, nowSec, ago int64) {
		if nowSec < 0 || nowSec > 4e9 || ago > 4e8 || ago < -4e8 {
			return
		}
		c, err := NewCrontab(m, h, dow, dom, moy, ny)
		if err != nil {
			return
		}
		now := time.Unix(nowSec, 0).UTC()
		due, next, err := c.IsDueTZAware(now.Add(-time.Duration(ago)*time.Second), now)
		if err != nil {
			return
		}
		if next < 0 || (!due && next == 0) {
			t.Fatalf("is_due = (%v, %v)", due, next)
		}
	})
}
