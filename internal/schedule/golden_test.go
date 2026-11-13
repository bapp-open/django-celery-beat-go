package schedule

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	_ "time/tzdata"
)

func goldenLines(t testing.TB, name string, fn func(line []byte)) int {
	t.Helper()
	f, err := os.Open(filepath.Join("..", "..", "testdata", "golden", name+".jsonl.gz"))
	if err != nil {
		t.Fatalf("golden file missing (run `make golden`): %v", err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	n := 0
	for sc.Scan() {
		fn(sc.Bytes())
		n++
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return n
}

var locCache = map[string]*time.Location{}

func loc(t testing.TB, name string) *time.Location {
	if l, ok := locCache[name]; ok {
		return l
	}
	l, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("zone %s: %v", name, err)
	}
	locCache[name] = l
	return l
}

func ts(t testing.TB, s string) time.Time {
	if !strings.HasSuffix(s, "Z") {
		s += "Z" // naive values are read as UTC, like the store does
	}
	v, err := time.Parse("2006-01-02T15:04:05.000000Z", s)
	if err != nil {
		t.Fatalf("bad time %q: %v", s, err)
	}
	return v
}

func tsp(t testing.TB, s *string) *time.Time {
	if s == nil {
		return nil
	}
	v := ts(t, *s)
	return &v
}

func TestGoldenCrontabParse(t *testing.T) {
	type rec struct {
		Max, Min int
		Spec     string
		OK       bool
		Values   []int
		Error    string
	}
	bad := 0
	n := goldenLines(t, "crontab_parse", func(line []byte) {
		var r rec
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatal(err)
		}
		got, err := ParseField(r.Spec, r.Max, r.Min)
		if (err == nil) != r.OK || (r.OK && !reflect.DeepEqual(got, r.Values)) {
			if bad++; bad < 20 {
				t.Errorf("ParseField(%q, %d, %d) = %v, %v; python: ok=%v %v %s", r.Spec, r.Max, r.Min, got, err, r.OK, r.Values, r.Error)
			}
		}
	})
	if bad > 0 || n < 1000 {
		t.Fatalf("%d of %d cases differ from Celery", bad, n)
	}
	t.Logf("%d cases match celery.schedules.crontab_parser", n)
}

type cronRec struct {
	M, H, Dow, Dom, Moy string
	TZ                  string `json:"tz"`
	Deadline            *float64
	Last, Now           string
	Error               string
	RemUs               *int64 `json:"rem_us"`
	Due                 bool
	Next                float64
}

func runCron(t *testing.T, file string, plain bool) {
	bad := 0
	n := goldenLines(t, file, func(line []byte) {
		var r cronRec
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatal(err)
		}
		fail := func(format string, a ...any) {
			if bad++; bad < 20 {
				t.Errorf("%s\n   case: %s", sprintf(format, a...), line)
			}
		}
		c, err := NewCrontab(r.M, r.H, r.Dow, r.Dom, r.Moy, loc(t, r.TZ))
		if err != nil {
			if !strings.HasPrefix(r.Error, "parse:") {
				fail("unexpected parse error %v", err)
			}
			return
		}
		if strings.HasPrefix(r.Error, "parse:") {
			fail("expected parse error")
			return
		}
		last, now := ts(t, r.Last), ts(t, r.Now)
		var (
			due  bool
			next float64
		)
		if plain {
			due, next, err = c.IsDuePlain(last, now, r.Deadline)
		} else {
			due, next, err = c.IsDueTZAware(last, now)
		}
		if r.Error != "" {
			if err == nil {
				fail("expected error %s, got due=%v next=%v", r.Error, due, next)
			}
			return
		}
		if err != nil {
			fail("unexpected error %v", err)
			return
		}
		if r.RemUs != nil {
			rem, _ := c.Remaining(last, now)
			if rem.Microseconds() != *r.RemUs {
				fail("remaining %d us, python %d us", rem.Microseconds(), *r.RemUs)
				return
			}
		}
		if due != r.Due || next != r.Next {
			fail("is_due = (%v, %v), python (%v, %v)", due, next, r.Due, r.Next)
		}
	})
	if bad > 0 || n < 1000 {
		t.Fatalf("%d of %d cases differ from Python", bad, n)
	}
	t.Logf("%d cases match Python", n)
}

func TestGoldenCrontabDueTZAware(t *testing.T) { runCron(t, "crontab_due", false) }
func TestGoldenCrontabDuePlain(t *testing.T)   { runCron(t, "crontab_plain", true) }

type entryRec struct {
	AppTZ string `json:"app_tz"`
	Now   string
	Kind  string
	Sched struct {
		Every               int64
		Period              string
		M, H, Dow, Dom, Moy string
		TZ                  string `json:"tz"`
		ClockedTime         string `json:"clocked_time"`
	}
	Enabled        bool
	OneOff         bool    `json:"one_off"`
	StartTime      *string `json:"start_time"`
	Expires        *string
	LastRunAt      *string `json:"last_run_at"`
	DateChanged    string  `json:"date_changed"`
	TotalRunCount  int64   `json:"total_run_count"`
	Error          string
	EffLast        string `json:"eff_last"`
	Due            bool
	Next           float64
	Saved          bool
	AfterEnabled   bool  `json:"after_enabled"`
	AfterCount     int64 `json:"after_count"`
	AfterNoChanges bool  `json:"after_no_changes"`
}

func runEntry(t *testing.T, file string, tzAware bool) {
	bad := 0
	n := goldenLines(t, file, func(line []byte) {
		var r entryRec
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatal(err)
		}
		fail := func(format string, a ...any) {
			if bad++; bad < 20 {
				t.Errorf("%s\n   case: %s", sprintf(format, a...), line)
			}
		}
		env := &Env{AppTZ: loc(t, r.AppTZ), TZAware: tzAware, EnableUTC: true}
		s := &Spec{Enabled: r.Enabled, OneOff: r.OneOff, StartTime: tsp(t, r.StartTime),
			Expires: tsp(t, r.Expires), LastRunAt: tsp(t, r.LastRunAt),
			DateChanged: ts(t, r.DateChanged), TotalRunCount: r.TotalRunCount}
		var err error
		switch r.Kind {
		case "interval":
			s.Kind = KindInterval
			s.Every, err = IntervalDuration(r.Sched.Every, r.Sched.Period)
		case "crontab":
			s.Kind = KindCrontab
			tz := env.AppTZ
			if tzAware {
				tz = loc(t, r.Sched.TZ)
			}
			s.Cron, err = NewCrontab(r.Sched.M, r.Sched.H, r.Sched.Dow, r.Sched.Dom, r.Sched.Moy, tz)
		case "clocked":
			s.Kind = KindClocked
			s.ClockedTime = ts(t, r.Sched.ClockedTime)
		}
		if err != nil {
			// e.g. the 31st of June: ValueError in crontab.__init__
			if !strings.HasPrefix(r.Error, "init:") {
				fail("unexpected schedule error: %v", err)
			}
			return
		}
		if strings.HasPrefix(r.Error, "init:") {
			fail("python refused the schedule (%s), Go accepted it", r.Error)
			return
		}
		if r.EffLast != "" && !s.EffectiveLastRun().Equal(ts(t, r.EffLast)) {
			fail("effective last_run_at %v, python %s", s.EffectiveLastRun(), r.EffLast)
			return
		}
		res, err := s.IsDue(env, ts(t, r.Now))
		if r.Error != "" {
			if err == nil {
				fail("expected error %s, got %+v", r.Error, res)
			}
			return
		}
		if err != nil {
			fail("unexpected error %v", err)
			return
		}
		if res.Due != r.Due || res.Next != r.Next {
			fail("is_due = (%v, %v), python (%v, %v)", res.Due, res.Next, r.Due, r.Next)
			return
		}
		want := ActNone
		if r.Saved && r.AfterNoChanges {
			want = ActDisableExpired
		} else if r.Saved {
			want = ActDisableOneOff
		}
		if res.Action != want {
			fail("action %v, python %v (saved=%v no_changes=%v)", res.Action, want, r.Saved, r.AfterNoChanges)
		}
	})
	if bad > 0 || n < 1000 {
		t.Fatalf("%d of %d cases differ from Python", bad, n)
	}
	t.Logf("%d cases match django_celery_beat ModelEntry.is_due", n)
}

func TestGoldenEntryDue(t *testing.T)      { runEntry(t, "entry_due", true) }
func TestGoldenEntryDueNaive(t *testing.T) { runEntry(t, "entry_due_naive", false) }
