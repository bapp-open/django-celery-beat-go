package scheduler

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/bapp-open/django-celery-beat-go/internal/store"
)

// soakDoc is written by tests/differential/gen_soak.py: the task set and
// the number of times real Python beat dispatched each task.
type soakDoc struct {
	T0, End string
	Tasks   int
	Days    float64
	Total   int             `json:"total_dispatches"`
	Counts  map[string]int  `json:"counts"`
	Enabled map[string]bool `json:"final_enabled"`
	Specs   []struct {
		ID          int64
		Name, Kind  string
		Every       int64
		Period      string
		M, H        string
		Dom, Moy    string
		Dow, TZ     string
		ClockedTime *string `json:"clocked_time"`
		OneOff      bool    `json:"one_off"`
		StartTime   *string `json:"start_time"`
		Expires     *string
		LastRunAt   *string `json:"last_run_at"`
		DateChanged string  `json:"date_changed"`
	}
}

func loadSoak(t *testing.T, name string) *soakDoc {
	f, err := os.Open(filepath.Join("..", "..", "testdata", "soak", name))
	if err != nil {
		t.Fatalf("soak reference missing (run `make soak-golden`): %v", err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var d soakDoc
	if err := json.NewDecoder(gz).Decode(&d); err != nil {
		t.Fatal(err)
	}
	return &d
}

func soakTime(t *testing.T, s string) time.Time {
	v, err := time.Parse("2006-01-02T15:04:05.000000Z", s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func soakTimeP(t *testing.T, s *string) *time.Time {
	if s == nil {
		return nil
	}
	v := soakTime(t, *s)
	return &v
}

// soakTasks builds `copies` replicas of the reference task set; replica k
// of task id i gets id i + k*1_000_000.
func soakTasks(t *testing.T, d *soakDoc, copies int) []store.Task {
	out := make([]store.Task, 0, len(d.Specs)*copies)
	for k := 0; k < copies; k++ {
		for _, s := range d.Specs {
			task := store.Task{ID: s.ID + int64(k)*1_000_000, Name: fmt.Sprintf("%s-%d", s.Name, k), Task: "soak.task",
				Args: "[]", Kwargs: "{}", Enabled: true, OneOff: s.OneOff,
				StartTime: soakTimeP(t, s.StartTime), Expires: soakTimeP(t, s.Expires),
				LastRunAt: soakTimeP(t, s.LastRunAt), DateChanged: soakTime(t, s.DateChanged)}
			switch s.Kind {
			case "interval":
				every, period := s.Every, s.Period
				task.IntervalEvery, task.IntervalPeriod = &every, &period
			case "crontab":
				task.Cron = &store.CronRow{Minute: s.M, Hour: s.H, DayOfWeek: s.Dow, DayOfMonth: s.Dom, MonthOfYear: s.Moy, Timezone: s.TZ}
			case "clocked":
				task.ClockedTime = soakTimeP(t, s.ClockedTime)
			}
			out = append(out, task)
		}
	}
	return out
}

// runSoak simulates the whole period and returns dispatch counts by
// periodic task id plus heap samples taken once per simulated day.
func runSoak(t *testing.T, d *soakDoc, copies int) (map[int64]int, []uint64, *world) {
	w := newWorld(t, "UTC")
	start, end := soakTime(t, d.T0), soakTime(t, d.End)
	w.clk.Set(start)
	w.db.KeepJournal = false // published rows are pruned; keep the run O(1)
	w.broker.keep = false
	w.opts.Log = nil
	w.db.PutQuiet(soakTasks(t, d, copies))

	counts := map[int64]int{}
	w.opts.Fault = func(point string) {}
	// count claims through the store hook-free path: every publish is one
	// claimed run, identified by the periodic task name
	byName := map[string]int64{}
	for _, task := range soakTasks(t, d, copies) {
		byName[task.Name] = task.ID
	}
	var heaps []uint64
	nextSample := start.Add(24 * time.Hour)
	err := w.runWith(w.opts, end, func(now time.Time) {
		if !now.Before(nextSample) {
			nextSample = nextSample.Add(24 * time.Hour)
			runtime.GC()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			heaps = append(heaps, ms.HeapAlloc)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	w.broker.mu.Lock()
	for name, n := range w.broker.count {
		counts[byName[name]] = n
	}
	w.broker.mu.Unlock()
	return counts, heaps, w
}

func compareSoak(t *testing.T, d *soakDoc, counts map[int64]int, copies int, w *world) {
	t.Helper()
	bad, total := 0, 0
	for k := 0; k < copies; k++ {
		for _, s := range d.Specs {
			id := s.ID + int64(k)*1_000_000
			want := d.Counts[fmt.Sprint(s.ID)]
			got := counts[id]
			total += got
			if got != want {
				if bad++; bad <= 10 {
					t.Errorf("task %d (%s %+v): gobeat dispatched %d times, Python beat %d", id, s.Kind, s, got, want)
				}
			}
			row, _ := w.db.Get(id)
			if row.Enabled != d.Enabled[fmt.Sprint(s.ID)] {
				if bad++; bad <= 10 {
					t.Errorf("task %d: enabled=%v at the end, Python beat left it %v", id, row.Enabled, d.Enabled[fmt.Sprint(s.ID)])
				}
			}
		}
	}
	if bad > 0 {
		t.Fatalf("%d differences from Python beat", bad)
	}
	if total != d.Total*copies {
		t.Fatalf("total dispatches %d, Python beat %d", total, d.Total*copies)
	}
	t.Logf("%d tasks, %g simulated days: %d dispatches, identical to Python beat for every task", len(d.Specs)*copies, d.Days, total)
}

// TestSoakSmallMatchesPythonBeat is the reference CI regenerates on every
// run (tests/differential/gen_soak.py --tasks 100 --days 3).
func TestSoakSmallMatchesPythonBeat(t *testing.T) {
	d := loadSoak(t, "soak_100_3d.json.gz")
	counts, _, w := runSoak(t, d, 1)
	compareSoak(t, d, counts, 1, w)
}

// TestSoak30DaysMatchesPythonBeat replays the committed 30-day reference
// (1,000 tasks, real DatabaseScheduler, about an hour of Python CPU).
func TestSoak30DaysMatchesPythonBeat(t *testing.T) {
	d := loadSoak(t, "soak_1000_30d.json.gz")
	counts, _, w := runSoak(t, d, 1)
	compareSoak(t, d, counts, 1, w)
}
