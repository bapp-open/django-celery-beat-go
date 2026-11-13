// Package shadow compares the dispatch log of Python beat with the log of
// gobeat running in dry-run mode beside it.
package shadow

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Event is one dispatch.
type Event struct {
	Name string
	At   time.Time
}

// celery/beat.py: Scheduler.apply_entry logs
// "Scheduler: Sending due task %s (%s)" with Celery's default log format
// "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s".
var beatLine = regexp.MustCompile(`^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{3}): \w+/[^\]]*\] Scheduler: Sending due task (.*) \(([^()]*)\)\s*$`)

// ParsePython reads a `celery beat` log (default format, loglevel INFO).
// loc is the zone the log timestamps are written in.
func ParsePython(r io.Reader, loc *time.Location) ([]Event, error) {
	var out []Event
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<16), 1<<24)
	for sc.Scan() {
		m := beatLine.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		t, err := time.ParseInLocation("2006-01-02 15:04:05.000", m[1]+"."+m[2], loc)
		if err != nil {
			return nil, err
		}
		out = append(out, Event{Name: m[3], At: t.UTC()})
	}
	return out, sc.Err()
}

// ParseGobeat reads gobeat's JSON log and returns its "dispatch" events.
func ParseGobeat(r io.Reader) ([]Event, error) {
	var out []Event
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<16), 1<<24)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 || line[0] != '{' {
			continue
		}
		var rec struct {
			Time time.Time `json:"time"`
			Msg  string    `json:"msg"`
			Name string    `json:"name"`
		}
		if json.Unmarshal(line, &rec) != nil || rec.Msg != "dispatch" {
			continue
		}
		out = append(out, Event{Name: rec.Name, At: rec.Time.UTC()})
	}
	return out, sc.Err()
}

// Diff is the outcome of a comparison.
type Diff struct {
	Matched    int
	OnlyPython []Event
	OnlyGobeat []Event
	MaxSkew    time.Duration
}

// Compare pairs events of the same task whose times differ by at most
// tolerance. Events outside [from, to] are ignored (start-up and shutdown
// edges, where one side was not yet running).
func Compare(py, gb []Event, tolerance time.Duration, from, to time.Time) Diff {
	group := func(evs []Event) map[string][]time.Time {
		g := map[string][]time.Time{}
		for _, e := range evs {
			if (!from.IsZero() && e.At.Before(from)) || (!to.IsZero() && e.At.After(to)) {
				continue
			}
			g[e.Name] = append(g[e.Name], e.At)
		}
		for _, ts := range g {
			sort.Slice(ts, func(i, j int) bool { return ts[i].Before(ts[j]) })
		}
		return g
	}
	p, g := group(py), group(gb)
	names := map[string]bool{}
	for n := range p {
		names[n] = true
	}
	for n := range g {
		names[n] = true
	}
	var d Diff
	for n := range names {
		a, b := p[n], g[n]
		i, j := 0, 0
		for i < len(a) && j < len(b) {
			skew := a[i].Sub(b[j])
			if skew < 0 {
				skew = -skew
			}
			switch {
			case skew <= tolerance:
				d.Matched++
				if skew > d.MaxSkew {
					d.MaxSkew = skew
				}
				i++
				j++
			case a[i].Before(b[j]):
				d.OnlyPython = append(d.OnlyPython, Event{n, a[i]})
				i++
			default:
				d.OnlyGobeat = append(d.OnlyGobeat, Event{n, b[j]})
				j++
			}
		}
		for ; i < len(a); i++ {
			d.OnlyPython = append(d.OnlyPython, Event{n, a[i]})
		}
		for ; j < len(b); j++ {
			d.OnlyGobeat = append(d.OnlyGobeat, Event{n, b[j]})
		}
	}
	byTime := func(e []Event) {
		sort.Slice(e, func(i, j int) bool {
			if !e[i].At.Equal(e[j].At) {
				return e[i].At.Before(e[j].At)
			}
			return e[i].Name < e[j].Name
		})
	}
	byTime(d.OnlyPython)
	byTime(d.OnlyGobeat)
	return d
}

// Main implements `gobeat shadow-diff`.
func Main(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("shadow-diff", flag.ContinueOnError)
	fs.SetOutput(stderr)
	pyPath := fs.String("python-log", "", "celery beat log file (default format, --loglevel=INFO)")
	gbPath := fs.String("gobeat-log", "", "gobeat log file (JSON, GOBEAT_DRY_RUN=1)")
	tol := fs.Duration("tolerance", 2*time.Second, "maximum time difference for two dispatches to match")
	tz := fs.String("python-tz", "UTC", "time zone of the timestamps in the celery beat log")
	trim := fs.Duration("trim", 0, "ignore dispatches closer than this to either end of the overlapping period")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *pyPath == "" || *gbPath == "" {
		fmt.Fprintln(stderr, "shadow-diff: --python-log and --gobeat-log are required")
		return 2
	}
	loc, err := time.LoadLocation(*tz)
	if err != nil {
		fmt.Fprintln(stderr, "shadow-diff:", err)
		return 2
	}
	read := func(path string, parse func(io.Reader) ([]Event, error)) ([]Event, error) {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		return parse(f)
	}
	py, err := read(*pyPath, func(r io.Reader) ([]Event, error) { return ParsePython(r, loc) })
	if err != nil {
		fmt.Fprintln(stderr, "shadow-diff:", err)
		return 2
	}
	gb, err := read(*gbPath, ParseGobeat)
	if err != nil {
		fmt.Fprintln(stderr, "shadow-diff:", err)
		return 2
	}
	if len(py) == 0 || len(gb) == 0 {
		fmt.Fprintf(stderr, "shadow-diff: nothing to compare (python: %d dispatches, gobeat: %d)\n", len(py), len(gb))
		return 2
	}
	// Only the period both logs cover can be compared.
	span := func(e []Event) (lo, hi time.Time) {
		lo, hi = e[0].At, e[0].At
		for _, x := range e {
			if x.At.Before(lo) {
				lo = x.At
			}
			if x.At.After(hi) {
				hi = x.At
			}
		}
		return
	}
	plo, phi := span(py)
	glo, ghi := span(gb)
	from, to := plo, phi
	if glo.After(from) {
		from = glo
	}
	if ghi.Before(to) {
		to = ghi
	}
	from, to = from.Add(*trim), to.Add(-*trim)
	d := Compare(py, gb, *tol, from, to)
	fmt.Fprintf(stdout, "compared %s .. %s (tolerance %s)\n", from.Format(time.RFC3339), to.Format(time.RFC3339), *tol)
	fmt.Fprintf(stdout, "matched: %d   max skew: %s   only python: %d   only gobeat: %d\n",
		d.Matched, d.MaxSkew.Round(time.Millisecond), len(d.OnlyPython), len(d.OnlyGobeat))
	for _, e := range d.OnlyPython {
		fmt.Fprintf(stdout, "- python only  %s  %s\n", e.At.Format("2006-01-02T15:04:05.000Z"), e.Name)
	}
	for _, e := range d.OnlyGobeat {
		fmt.Fprintf(stdout, "+ gobeat only  %s  %s\n", e.At.Format("2006-01-02T15:04:05.000Z"), e.Name)
	}
	if len(d.OnlyPython)+len(d.OnlyGobeat) > 0 {
		return 1
	}
	fmt.Fprintln(stdout, strings.Repeat("=", 8), "logs agree")
	return 0
}
