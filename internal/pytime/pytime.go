// Package pytime reproduces the parts of Python's datetime semantics that
// Celery's schedule arithmetic depends on.
//
// Python adds timedeltas to aware datetimes in *wall-clock* fields and only
// then resolves the UTC offset, always with fold=0 after arithmetic
// (PEP 495; CPython Modules/_datetimemodule.c add_datetime_timedelta).
// celery/utils/time.py: remaining() and ffwd.__radd__ rely on this, and it
// is what defines behaviour in DST gaps and overlaps.
package pytime

import (
	"fmt"
	"time"
)

// Wall is a naive (zone-less) datetime with microsecond resolution, the
// equivalent of the field tuple of a Python datetime.
type Wall struct {
	Year, Month, Day         int
	Hour, Minute, Second, Us int
}

// InZone returns the wall-clock fields of instant t in loc
// (datetime.astimezone).
func InZone(t time.Time, loc *time.Location) Wall {
	t = t.In(loc)
	return Wall{t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond() / 1000}
}

// naive returns the fields as a UTC time.Time, used purely as a calendar
// calculator.
func (w Wall) naive() time.Time {
	return time.Date(w.Year, time.Month(w.Month), w.Day, w.Hour, w.Minute, w.Second, w.Us*1000, time.UTC)
}

func fromNaive(t time.Time) Wall {
	return Wall{t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond() / 1000}
}

// Add performs naive field arithmetic (datetime + timedelta).
func (w Wall) Add(d time.Duration) Wall { return fromNaive(w.naive().Add(d)) }

// AddDays adds whole days in naive arithmetic.
func (w Wall) AddDays(n int) Wall { return fromNaive(w.naive().AddDate(0, 0, n)) }

// Sub returns the naive difference w - o (what Python computes when both
// datetimes share the same tzinfo object).
func (w Wall) Sub(o Wall) time.Duration { return w.naive().Sub(o.naive()) }

// Before reports naive ordering (same-tzinfo comparison in Python).
func (w Wall) Before(o Wall) bool { return w.naive().Before(o.naive()) }

// Weekday returns Python's datetime.weekday(): Monday == 0.
func (w Wall) Weekday() int { return (int(w.naive().Weekday()) + 6) % 7 }

// IsoWeekdayMod7 returns isoweekday() % 7: Sunday == 0.
func (w Wall) IsoWeekdayMod7() int { return int(w.naive().Weekday()) }

// DaysInMonth is calendar.monthrange(year, month)[1].
func DaysInMonth(year, month int) int {
	return time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// UTC resolves the wall time in loc to an instant using fold=0:
// in a gap the offset in force before the transition is used, in an
// overlap the first occurrence is chosen (PEP 495, zoneinfo
// ZoneInfo.utcoffset / _find_trans).
func (w Wall) UTC(loc *time.Location) time.Time {
	n := w.naive()
	sec := n.Unix()
	off := offsetFold0(sec, loc)
	return time.Unix(sec-int64(off), int64(n.Nanosecond())).UTC()
}

// offsetFold0 returns the UTC offset (seconds) that zoneinfo reports for
// the naive local second count sec with fold=0.
func offsetFold0(sec int64, loc *time.Location) int {
	if loc == time.UTC {
		return 0
	}
	// Start comfortably before any instant that could map to this wall
	// time and walk forward over transitions. A transition at instant T
	// from offset a to offset b starts applying to wall times
	// >= T + max(a, b) when fold=0.
	t := time.Unix(sec-72*3600, 0).In(loc)
	for i := 0; i < 64; i++ {
		_, off := t.Zone()
		_, end := t.ZoneBounds()
		if end.IsZero() {
			return off
		}
		_, next := end.In(loc).Zone()
		thr := end.Unix() + int64(max(off, next))
		if sec < thr {
			return off
		}
		t = end.In(loc)
	}
	_, off := t.Zone()
	return off
}

// IsoFormat renders instant t in loc like datetime.isoformat() does for an
// aware datetime: microseconds are omitted when zero and the offset is
// written as +HH:MM (with :SS when the offset has seconds).
func IsoFormat(t time.Time, loc *time.Location) string {
	t = t.In(loc)
	_, off := t.Zone()
	s := fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d", t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second())
	if us := t.Nanosecond() / 1000; us != 0 {
		s += fmt.Sprintf(".%06d", us)
	}
	sign := byte('+')
	if off < 0 {
		sign = '-'
		off = -off
	}
	s += fmt.Sprintf("%c%02d:%02d", sign, off/3600, off%3600/60)
	if off%60 != 0 {
		s += fmt.Sprintf(":%02d", off%60)
	}
	return s
}

// TotalSeconds is timedelta.total_seconds(): microseconds / 10**6 as a
// correctly rounded float.
func TotalSeconds(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1e6
}

// IsoFormatNaive is datetime.isoformat() for a naive datetime.
func IsoFormatNaive(w Wall) string {
	s := fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d", w.Year, w.Month, w.Day, w.Hour, w.Minute, w.Second)
	if w.Us != 0 {
		s += fmt.Sprintf(".%06d", w.Us)
	}
	return s
}

// IsoFormatWall is isoformat() for the aware datetime with wall fields w
// in loc and fold=0 - the result of `aware + timedelta`, which may sit in
// a DST gap or overlap.
func IsoFormatWall(w Wall, loc *time.Location) string {
	off := offsetFold0(w.naive().Unix(), loc)
	return IsoFormatNaive(w) + formatOffset(off)
}

func formatOffset(off int) string {
	sign := byte('+')
	if off < 0 {
		sign = '-'
		off = -off
	}
	s := fmt.Sprintf("%c%02d:%02d", sign, off/3600, off%3600/60)
	if off%60 != 0 {
		s += fmt.Sprintf(":%02d", off%60)
	}
	return s
}
