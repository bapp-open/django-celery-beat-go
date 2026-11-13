package schedule

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/bapp-open/django-celery-beat-go/internal/pytime"
)

// ErrRollover mirrors the RuntimeError raised by crontab._delta_to_next
// when no matching date exists (for example day 31 of February).
var ErrRollover = errors.New("unable to rollover, time specification is probably invalid")

// Crontab is an expanded crontab schedule.
// Port of celery/schedules.py: crontab.
type Crontab struct {
	Minute, Hour, DayOfWeek, DayOfMonth, MonthOfYear []int // sorted sets
	// allDomMoy is `_orig_day_of_month == '*' and _orig_month_of_year == '*'`
	// (a comparison of the *raw* field text, as in Python).
	allDomMoy bool
	// TZ is the zone the fields are interpreted in: the CrontabSchedule
	// timezone (TzAwareCrontab.tz) or app.timezone for a plain crontab.
	TZ *time.Location
}

// cronDaysInMonth is celery/schedules.py: CRON_DAYS_IN_MONTH.
var cronDaysInMonth = [13]int{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

// NewCrontab parses the five fields exactly as crontab.__init__ does.
func NewCrontab(minute, hour, dayOfWeek, dayOfMonth, monthOfYear string, tz *time.Location) (*Crontab, error) {
	c := &Crontab{TZ: tz, allDomMoy: dayOfMonth == "*" && monthOfYear == "*"}
	var err error
	if c.Hour, err = ParseField(hour, 24, 0); err != nil {
		return nil, err
	}
	if c.Minute, err = ParseField(minute, 60, 0); err != nil {
		return nil, err
	}
	if c.DayOfWeek, err = ParseField(dayOfWeek, 7, 0); err != nil {
		return nil, err
	}
	if c.DayOfMonth, err = ParseField(dayOfMonth, 31, 1); err != nil {
		return nil, err
	}
	if c.MonthOfYear, err = ParseField(monthOfYear, 12, 1); err != nil {
		return nil, err
	}
	// crontab.__init__: "day_of_month ... never occurs in month_of_year"
	possible := false
	for _, m := range c.MonthOfYear {
		possible = possible || c.DayOfMonth[0] <= cronDaysInMonth[m]
	}
	if !possible {
		return nil, fmt.Errorf("%w: day_of_month %q never occurs in month_of_year %q", ErrValue, dayOfMonth, monthOfYear)
	}
	return c, nil
}

func has(set []int, v int) bool {
	i := sort.SearchInts(set, v)
	return i < len(set) && set[i] == v
}

// firstGreater returns min(x for x in set if x > v).
func firstGreater(set []int, v int) (int, bool) {
	i := sort.SearchInts(set, v+1)
	if i < len(set) {
		return set[i], true
	}
	return 0, false
}

// ffwd is celery/utils/time.py: ffwd. Nil-able fields use -1 for "None".
type ffwd struct {
	year, month, day    int // 0 == None (Python uses `self.year or other.year`)
	weeks               int
	weekday             int // -1 == None
	hour, minute        int // -1 == None
	second, microsecond int // -1 == None
}

// apply is ffwd.__radd__: field replacement followed by naive day
// arithmetic.
func (f ffwd) apply(o pytime.Wall) pytime.Wall {
	year, month := o.Year, o.Month
	if f.year != 0 {
		year = f.year
	}
	if f.month != 0 {
		month = f.month
	}
	day := o.Day
	if f.day != 0 {
		day = f.day
	}
	if dim := pytime.DaysInMonth(year, month); day > dim {
		day = dim
	}
	ret := o
	ret.Year, ret.Month, ret.Day = year, month, day
	if f.hour >= 0 {
		ret.Hour = f.hour
	}
	if f.minute >= 0 {
		ret.Minute = f.minute
	}
	if f.second >= 0 {
		ret.Second = f.second
	}
	if f.microsecond >= 0 {
		ret.Us = f.microsecond
	}
	if f.weekday >= 0 {
		ret = ret.AddDays((7 - ret.Weekday() + f.weekday) % 7)
	}
	return ret.AddDays(f.weeks * 7)
}

// remainingDelta ports crontab.remaining_delta. last is the wall-clock
// reading of last_run_at in lastTZ (normally c.TZ; ModelEntry's
// start_time path passes app.timezone), now is the current instant.
func (c *Crontab) remainingDelta(last pytime.Wall, lastTZ *time.Location, now time.Time) (ffwd, error) {
	dowNum := last.IsoWeekdayMod7()
	executeThisDate := has(c.MonthOfYear, last.Month) && has(c.DayOfMonth, last.Day) && has(c.DayOfWeek, dowNum)
	executeThisHour := executeThisDate &&
		has(c.Hour, last.Hour) && last.Minute < c.Minute[len(c.Minute)-1]

	if executeThisHour {
		nextMinute, _ := firstGreater(c.Minute, last.Minute)
		return ffwd{weekday: -1, hour: -1, minute: nextMinute, second: 0, microsecond: 0}, nil
	}
	nextMinute := c.Minute[0]
	executeToday := executeThisDate && last.Hour < c.Hour[len(c.Hour)-1]
	if executeToday {
		nextHour, _ := firstGreater(c.Hour, last.Hour)
		return ffwd{weekday: -1, hour: nextHour, minute: nextMinute, second: 0, microsecond: 0}, nil
	}
	nextHour := c.Hour[0]
	if c.allDomMoy {
		nextDay, ok := firstGreater(c.DayOfWeek, dowNum)
		if !ok {
			nextDay = c.DayOfWeek[0]
		}
		weeks := 0
		if nextDay == dowNum {
			weeks = 1
		}
		return ffwd{weeks: weeks, weekday: ((nextDay-1)%7 + 7) % 7, hour: nextHour, minute: nextMinute, second: 0, microsecond: 0}, nil
	}
	return c.deltaToNext(last, lastTZ, nextHour, nextMinute)
}

// deltaToNext ports crontab._delta_to_next: used when day_of_month and/or
// month_of_year restrict the schedule. A date must satisfy day_of_month,
// month_of_year AND day_of_week (Celery does not implement cron's
// "either" rule).
func (c *Crontab) deltaToNext(last pytime.Wall, lastTZ *time.Location, nextHour, nextMinute int) (ffwd, error) {
	days, months := c.DayOfMonth, c.MonthOfYear
	year := last.Year
	var dom, moy int

	sameZone := lastTZ == c.TZ || lastTZ.String() == c.TZ.String()
	var lastUTC time.Time
	if !sameZone {
		lastUTC = last.UTC(lastTZ)
	}
	isBeforeLastRun := func(y, m, d int) bool {
		cand := pytime.Wall{Year: y, Month: m, Day: d, Hour: nextHour, Minute: nextMinute}
		if sameZone {
			// same tzinfo object: Python compares wall-clock fields.
			return cand.Before(last)
		}
		return cand.UTC(c.TZ).Before(lastUTC)
	}
	rollOver := func() error {
		for i := 0; i < 2000; i++ {
			flag := dom == len(days) ||
				days[dom] > pytime.DaysInMonth(year, months[moy]) ||
				isBeforeLastRun(year, months[moy], days[dom])
			if !flag {
				return nil
			}
			dom = 0
			moy++
			if moy == len(months) {
				moy = 0
				year++
			}
		}
		return ErrRollover
	}

	if has(months, last.Month) {
		dom = sort.SearchInts(days, last.Day+1)   // bisect(days, last.day)
		moy = sort.SearchInts(months, last.Month) // bisect_left
	} else {
		dom = 0
		moy = sort.SearchInts(months, last.Month+1) // bisect
		if moy == len(months) {
			moy = 0
		}
	}
	if err := rollOver(); err != nil {
		return ffwd{}, err
	}
	for guard := 0; ; guard++ {
		if guard > 1_000_000 {
			return ffwd{}, ErrRollover
		}
		th := pytime.Wall{Year: year, Month: months[moy], Day: days[dom]}
		if has(c.DayOfWeek, th.IsoWeekdayMod7()) {
			break
		}
		dom++
		if err := rollOver(); err != nil {
			return ffwd{}, err
		}
	}
	return ffwd{year: year, month: months[moy], day: days[dom], weekday: -1,
		hour: nextHour, minute: nextMinute, second: 0, microsecond: 0}, nil
}

// remainingWall is crontab.remaining_estimate for a wall-clock last run:
// remaining(last, delta, now) from celery/utils/time.py.
func (c *Crontab) remainingWall(last pytime.Wall, lastTZ *time.Location, now time.Time) (time.Duration, error) {
	d, err := c.remainingDelta(last, lastTZ, now)
	if err != nil {
		return 0, err
	}
	end := d.apply(last)
	return end.UTC(lastTZ).Sub(now), nil
}

// Remaining returns remaining_estimate(last_run_at) with last_run_at
// converted to the crontab's zone, i.e. the (possibly negative) time until
// the first matching minute after last.
func (c *Crontab) Remaining(last, now time.Time) (time.Duration, error) {
	return c.remainingWall(pytime.InZone(last, c.TZ), c.TZ, now)
}

func nonNeg(d time.Duration) float64 {
	if d < 0 {
		return 0
	}
	return pytime.TotalSeconds(d)
}

// IsDueTZAware ports django_celery_beat/tzcrontab.py:
// TzAwareCrontab.is_due. beat_cron_starting_deadline is not consulted
// there, and so not here either.
func (c *Crontab) IsDueTZAware(last, now time.Time) (bool, float64, error) {
	rem, err := c.Remaining(last, now)
	if err != nil {
		return false, 0, err
	}
	if rem > 0 {
		return false, pytime.TotalSeconds(rem), nil
	}
	rem, err = c.Remaining(now, now)
	if err != nil {
		return false, 0, err
	}
	return true, nonNeg(rem), nil
}

// IsDuePlain ports celery/schedules.py: crontab.is_due, used when
// DJANGO_CELERY_BEAT_TZ_AWARE is False. c.TZ must be app.timezone and
// deadline is beat_cron_starting_deadline (nil when unset).
func (c *Crontab) IsDuePlain(last, now time.Time, deadline *float64) (bool, float64, error) {
	lw := pytime.InZone(last, c.TZ)
	remDelta, err := c.remainingWall(lw, c.TZ, now)
	if err != nil {
		return false, 0, err
	}
	rem := nonNeg(remDelta)
	due := remDelta <= 0
	passed := false
	if deadline != nil && remDelta < 0 {
		// "deadline_since = now - deadline - 1µs": if the next run after
		// that instant is still in the future, every missed run is
		// older than the deadline.
		since := now.Add(-time.Duration(math.Round(*deadline*1e6))*time.Microsecond - time.Microsecond)
		r, err := c.Remaining(since, now)
		if err != nil {
			return false, 0, err
		}
		if r > 0 {
			passed, due = true, false
		}
	}
	if due || passed {
		r, err := c.Remaining(now, now)
		if err != nil {
			return false, 0, err
		}
		rem = nonNeg(r)
	}
	return due, rem, nil
}
