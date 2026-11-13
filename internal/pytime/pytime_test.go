package pytime

import (
	"testing"
	"time"
	_ "time/tzdata"
)

func mustLoc(t *testing.T, name string) *time.Location {
	l, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestWallToUTCFold0(t *testing.T) {
	ny := mustLoc(t, "America/New_York")
	for _, c := range []struct {
		name string
		w    Wall
		want string
	}{
		// spring forward 2024-03-10: 02:30 does not exist; fold=0 uses
		// the offset before the transition (EST)
		{"gap", Wall{2024, 3, 10, 2, 30, 0, 0}, "2024-03-10T07:30:00Z"},
		{"before gap", Wall{2024, 3, 10, 1, 59, 59, 0}, "2024-03-10T06:59:59Z"},
		{"after gap", Wall{2024, 3, 10, 3, 0, 0, 0}, "2024-03-10T07:00:00Z"},
		// fall back 2024-11-03: 01:30 happens twice; fold=0 is the first (EDT)
		{"overlap", Wall{2024, 11, 3, 1, 30, 0, 0}, "2024-11-03T05:30:00Z"},
		{"after overlap", Wall{2024, 11, 3, 2, 0, 0, 0}, "2024-11-03T07:00:00Z"},
		{"plain", Wall{2024, 7, 1, 12, 0, 0, 123456}, "2024-07-01T16:00:00.123456Z"},
	} {
		if got := c.w.UTC(ny).Format(time.RFC3339Nano); got != c.want {
			t.Errorf("%s: %v -> %s, want %s", c.name, c.w, got, c.want)
		}
	}
	// a zone that skipped a whole day (Samoa, 2011-12-30)
	apia := mustLoc(t, "Pacific/Apia")
	if got := (Wall{2011, 12, 30, 12, 0, 0, 0}).UTC(apia).Format(time.RFC3339); got != "2011-12-30T22:00:00Z" {
		t.Errorf("Apia skipped day: %s", got)
	}
	if got := (Wall{2024, 1, 1, 0, 0, 0, 0}).UTC(time.UTC); !got.Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("UTC: %v", got)
	}
}

func TestWallArithmetic(t *testing.T) {
	w := Wall{2024, 1, 31, 23, 59, 59, 999999}
	if got := w.Add(time.Microsecond); got != (Wall{2024, 2, 1, 0, 0, 0, 0}) {
		t.Errorf("Add: %v", got)
	}
	if got := w.AddDays(30); got != (Wall{2024, 3, 1, 23, 59, 59, 999999}) {
		t.Errorf("AddDays: %v", got)
	}
	if (Wall{2024, 11, 17, 0, 0, 0, 0}).Weekday() != 6 || (Wall{2024, 11, 17, 0, 0, 0, 0}).IsoWeekdayMod7() != 0 {
		t.Error("2024-11-17 is a Sunday: weekday() 6, isoweekday()%7 0")
	}
	if DaysInMonth(2024, 2) != 29 || DaysInMonth(2023, 2) != 28 || DaysInMonth(2024, 12) != 31 {
		t.Error("DaysInMonth")
	}
}

func TestIsoFormat(t *testing.T) {
	ist := mustLoc(t, "Asia/Kolkata")
	ts := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	for got, want := range map[string]string{
		IsoFormat(ts, time.UTC):                       "2024-05-06T07:08:09+00:00",
		IsoFormat(ts.Add(1500*time.Microsecond), ist): "2024-05-06T12:38:09.001500+05:30",
		IsoFormat(ts, mustLoc(t, "America/St_Johns")): "2024-05-06T04:38:09-02:30",
		IsoFormatNaive(Wall{2024, 5, 6, 7, 8, 9, 10}): "2024-05-06T07:08:09.000010",
		// aware + timedelta landing in a DST gap keeps the old offset
		IsoFormatWall(Wall{2024, 3, 10, 2, 30, 0, 0}, mustLoc(t, "America/New_York")): "2024-03-10T02:30:00-05:00",
	} {
		if got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	}
	if TotalSeconds(1500*time.Millisecond) != 1.5 {
		t.Error("TotalSeconds")
	}
}
