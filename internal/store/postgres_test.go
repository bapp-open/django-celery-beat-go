package store

import (
	"testing"
	"time"
)

// USE_TZ=False on `timestamp with time zone` columns: Django writes a naive
// value in its connection zone and reads the stored instant back as wall time
// there. gobeat must read the same naive value, as UTC.
func TestLocalZoneReadsWallTime(t *testing.T) {
	bucharest, err := time.LoadLocation("Europe/Bucharest")
	if err != nil {
		t.Skip("no tzdata:", err)
	}
	s := &pg{local: bucharest}
	cases := []struct {
		name    string
		instant time.Time
		want    time.Time
	}{
		// Python beat stored utcnow() = 10:00 naive; Postgres took it as
		// 10:00 Bucharest (EET, UTC+2) = 08:00Z.
		{"winter", time.Date(2026, 11, 25, 8, 0, 0, 0, time.UTC), time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)},
		{"summer", time.Date(2026, 7, 1, 7, 30, 0, 0, time.UTC), time.Date(2026, 7, 1, 10, 30, 0, 0, time.UTC)},
		// Both occurrences of the repeated hour on 2026-10-25 (03:30 EEST
		// then 03:30 EET) read as the same naive 03:30.
		{"repeated hour, first", time.Date(2026, 10, 25, 0, 30, 0, 0, time.UTC), time.Date(2026, 10, 25, 3, 30, 0, 0, time.UTC)},
		{"repeated hour, second", time.Date(2026, 10, 25, 1, 30, 0, 0, time.UTC), time.Date(2026, 10, 25, 3, 30, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		got := s.utc(c.instant)
		if !got.Equal(c.want) || got.Location() != time.UTC {
			t.Errorf("%s: utc(%s) = %s, want %s", c.name, c.instant, got, c.want)
		}
	}
	if got := s.seenCol(); got != "last_run_at::timestamp" {
		t.Errorf("seenCol() = %q; the compare-and-swap must compare wall time", got)
	}
}

func TestUTCZoneIsUnchanged(t *testing.T) {
	s := &pg{}
	in := time.Date(2026, 11, 25, 8, 0, 0, 0, time.FixedZone("x", 3*3600))
	if got := s.utc(in); !got.Equal(in) || got.Location() != time.UTC {
		t.Errorf("utc(%s) = %s", in, got)
	}
	if s.utcp(nil) != nil {
		t.Error("utcp(nil) != nil")
	}
	if got := s.seenCol(); got != "last_run_at" {
		t.Errorf("seenCol() = %q", got)
	}
}
