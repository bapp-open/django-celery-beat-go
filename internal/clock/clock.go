// Package clock provides the injectable time source used by all scheduling
// code. Nothing outside this package and cmd/ may call time.Now directly.
package clock

import (
	"sync"
	"time"
)

// Clock is the only source of "now" for scheduling decisions.
type Clock interface {
	// Now returns the current instant in UTC, truncated to microseconds
	// (the resolution of Python datetimes and Postgres timestamps) and
	// without a monotonic reading.
	Now() time.Time
}

// System is the wall clock.
type System struct{}

// Now implements Clock.
func (System) Now() time.Time { return Trunc(time.Now()) }

// Trunc normalises t to UTC, microsecond resolution, no monotonic reading.
func Trunc(t time.Time) time.Time {
	return t.UTC().Truncate(time.Microsecond)
}

// Fake is a manually advanced clock for tests and simulations.
type Fake struct {
	mu sync.Mutex
	t  time.Time
}

// NewFake returns a fake clock set to t.
func NewFake(t time.Time) *Fake { return &Fake{t: Trunc(t)} }

// Now implements Clock.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

// Advance moves the clock by d (which may be negative).
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.t = Trunc(f.t.Add(d))
	f.mu.Unlock()
}

// Set jumps the clock to t.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	f.t = Trunc(t)
	f.mu.Unlock()
}
