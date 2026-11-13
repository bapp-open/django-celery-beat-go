//go:build !race

package scheduler

import "testing"

// The race detector makes this run ~10x slower; the same code paths are
// covered under -race by TestSoak30DaysMatchesPythonBeat. `make test`
// runs this one in a second, race-free pass.

// TestSoak10kTasks30Days runs 10,000 periodic tasks (ten replicas of the
// reference set) for 30 simulated days: every replica must be dispatched
// exactly as often as Python beat dispatched its original, and the heap
// must stay flat.
func TestSoak10kTasks30Days(t *testing.T) {
	d := loadSoak(t, "soak_1000_30d.json.gz")
	const copies = 10
	counts, heaps, w := runSoak(t, d, copies)
	compareSoak(t, d, counts, copies, w)
	if len(heaps) < 20 {
		t.Fatalf("only %d heap samples", len(heaps))
	}
	early, late := heaps[2], heaps[len(heaps)-1]
	var peak uint64
	for _, h := range heaps[2:] {
		peak = max(peak, h)
	}
	t.Logf("live heap: day 3 %.1f MB, day %d %.1f MB, peak %.1f MB", float64(early)/1e6, len(heaps), float64(late)/1e6, float64(peak)/1e6)
	// flat: no growth beyond 10% (+1 MB of noise) over 27 simulated days
	if float64(late) > float64(early)*1.10+1e6 || float64(peak) > float64(early)*1.25+2e6 {
		t.Fatalf("memory is not flat: day 3 %d bytes, last day %d bytes, peak %d bytes", early, late, peak)
	}
}
