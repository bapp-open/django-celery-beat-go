package metrics

import (
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	r := New()
	r.Add("gobeat_dispatch_total", 2, "result", "published")
	r.Add("gobeat_dispatch_total", 1, "result", "published")
	r.Set("gobeat_leader", 1)
	r.ObserveLateness(0.003)
	r.ObserveLateness(42)
	var sb strings.Builder
	if _, err := r.WriteTo(&sb); err != nil {
		t.Fatal(err)
	}
	out := sb.String()
	for _, want := range []string{
		"# TYPE gobeat_dispatch_total counter\n", `gobeat_dispatch_total{result="published"} 3` + "\n",
		"# TYPE gobeat_leader gauge\ngobeat_leader 1\n",
		`gobeat_dispatch_lateness_seconds_bucket{le="0.005"} 1` + "\n",
		`gobeat_dispatch_lateness_seconds_bucket{le="60"} 2` + "\n",
		`gobeat_dispatch_lateness_seconds_bucket{le="+Inf"} 2` + "\n",
		"gobeat_dispatch_lateness_seconds_count 2\n", "gobeat_dispatch_lateness_seconds_sum 42.003\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	var nilReg *Registry
	nilReg.Add("x", 1) // a nil registry is a no-op
	nilReg.Set("x", 1)
	nilReg.ObserveLateness(1)
}
