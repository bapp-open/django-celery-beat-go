// Package metrics is a tiny Prometheus text-format registry. It exists so
// that gobeat does not pay the memory cost of a full client library for a
// handful of counters.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
)

type desc struct{ help, typ string }

var descs = map[string]desc{
	"gobeat_dispatch_total":              {"Runs handed to the broker (result=published) or logged in dry-run mode (result=dry_run).", "counter"},
	"gobeat_dispatch_lateness_seconds":   {"Delay between the instant a run became due and its dispatch.", "histogram"},
	"gobeat_errors_total":                {"Errors by kind (db, broker, task, settings).", "counter"},
	"gobeat_reloads_total":               {"Schedule reloads by reason.", "counter"},
	"gobeat_claim_conflicts_total":       {"Claims lost because the row changed (another writer got there first).", "counter"},
	"gobeat_skipped_runs_total":          {"Runs skipped by the missed-run policy or duplicate suppression.", "counter"},
	"gobeat_abandoned_total":             {"Claimed runs given up on after GOBEAT_PUBLISH_MAX_AGE.", "counter"},
	"gobeat_publish_retries_total":       {"Publish attempts that failed and were queued for retry.", "counter"},
	"gobeat_leader":                      {"1 when this instance holds the leader lock.", "gauge"},
	"gobeat_tasks":                       {"Enabled periodic tasks currently scheduled.", "gauge"},
	"gobeat_tasks_refused":               {"Enabled periodic tasks refused as unsupported/invalid.", "gauge"},
	"gobeat_pending_publishes":           {"Claimed runs waiting for the broker.", "gauge"},
	"gobeat_last_tick_timestamp_seconds": {"Unix time of the last scheduler loop iteration.", "gauge"},
	"gobeat_info":                        {"Build and mode information.", "gauge"},
}

var buckets = []float64{0.005, 0.025, 0.1, 0.5, 1, 2.5, 5, 15, 60, 300, 1800, 3600}

// Registry holds all gobeat metrics.
type Registry struct {
	mu     sync.Mutex
	vals   map[string]float64 // "name{labels}" -> value
	hCount uint64
	hSum   float64
	hBkt   []uint64
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{vals: map[string]float64{}, hBkt: make([]uint64, len(buckets))}
}

func key(name string, labels []string) string {
	if len(labels) == 0 {
		return name
	}
	var sb strings.Builder
	sb.WriteString(name)
	sb.WriteByte('{')
	for i := 0; i+1 < len(labels); i += 2 {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%s=%q", labels[i], labels[i+1])
	}
	sb.WriteByte('}')
	return sb.String()
}

// Add increments a counter; labels are key, value pairs.
func (r *Registry) Add(name string, v float64, labels ...string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.vals[key(name, labels)] += v
	r.mu.Unlock()
}

// Set sets a gauge.
func (r *Registry) Set(name string, v float64, labels ...string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.vals[key(name, labels)] = v
	r.mu.Unlock()
}

// Get returns a value (0 when unset); used by tests.
func (r *Registry) Get(name string, labels ...string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.vals[key(name, labels)]
}

// ObserveLateness records one dispatch lateness sample.
func (r *Registry) ObserveLateness(seconds float64) {
	if r == nil || math.IsNaN(seconds) {
		return
	}
	r.mu.Lock()
	r.hCount++
	r.hSum += seconds
	for i, b := range buckets {
		if seconds <= b {
			r.hBkt[i]++
		}
	}
	r.mu.Unlock()
}

// WriteTo renders the Prometheus text exposition format.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	keys := make([]string, 0, len(r.vals))
	for k := range r.vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	seen := map[string]bool{}
	for _, k := range keys {
		name := k
		if i := strings.IndexByte(k, '{'); i >= 0 {
			name = k[:i]
		}
		if !seen[name] {
			seen[name] = true
			if d, ok := descs[name]; ok {
				fmt.Fprintf(&sb, "# HELP %s %s\n# TYPE %s %s\n", name, d.help, name, d.typ)
			}
		}
		fmt.Fprintf(&sb, "%s %g\n", k, r.vals[k])
	}
	const h = "gobeat_dispatch_lateness_seconds"
	fmt.Fprintf(&sb, "# HELP %s %s\n# TYPE %s histogram\n", h, descs[h].help, h)
	for i, b := range buckets {
		fmt.Fprintf(&sb, "%s_bucket{le=\"%g\"} %d\n", h, b, r.hBkt[i])
	}
	fmt.Fprintf(&sb, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n", h, r.hCount, h, r.hSum, h, r.hCount)
	r.mu.Unlock()
	n, err := io.WriteString(w, sb.String())
	return int64(n), err
}
