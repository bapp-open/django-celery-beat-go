// Package httpd serves /healthz and /metrics.
package httpd

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/bapp-open/django-celery-beat-go/internal/metrics"
)

// Health tracks the scheduler loop's liveness.
type Health struct {
	mu     sync.Mutex
	last   time.Time
	state  string
	maxAge time.Duration
	now    func() time.Time
}

// NewHealth returns a tracker that reports unhealthy when the loop has not
// ticked for maxAge. now is the wall clock (injected for tests); liveness
// is about real elapsed time, not the scheduling clock.
func NewHealth(maxAge time.Duration, now func() time.Time) *Health {
	return &Health{maxAge: maxAge, now: now, last: now(), state: "starting"}
}

// Beat records a loop iteration.
func (h *Health) Beat(state string) {
	h.mu.Lock()
	h.last, h.state = h.now(), state
	h.mu.Unlock()
}

// Status returns whether the loop is alive, its state and tick age.
func (h *Health) Status() (ok bool, state string, age time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	age = h.now().Sub(h.last)
	return age <= h.maxAge, h.state, age
}

// Handler returns the HTTP handler.
func Handler(h *Health, m *metrics.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		ok, state, age := h.Status()
		w.Header().Set("Content-Type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": ok, "state": state, "last_tick_age_seconds": age.Seconds(),
		})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = m.WriteTo(w)
	})
	return mux
}

// Serve runs the server until ctx is cancelled.
func Serve(ctx context.Context, addr string, handler http.Handler) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	err = srv.Serve(ln)
	<-done
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Probe GETs a /healthz URL and reports whether it answered 200.
func Probe(url string) (bool, error) {
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}
