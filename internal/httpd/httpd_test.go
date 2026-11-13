package httpd

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bapp-open/django-celery-beat-go/internal/metrics"
)

func TestHealthzFailsWhenTheLoopStopsTicking(t *testing.T) {
	now := time.Unix(1000, 0)
	h := NewHealth(30*time.Second, func() time.Time { return now })
	srv := httptest.NewServer(Handler(h, metrics.New()))
	defer srv.Close()
	get := func(path string) (int, string) {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	h.Beat("leader")
	now = now.Add(29 * time.Second)
	if code, body := get("/healthz"); code != 200 || !strings.Contains(body, `"state":"leader"`) {
		t.Fatalf("fresh tick: %d %s", code, body)
	}
	now = now.Add(2 * time.Second)
	if code, body := get("/healthz"); code != 503 || !strings.Contains(body, `"ok":false`) {
		t.Fatalf("stale tick must fail: %d %s", code, body)
	}
	h.Beat("standby")
	if code, _ := get("/healthz"); code != 200 {
		t.Fatalf("a standby that ticks is healthy: %d", code)
	}
	if code, body := get("/metrics"); code != 200 || !strings.Contains(body, "gobeat_dispatch_lateness_seconds_count") {
		t.Fatalf("metrics: %d %s", code, body)
	}
	http.DefaultClient.CloseIdleConnections()
}

func TestServeStopsOnCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, addr, Handler(NewHealth(time.Minute, time.Now), metrics.New()))
	}()
	var ok bool
	for i := 0; i < 100 && !ok; i++ {
		ok, _ = Probe("http://" + addr + "/healthz")
		if !ok {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !ok {
		t.Fatal("server did not come up")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("graceful shutdown: %v", err)
	}
	if ok, _ := Probe("http://" + addr + "/healthz"); ok {
		t.Fatal("still serving after shutdown")
	}
}
