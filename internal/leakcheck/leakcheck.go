// Package leakcheck fails a test binary when goroutines outlive the tests.
// Every package calls it from TestMain.
package leakcheck

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

var ignore = []string{
	"testing.Main(", "testing.(*M).", "testing.tRunner(", "testing.runTests(",
	"leakcheck.", "runtime.gc", "runtime.MHeap", "os/signal.", "runtime.ensureSigM",
	"runtime.ReadTrace", "testing.(*F).", "runtime/pprof", "created by runtime",
}

func leaked() []string {
	buf := make([]byte, 4<<20)
	buf = buf[:runtime.Stack(buf, true)]
	var out []string
	for _, g := range strings.Split(string(buf), "\n\n") {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}
		skip := false
		for _, s := range ignore {
			skip = skip || strings.Contains(g, s)
		}
		if !skip {
			out = append(out, g)
		}
	}
	return out
}

// Main runs the tests and then verifies no goroutine is left behind.
func Main(m *testing.M) {
	code := m.Run()
	if code == 0 {
		var left []string
		for i := 0; i < 50; i++ {
			if left = leaked(); len(left) == 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if len(left) > 0 {
			fmt.Fprintf(os.Stderr, "leakcheck: %d goroutine(s) leaked:\n\n%s\n", len(left), strings.Join(left, "\n\n"))
			code = 1
		}
	}
	os.Exit(code)
}
