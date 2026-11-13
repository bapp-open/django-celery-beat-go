package shadow

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const pyLog = `[2024-11-13 12:00:00,010: INFO/MainProcess] beat: Starting...
[2024-11-13 12:00:03,004: INFO/MainProcess] Scheduler: Sending due task every-3s (app.tasks.a)
[2024-11-13 12:00:05,001: INFO/MainProcess] Scheduler: Sending due task name (with) parens (app.tasks.b)
[2024-11-13 12:00:06,002: INFO/MainProcess] Scheduler: Sending due task every-3s (app.tasks.a)
[2024-11-13 12:00:09,003: INFO/MainProcess] Scheduler: Sending due task every-3s (app.tasks.a)
[2024-11-13 12:00:09,500: INFO/MainProcess] DatabaseScheduler: Schedule changed.
[2024-11-13 12:00:12,003: INFO/MainProcess] Scheduler: Sending due task every-3s (app.tasks.a)
[2024-11-13 12:00:15,003: INFO/MainProcess] Scheduler: Sending due task every-3s (app.tasks.a)
`

const gbLog = `{"time":"2024-11-13T12:00:00.1Z","level":"INFO","msg":"scheduler started","dry_run":true}
{"time":"2024-11-13T12:00:03.001Z","level":"INFO","msg":"dispatch","name":"every-3s","dry_run":true}
{"time":"2024-11-13T12:00:05.2Z","level":"INFO","msg":"dispatch","name":"name (with) parens","dry_run":true}
{"time":"2024-11-13T12:00:06.001Z","level":"INFO","msg":"dispatch","name":"every-3s","dry_run":true}
not json
{"time":"2024-11-13T12:00:09.001Z","level":"INFO","msg":"dispatch","name":"every-3s","dry_run":true}
{"time":"2024-11-13T12:00:12.001Z","level":"INFO","msg":"dispatch","name":"every-3s","dry_run":true}
{"time":"2024-11-13T12:00:15.001Z","level":"INFO","msg":"dispatch","name":"every-3s","dry_run":true}
`

func write(t *testing.T, dir, name, content string) string {
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLogsAgree(t *testing.T) {
	dir := t.TempDir()
	var out, errOut strings.Builder
	code := Main([]string{"--python-log", write(t, dir, "py.log", pyLog), "--gobeat-log", write(t, dir, "gb.log", gbLog)}, &out, &errOut)
	if code != 0 || !strings.Contains(out.String(), "matched: 6") || !strings.Contains(out.String(), "logs agree") {
		t.Fatalf("exit %d\n%s%s", code, out.String(), errOut.String())
	}
}

func TestDifferencesAreReported(t *testing.T) {
	dir := t.TempDir()
	gb := strings.Replace(gbLog, `{"time":"2024-11-13T12:00:06.001Z","level":"INFO","msg":"dispatch","name":"every-3s","dry_run":true}`+"\n", "", 1)
	gb = strings.Replace(gb, "12:00:05.2Z", "12:00:07.9Z", 1) // 2.9 s late: outside the tolerance
	var out, errOut strings.Builder
	code := Main([]string{"--python-log", write(t, dir, "py.log", pyLog), "--gobeat-log", write(t, dir, "gb.log", gb)}, &out, &errOut)
	s := out.String()
	if code != 1 || !strings.Contains(s, "- python only  2024-11-13T12:00:06.002Z  every-3s") ||
		!strings.Contains(s, "+ gobeat only  2024-11-13T12:00:07.900Z  name (with) parens") ||
		!strings.Contains(s, "only python: 1   only gobeat: 1") {
		t.Fatalf("exit %d\n%s", code, s)
	}
}

func TestPythonTimezoneAndUsage(t *testing.T) {
	loc, _ := time.LoadLocation("Europe/Bucharest")
	evs, err := ParsePython(strings.NewReader(pyLog), loc)
	if err != nil || len(evs) != 6 || evs[0].At.Format(time.RFC3339) != "2024-11-13T10:00:03Z" {
		t.Fatalf("%v %v", evs, err)
	}
	var out, errOut strings.Builder
	if Main(nil, &out, &errOut) != 2 || Main([]string{"--python-log", "/nonexistent", "--gobeat-log", "/nonexistent"}, &out, &errOut) != 2 {
		t.Fatal("usage errors must exit 2")
	}
}
