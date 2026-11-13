package pyjson

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestGoldenPyJSON(t *testing.T) {
	f, err := os.Open(filepath.Join("..", "..", "testdata", "golden", "pyjson.jsonl.gz"))
	if err != nil {
		t.Fatalf("golden file missing (run `make golden`): %v", err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	n, bad := 0, 0
	for sc.Scan() {
		n++
		var r struct {
			Text     string
			Error    string
			Dumps    string
			Repr1024 string `json:"repr1024_json"`
			Repr40   string `json:"repr40_json"`
		}
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		fail := func(format string, a ...any) {
			if bad++; bad < 4 {
				msg := fmt.Sprintf(format, a...)
				if len(msg) > 1500 {
					msg = msg[:1500] + "..."
				}
				t.Error(msg)
			}
		}
		v, err := Loads(r.Text)
		if r.Error != "" {
			if err == nil {
				fail("Loads(%q) succeeded, python raised %s", r.Text, r.Error)
			}
			continue
		}
		if err != nil {
			fail("Loads(%q) = %v, python accepted it", r.Text, err)
			continue
		}
		if got := string(Dumps(v)); got != r.Dumps {
			fail("dumps mismatch for %q\n got  %s\n want %s", r.Text, got, r.Dumps)
			continue
		}
		// expected reprs are stored JSON-encoded (they may contain lone
		// surrogates, which do not survive a Go string round trip).
		if got := string(AppendString(nil, Str(SafeRepr(v, 1024)))); got != r.Repr1024 {
			fail("saferepr(1024) mismatch for %q\n got  %s\n want %s", r.Text, got, r.Repr1024)
			continue
		}
		if got := string(AppendString(nil, Str(SafeRepr(v, 40)))); got != r.Repr40 {
			fail("saferepr(40) mismatch for %q\n got  %s\n want %s", r.Text, got, r.Repr40)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if bad > 0 || n < 1000 {
		t.Fatalf("%d of %d cases differ from Python", bad, n)
	}
	t.Logf("%d cases match json.loads/json.dumps/saferepr", n)
}
