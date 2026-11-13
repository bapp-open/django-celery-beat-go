package pyjson

import (
	"errors"
	"math"
	"testing"
)

func TestFloatRepr(t *testing.T) {
	for f, want := range map[float64]string{
		1: "1.0", 1.5: "1.5", 100: "100.0", 1e15: "1000000000000000.0", 1e16: "1e+16",
		1e22: "1e+22", 0.0001: "0.0001", 0.00001: "1e-05", 5e-324: "5e-324",
		123456789012345680: "1.2345678901234568e+17", 0.1: "0.1", -2.5e-7: "-2.5e-07",
		math.Copysign(0, -1): "-0.0", 1.7976931348623157e308: "1.7976931348623157e+308",
	} {
		if got := FloatRepr(f); got != want {
			t.Errorf("FloatRepr(%g) = %s, want %s", f, got, want)
		}
	}
}

func TestLoadsDumpsPythonBehaviours(t *testing.T) {
	for in, want := range map[string]string{
		`{"b":1,"a":2,"b":3}`:               `{"b": 3, "a": 2}`, // first position, last value
		`[1E2, -0, 1.50, 1e400]`:            `[100.0, 0, 1.5, Infinity]`,
		`[NaN, -Infinity]`:                  `[NaN, -Infinity]`,
		`123456789012345678901234567890`:    `123456789012345678901234567890`,
		"\"\\ud83d\\ude00 \u00e9 \\u007f\"": "\"\\ud83d\\ude00 \\u00e9 \\u007f\"",
		"\"\\ud83d\"":                       "\"\\ud83d\"", // lone surrogate survives
		` [ ] `:                             `[]`,
	} {
		v, err := Loads(in)
		if err != nil {
			t.Errorf("Loads(%s): %v", in, err)
			continue
		}
		if got := string(Dumps(v)); got != want {
			t.Errorf("Dumps(Loads(%s)) = %s, want %s", in, got, want)
		}
	}
	for _, bad := range []string{``, `01`, `1.`, `[1,]`, `{"a"}`, `'a'`, "\"a\nb\"", `[1] x`, `{"a":1,}`, `-`, `"\x"`, "\xff"} {
		if _, err := Loads(bad); err == nil {
			t.Errorf("Loads(%q) must fail like json.loads", bad)
		}
	}
	if _, err := Loads(`{"__type__": "datetime", "__value__": "2024-01-01T00:00:00"}`); !errors.Is(err, ErrTyped) {
		t.Errorf("kombu typed marker: %v", err)
	}
}

func TestSafeRepr(t *testing.T) {
	v, _ := Loads(`[1, "it's", null, true, 1.0, {"k": [[["deep"]]]}]`)
	if got, want := SafeRepr(v, 1024), `[1, 'it\'s', None, True, 1.0, {'k': [[...]]}]`; got != want {
		t.Errorf("got %s want %s", got, want)
	}
	if got := SafeRepr(EmptyTuple{}, 1024); got != "()" {
		t.Errorf("empty tuple: %s", got)
	}
	long, _ := Loads(`["aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc"]`)
	if got, want := SafeRepr(long, 20), `['aaaaaaaaaa', 'bbbbb...', ...]`; got != want {
		t.Errorf("truncated: got %s want %s", got, want)
	}
}

func FuzzLoads(f *testing.F) {
	for _, s := range []string{`[]`, `{"a": [1, 2.5, "x", null, true]}`, `"\ud83d"`, `[NaN]`, `{"a":{"a":{"a":{"a":1}}}}`, `1e999`, `[[[[[[]]]]]]`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, text string) {
		v, err := Loads(text)
		if err != nil {
			return
		}
		out := Dumps(v)
		v2, err := Loads(string(out))
		if err != nil {
			t.Fatalf("Dumps produced unparsable JSON %q from %q: %v", out, text, err)
		}
		if string(Dumps(v2)) != string(out) {
			t.Fatalf("dumps is not a fixed point for %q", text)
		}
		_ = SafeRepr(v, 1024)
		_ = SafeRepr(v, 7)
		_ = SafeRepr(v, -1)
	})
}
