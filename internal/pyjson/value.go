// Package pyjson reproduces Python's json.loads / json.dumps and Celery's
// saferepr for the JSON documents stored in PeriodicTask.args, kwargs and
// headers, so that message bodies and the argsrepr/kwargsrepr headers are
// byte-identical to what Python beat sends.
//
// Sources: CPython Lib/json (decoder.py, encoder.py, scanner.py),
// kombu/utils/json.py (loads/dumps, object_hook), celery/utils/saferepr.py,
// celery/utils/text.py (truncate).
package pyjson

import (
	"errors"
	"math/big"
)

// Value is one of: nil (None), bool, *big.Int, float64, Str, *List, *Dict.
type Value any

// Str is a Python str. It is stored as "generalised UTF-8": like UTF-8 but
// lone surrogates (which json.loads accepts through \uD800 escapes) are
// kept as their three-byte encoding.
type Str string

// List is a Python list.
type List struct{ Items []Value }

// Dict is a Python dict: insertion ordered; assigning an existing key
// keeps its position and replaces the value.
type Dict struct {
	Keys []Str
	Vals []Value
	idx  map[Str]int
}

// NewDict returns an empty dict.
func NewDict() *Dict { return &Dict{idx: map[Str]int{}} }

// Set is d[k] = v.
func (d *Dict) Set(k Str, v Value) {
	if i, ok := d.idx[k]; ok {
		d.Vals[i] = v
		return
	}
	d.idx[k] = len(d.Keys)
	d.Keys = append(d.Keys, k)
	d.Vals = append(d.Vals, v)
}

// Get is d.get(k).
func (d *Dict) Get(k Str) (Value, bool) {
	i, ok := d.idx[k]
	if !ok {
		return nil, false
	}
	return d.Vals[i], true
}

// Len is len(d).
func (d *Dict) Len() int { return len(d.Keys) }

// Int builds an integer value.
func Int(n int64) Value { return big.NewInt(n) }

// ErrTyped is returned for kombu's {"__type__": ..., "__value__": ...}
// markers, which kombu.utils.json.loads turns into datetimes, UUIDs,
// Decimals, ... gobeat does not reproduce those conversions.
var ErrTyped = errors.New(`kombu typed JSON marker ("__type__"/"__value__") is not supported`)

// --- generalised UTF-8 helpers ---------------------------------------------

// appendRune appends r, allowing surrogate code points.
func appendRune(b []byte, r rune) []byte {
	switch {
	case r < 0x80:
		return append(b, byte(r))
	case r < 0x800:
		return append(b, 0xC0|byte(r>>6), 0x80|byte(r)&0x3F)
	case r < 0x10000:
		return append(b, 0xE0|byte(r>>12), 0x80|byte(r>>6)&0x3F, 0x80|byte(r)&0x3F)
	default:
		return append(b, 0xF0|byte(r>>18), 0x80|byte(r>>12)&0x3F, 0x80|byte(r>>6)&0x3F, 0x80|byte(r)&0x3F)
	}
}

// decodeRune decodes one code point (surrogates allowed) from s.
// Invalid bytes decode as U+FFFD with size 1.
func decodeRune(s string) (rune, int) {
	c := s[0]
	switch {
	case c < 0x80:
		return rune(c), 1
	case c&0xE0 == 0xC0 && len(s) >= 2 && s[1]&0xC0 == 0x80:
		if r := rune(c&0x1F)<<6 | rune(s[1]&0x3F); r >= 0x80 {
			return r, 2
		}
	case c&0xF0 == 0xE0 && len(s) >= 3 && s[1]&0xC0 == 0x80 && s[2]&0xC0 == 0x80:
		if r := rune(c&0x0F)<<12 | rune(s[1]&0x3F)<<6 | rune(s[2]&0x3F); r >= 0x800 {
			return r, 3
		}
	case c&0xF8 == 0xF0 && len(s) >= 4 && s[1]&0xC0 == 0x80 && s[2]&0xC0 == 0x80 && s[3]&0xC0 == 0x80:
		if r := rune(c&0x07)<<18 | rune(s[1]&0x3F)<<12 | rune(s[2]&0x3F)<<6 | rune(s[3]&0x3F); r >= 0x10000 && r <= 0x10FFFF {
			return r, 4
		}
	}
	return 0xFFFD, 1
}

// Runes returns the code points of s (len(s) in Python == len(Runes())).
func (s Str) Runes() []rune {
	out := make([]rune, 0, len(s))
	for i := 0; i < len(s); {
		r, n := decodeRune(string(s[i:]))
		out = append(out, r)
		i += n
	}
	return out
}

// FromRunes builds a Str from code points.
func FromRunes(rs []rune) Str {
	b := make([]byte, 0, len(rs))
	for _, r := range rs {
		b = appendRune(b, r)
	}
	return Str(b)
}

// HasSurrogates reports whether s contains a lone surrogate, i.e. cannot
// be encoded as UTF-8 by Python (str.encode('utf-8') raises).
func (s Str) HasSurrogates() bool {
	for i := 0; i+2 < len(s); i++ {
		if s[i] == 0xED && s[i+1] >= 0xA0 {
			return true
		}
	}
	return false
}
