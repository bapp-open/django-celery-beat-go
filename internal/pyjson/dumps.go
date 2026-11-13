package pyjson

import (
	"math"
	"math/big"
	"strconv"
	"strings"
)

const hexdigits = "0123456789abcdef"

// AppendString appends the JSON encoding of s as json.dumps writes it with
// ensure_ascii=True (json.encoder.py_encode_basestring_ascii).
func AppendString(b []byte, s Str) []byte {
	b = append(b, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < 0x80 {
			switch c {
			case '"':
				b = append(b, '\\', '"')
			case '\\':
				b = append(b, '\\', '\\')
			case '\n':
				b = append(b, '\\', 'n')
			case '\r':
				b = append(b, '\\', 'r')
			case '\t':
				b = append(b, '\\', 't')
			case '\b':
				b = append(b, '\\', 'b')
			case '\f':
				b = append(b, '\\', 'f')
			default:
				if c < 0x20 || c == 0x7f { // ESCAPE_ASCII: anything outside ' '..'~'
					b = appendU(b, rune(c))
				} else {
					b = append(b, c)
				}
			}
			i++
			continue
		}
		r, n := decodeRune(string(s[i:]))
		i += n
		if r >= 0x10000 {
			r -= 0x10000
			b = appendU(b, 0xD800|(r>>10)&0x3FF)
			b = appendU(b, 0xDC00|r&0x3FF)
		} else {
			b = appendU(b, r)
		}
	}
	return append(b, '"')
}

func appendU(b []byte, r rune) []byte {
	return append(b, '\\', 'u', hexdigits[r>>12&0xF], hexdigits[r>>8&0xF], hexdigits[r>>4&0xF], hexdigits[r&0xF])
}

// FloatRepr is Python's repr(float) (float_repr_style 'short'):
// shortest round-trip digits, fixed notation when the decimal point
// position is in (-4, 16], scientific otherwise.
func FloatRepr(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	e := strconv.FormatFloat(f, 'e', -1, 64) // d.ddddde±XX
	neg := strings.HasPrefix(e, "-")
	e = strings.TrimPrefix(e, "-")
	mant, expStr, _ := strings.Cut(e, "e")
	exp, _ := strconv.Atoi(expStr)
	digits := strings.Replace(mant, ".", "", 1)
	decpt := exp + 1 // value = 0.digits * 10**decpt
	var sb strings.Builder
	if neg {
		sb.WriteByte('-')
	}
	if decpt > -4 && decpt <= 16 {
		switch {
		case decpt <= 0:
			sb.WriteString("0.")
			sb.WriteString(strings.Repeat("0", -decpt))
			sb.WriteString(digits)
		case decpt >= len(digits):
			sb.WriteString(digits)
			sb.WriteString(strings.Repeat("0", decpt-len(digits)))
			sb.WriteString(".0")
		default:
			sb.WriteString(digits[:decpt])
			sb.WriteByte('.')
			sb.WriteString(digits[decpt:])
		}
		return sb.String()
	}
	sb.WriteString(digits[:1])
	if len(digits) > 1 {
		sb.WriteByte('.')
		sb.WriteString(digits[1:])
	}
	sb.WriteByte('e')
	x := decpt - 1
	if x < 0 {
		sb.WriteByte('-')
		x = -x
	} else {
		sb.WriteByte('+')
	}
	if x < 10 {
		sb.WriteByte('0')
	}
	sb.WriteString(strconv.Itoa(x))
	return sb.String()
}

// jsonFloat is json.encoder's floatstr with allow_nan=True.
func jsonFloat(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	return FloatRepr(f)
}

// Append appends json.dumps(v) with Python's default separators
// (", " and ": ") and ensure_ascii=True, which is what
// kombu.utils.json.dumps produces.
func Append(b []byte, v Value) []byte {
	switch x := v.(type) {
	case nil:
		return append(b, "null"...)
	case bool:
		if x {
			return append(b, "true"...)
		}
		return append(b, "false"...)
	case *big.Int:
		return x.Append(b, 10)
	case int:
		return strconv.AppendInt(b, int64(x), 10)
	case int64:
		return strconv.AppendInt(b, x, 10)
	case float64:
		return append(b, jsonFloat(x)...)
	case Str:
		return AppendString(b, x)
	case string:
		return AppendString(b, Str(x))
	case *List:
		b = append(b, '[')
		for i, it := range x.Items {
			if i > 0 {
				b = append(b, ',', ' ')
			}
			b = Append(b, it)
		}
		return append(b, ']')
	case *Dict:
		b = append(b, '{')
		for i, k := range x.Keys {
			if i > 0 {
				b = append(b, ',', ' ')
			}
			b = AppendString(b, k)
			b = append(b, ':', ' ')
			b = Append(b, x.Vals[i])
		}
		return append(b, '}')
	}
	panic("pyjson: unsupported value type")
}

// Dumps returns json.dumps(v).
func Dumps(v Value) []byte { return Append(nil, v) }
