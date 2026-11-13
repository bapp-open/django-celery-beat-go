package pyjson

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"
)

// maxDepth guards against stack exhaustion; CPython raises RecursionError
// at roughly this nesting depth as well.
const maxDepth = 500

type parser struct {
	s   string
	pos int
}

// Loads parses JSON text exactly as kombu.utils.json.loads (CPython
// json.loads with kombu's object_hook) does. Notable Python behaviours:
// NaN/Infinity/-Infinity are accepted, integers are arbitrary precision,
// duplicate keys keep the first position and the last value, lone
// surrogate escapes are preserved.
func Loads(text string) (Value, error) {
	if !utf8.ValidString(text) {
		return nil, fmt.Errorf("json: input is not valid UTF-8")
	}
	if strings.HasPrefix(text, "\xef\xbb\xbf") {
		return nil, fmt.Errorf("json: unexpected UTF-8 BOM")
	}
	p := &parser{s: text}
	p.ws()
	v, err := p.value(0)
	if err != nil {
		return nil, err
	}
	p.ws()
	if p.pos != len(p.s) {
		return nil, p.errf("extra data")
	}
	return v, nil
}

func (p *parser) errf(format string, a ...any) error {
	return fmt.Errorf("json: %s at char %d", fmt.Sprintf(format, a...), p.pos)
}

func (p *parser) ws() {
	for p.pos < len(p.s) {
		switch p.s[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *parser) value(depth int) (Value, error) {
	if depth > maxDepth {
		return nil, p.errf("nesting too deep")
	}
	if p.pos >= len(p.s) {
		return nil, p.errf("expecting value")
	}
	rest := p.s[p.pos:]
	switch c := p.s[p.pos]; {
	case c == '"':
		p.pos++
		return p.str()
	case c == '{':
		p.pos++
		return p.object(depth)
	case c == '[':
		p.pos++
		return p.array(depth)
	case strings.HasPrefix(rest, "null"):
		p.pos += 4
		return nil, nil
	case strings.HasPrefix(rest, "true"):
		p.pos += 4
		return true, nil
	case strings.HasPrefix(rest, "false"):
		p.pos += 5
		return false, nil
	case strings.HasPrefix(rest, "NaN"):
		p.pos += 3
		return math.NaN(), nil
	case strings.HasPrefix(rest, "Infinity"):
		p.pos += 8
		return math.Inf(1), nil
	case strings.HasPrefix(rest, "-Infinity"):
		p.pos += 9
		return math.Inf(-1), nil
	case c == '-' || (c >= '0' && c <= '9'):
		return p.number()
	}
	return nil, p.errf("expecting value")
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// number implements json.scanner's NUMBER_RE:
// (-?(?:0|[1-9]\d*))(\.\d+)?([eE][-+]?\d+)?
func (p *parser) number() (Value, error) {
	start := p.pos
	i := p.pos
	if i < len(p.s) && p.s[i] == '-' {
		i++
	}
	if i >= len(p.s) || !isDigit(p.s[i]) {
		return nil, p.errf("expecting value")
	}
	if p.s[i] == '0' {
		i++
	} else {
		for i < len(p.s) && isDigit(p.s[i]) {
			i++
		}
	}
	isFloat := false
	if i+1 < len(p.s) && p.s[i] == '.' && isDigit(p.s[i+1]) {
		isFloat = true
		i += 2
		for i < len(p.s) && isDigit(p.s[i]) {
			i++
		}
	}
	if i < len(p.s) && (p.s[i] == 'e' || p.s[i] == 'E') {
		j := i + 1
		if j < len(p.s) && (p.s[j] == '+' || p.s[j] == '-') {
			j++
		}
		if j < len(p.s) && isDigit(p.s[j]) {
			for j < len(p.s) && isDigit(p.s[j]) {
				j++
			}
			isFloat = true
			i = j
		}
	}
	lit := p.s[start:i]
	p.pos = i
	if isFloat {
		f, err := strconv.ParseFloat(lit, 64)
		if err != nil && !isRange(err) {
			return nil, p.errf("bad float %q", lit)
		}
		return f, nil
	}
	n, ok := new(big.Int).SetString(lit, 10)
	if !ok {
		return nil, p.errf("bad int %q", lit)
	}
	return n, nil
}

func isRange(err error) bool {
	ne, ok := err.(*strconv.NumError)
	return ok && ne.Err == strconv.ErrRange
}

func hex4(s string) (rune, bool) {
	if len(s) < 4 {
		return 0, false
	}
	var r rune
	for i := 0; i < 4; i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			r = r<<4 | rune(c-'0')
		case c >= 'a' && c <= 'f':
			r = r<<4 | rune(c-'a'+10)
		case c >= 'A' && c <= 'F':
			r = r<<4 | rune(c-'A'+10)
		default:
			return 0, false
		}
	}
	return r, true
}

// str parses a string body (opening quote consumed); json.decoder
// py_scanstring with strict=True.
func (p *parser) str() (Str, error) {
	var b []byte
	for {
		if p.pos >= len(p.s) {
			return "", p.errf("unterminated string")
		}
		c := p.s[p.pos]
		switch {
		case c == '"':
			p.pos++
			return Str(b), nil
		case c < 0x20:
			return "", p.errf("invalid control character")
		case c != '\\':
			b = append(b, c)
			p.pos++
		default:
			p.pos++
			if p.pos >= len(p.s) {
				return "", p.errf("unterminated string")
			}
			e := p.s[p.pos]
			p.pos++
			switch e {
			case '"', '\\', '/':
				b = append(b, e)
			case 'b':
				b = append(b, '\b')
			case 'f':
				b = append(b, '\f')
			case 'n':
				b = append(b, '\n')
			case 'r':
				b = append(b, '\r')
			case 't':
				b = append(b, '\t')
			case 'u':
				r, ok := hex4(p.s[p.pos:])
				if !ok {
					return "", p.errf("invalid \\uXXXX escape")
				}
				p.pos += 4
				if r >= 0xD800 && r <= 0xDBFF && strings.HasPrefix(p.s[p.pos:], "\\u") {
					if r2, ok := hex4(p.s[p.pos+2:]); ok && r2 >= 0xDC00 && r2 <= 0xDFFF {
						r = 0x10000 + (r-0xD800)<<10 | (r2 - 0xDC00)
						p.pos += 6
					}
				}
				b = appendRune(b, r)
			default:
				return "", p.errf("invalid \\escape")
			}
		}
	}
}

func (p *parser) array(depth int) (Value, error) {
	l := &List{}
	p.ws()
	if p.pos < len(p.s) && p.s[p.pos] == ']' {
		p.pos++
		return l, nil
	}
	for {
		p.ws()
		v, err := p.value(depth + 1)
		if err != nil {
			return nil, err
		}
		l.Items = append(l.Items, v)
		p.ws()
		if p.pos >= len(p.s) {
			return nil, p.errf("expecting ',' delimiter")
		}
		switch p.s[p.pos] {
		case ',':
			p.pos++
		case ']':
			p.pos++
			return l, nil
		default:
			return nil, p.errf("expecting ',' delimiter")
		}
	}
}

func (p *parser) object(depth int) (Value, error) {
	d := NewDict()
	p.ws()
	if p.pos < len(p.s) && p.s[p.pos] == '}' {
		p.pos++
		return d, nil
	}
	for {
		p.ws()
		if p.pos >= len(p.s) || p.s[p.pos] != '"' {
			return nil, p.errf("expecting property name enclosed in double quotes")
		}
		p.pos++
		k, err := p.str()
		if err != nil {
			return nil, err
		}
		p.ws()
		if p.pos >= len(p.s) || p.s[p.pos] != ':' {
			return nil, p.errf("expecting ':' delimiter")
		}
		p.pos++
		p.ws()
		v, err := p.value(depth + 1)
		if err != nil {
			return nil, err
		}
		d.Set(k, v)
		p.ws()
		if p.pos >= len(p.s) {
			return nil, p.errf("expecting ',' delimiter")
		}
		switch p.s[p.pos] {
		case ',':
			p.pos++
		case '}':
			p.pos++
			// kombu/utils/json.py: object_hook
			if d.Len() == 2 {
				_, a := d.Get("__type__")
				_, b := d.Get("__value__")
				if a && b {
					return nil, ErrTyped
				}
			}
			return d, nil
		default:
			return nil, p.errf("expecting ',' delimiter")
		}
	}
}
