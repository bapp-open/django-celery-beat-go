package schedule

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"
)

// ErrParse mirrors celery.schedules.ParseException; ErrValue mirrors the
// ValueError raised for out-of-range or unparsable numbers. Both make the
// crontab invalid; Python beat would refuse the entry as well.
var (
	ErrParse = errors.New("crontab parse error")
	ErrValue = errors.New("crontab value error")
)

// isWord reports whether r matches Python's regex \w for str patterns
// (alphanumeric in the Unicode sense, or underscore).
func isWord(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r)
}

// wordRun returns the length in bytes of the leading run of \w runes.
func wordRun(s string) int {
	for i, r := range s {
		if !isWord(r) {
			return i
		}
	}
	return len(s)
}

// ParseField expands one crontab field to the sorted set of values it
// selects. Port of celery/schedules.py: crontab_parser.parse and
// crontab._expand_cronspec (str branch). max is the number of values
// (60, 24, 7, 31, 12) and min the smallest value (0 or 1).
func ParseField(spec string, max, min int) ([]int, error) {
	set := make(map[int]struct{})
	for _, part := range strings.Split(spec, ",") {
		if part == "" {
			return nil, fmt.Errorf("%w: empty part", ErrParse)
		}
		vals, err := parsePart(part, max, min)
		if err != nil {
			return nil, err
		}
		for _, v := range vals {
			set[v] = struct{}{}
		}
	}
	out := make([]int, 0, len(set))
	for v := range set {
		// crontab._expand_cronspec: "assure the result does not precede
		// the min or exceed the max".
		if v >= max+min || v < min {
			return nil, fmt.Errorf("%w: %d outside %d-%d", ErrValue, v, min, max-1+min)
		}
		out = append(out, v)
	}
	sort.Ints(out)
	return out, nil
}

// parsePart ports crontab_parser._parse_part. The four regexes are tried
// in order with re.match (anchored at the start only, so trailing text
// after a match is ignored exactly as in Python):
//
//	(\w+?)-(\w+)/(\w+)?   range with steps
//	(\w+?)-(\w+)          range
//	\*/(\w+)?             star with steps
//	^\*$                  star
func parsePart(part string, max, min int) ([]int, error) {
	// range forms: a leading word run immediately followed by '-' and
	// another word run.
	if n := wordRun(part); n > 0 && n < len(part) && part[n] == '-' {
		rest := part[n+1:]
		if m := wordRun(rest); m > 0 {
			from, to := part[:n], rest[:m]
			after := rest[m:]
			if strings.HasPrefix(after, "/") {
				steps := after[1:]
				steps = steps[:wordRun(steps)]
				if steps == "" {
					return nil, fmt.Errorf("%w: empty filter", ErrParse)
				}
				r, err := expandRange(from, to, true, max, min)
				if err != nil {
					return nil, err
				}
				return stepSlice(r, steps)
			}
			return expandRange(from, to, true, max, min)
		}
	}
	if strings.HasPrefix(part, "*/") {
		steps := part[2:]
		steps = steps[:wordRun(steps)]
		if steps == "" {
			return nil, fmt.Errorf("%w: empty filter", ErrParse)
		}
		return stepSlice(expandStar(max, min), steps)
	}
	if part == "*" || part == "*\n" { // '$' also matches before a final newline
		return expandStar(max, min), nil
	}
	return expandRange(part, "", false, max, min)
}

func expandStar(max, min int) []int {
	out := make([]int, 0, max)
	for i := min; i < max+min; i++ {
		out = append(out, i)
	}
	return out
}

// stepSlice is list[::int(steps)].
func stepSlice(vals []int, steps string) ([]int, error) {
	n, ok := pyInt(steps)
	if !ok {
		return nil, fmt.Errorf("%w: invalid step %q", ErrValue, steps)
	}
	if n == 0 {
		return nil, fmt.Errorf("%w: slice step cannot be zero", ErrValue)
	}
	if n < 0 { // unreachable through \w+, kept for safety: [::-n] reverses
		return nil, fmt.Errorf("%w: negative step", ErrValue)
	}
	if n >= int64(len(vals)) {
		return vals[:1], nil
	}
	out := make([]int, 0, len(vals)/int(n)+1)
	for i := 0; i < len(vals); i += int(n) {
		out = append(out, vals[i])
	}
	return out, nil
}

// expandRange ports crontab_parser._expand_range, including wrap-around
// ranges such as fri-mon or 22-2.
func expandRange(from, to string, hasTo bool, max, min int) ([]int, error) {
	fr, err := expandNumber(from, max, min)
	if err != nil {
		return nil, err
	}
	if !hasTo {
		return []int{fr}, nil
	}
	t, err := expandNumber(to, max, min)
	if err != nil {
		return nil, err
	}
	var out []int
	if t < fr {
		for i := fr; i < min+max; i++ {
			out = append(out, i)
		}
		for i := min; i <= t; i++ {
			out = append(out, i)
		}
		return out, nil
	}
	for i := fr; i <= t; i++ {
		out = append(out, i)
	}
	return out, nil
}

var monthNames = map[string]int{"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
var dayNames = map[string]int{"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6}

// first3Lower is name[0:3].lower() (celery/utils/time.py: weekday,
// yearmonth). Only ASCII letters can produce a match.
func first3Lower(s string) string {
	r := []rune(s)
	if len(r) > 3 {
		r = r[:3]
	}
	return strings.ToLower(string(r))
}

// expandNumber ports crontab_parser._expand_number: int(), then month
// name, then weekday name (in that order, for every field).
func expandNumber(s string, max, min int) (int, error) {
	if strings.HasPrefix(s, "-") {
		return 0, fmt.Errorf("%w: negative numbers not supported", ErrParse)
	}
	var i int64
	if n, ok := pyInt(s); ok {
		i = n
	} else if m, ok := monthNames[first3Lower(s)]; ok {
		i = int64(m)
	} else if d, ok := dayNames[first3Lower(s)]; ok {
		i = int64(d)
	} else {
		return 0, fmt.Errorf("%w: invalid literal %q", ErrValue, s)
	}
	if i > int64(min+max-1) {
		return 0, fmt.Errorf("%w: invalid end range: %d > %d", ErrValue, i, min+max-1)
	}
	if i < int64(min) {
		return 0, fmt.Errorf("%w: invalid beginning range: %d < %d", ErrValue, i, min)
	}
	return int(i), nil
}

// pySpace is Py_UNICODE_ISSPACE.
func pySpace(r rune) bool {
	return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f)
}

// digitValue returns the decimal value of a Unicode Nd rune. Every Nd
// block is a run of digits zero..nine (possibly several runs back to
// back), so the value is the offset inside the contiguous run modulo 10.
func digitValue(r rune) int {
	start := r
	for start > 0 && unicode.IsDigit(start-1) {
		start--
	}
	return int(r-start) % 10
}

// pyInt is Python's int(str) for base 10: surrounding whitespace, an
// optional sign, Unicode decimal digits and single underscores between
// digits are accepted. Values that do not fit in int64 saturate (any such
// value is out of range for every crontab field anyway).
func pyInt(s string) (int64, bool) {
	rs := []rune(s)
	i, j := 0, len(rs)
	for i < j && pySpace(rs[i]) {
		i++
	}
	for j > i && pySpace(rs[j-1]) {
		j--
	}
	rs = rs[i:j]
	if len(rs) == 0 {
		return 0, false
	}
	neg := false
	if rs[0] == '+' || rs[0] == '-' {
		neg = rs[0] == '-'
		rs = rs[1:]
	}
	if len(rs) == 0 {
		return 0, false
	}
	var n int64
	prevDigit := false
	for _, r := range rs {
		switch {
		case r == '_':
			if !prevDigit {
				return 0, false
			}
			prevDigit = false
		case unicode.IsDigit(r):
			d := int64(digitValue(r))
			if n > (1<<62)/10 {
				n = 1 << 62
			} else {
				n = n*10 + d
			}
			prevDigit = true
		default:
			return 0, false
		}
	}
	if !prevDigit {
		return 0, false
	}
	if neg {
		n = -n
	}
	return n, true
}
