package pyjson

import (
	"math/big"
	"strings"
)

// Port of celery/utils/saferepr.py as of Celery 5.7 (saferepr, _saferepr,
// reprstream) for
// the value types JSON can produce, plus the empty tuple Celery passes
// when a task has no positional arguments.

type tokKind int

const (
	tokValue tokKind = iota
	tokLiteral
	tokKey
	tokDirty
)

type tok struct {
	kind      tokKind
	val       Value  // tokValue
	lit       string // tokLiteral
	truncate  bool   // tokLiteral
	direction int    // tokLiteral
	key       Str    // tokKey
}

var (
	litDictStart = tok{kind: tokLiteral, lit: "{", direction: +1}
	litDictKVSep = tok{kind: tokLiteral, lit: ": ", truncate: true}
	litDictEnd   = tok{kind: tokLiteral, lit: "}", direction: -1}
	litListStart = tok{kind: tokLiteral, lit: "[", direction: +1}
	litListEnd   = tok{kind: tokLiteral, lit: "]", direction: -1}
	litListSep   = tok{kind: tokLiteral, lit: ", ", truncate: true}
	litTupStart  = tok{kind: tokLiteral, lit: "(", direction: +1}
	litTupEnd    = tok{kind: tokLiteral, lit: ")", direction: -1}
)

// EmptyTuple is Python's () (what `args or ()` yields for no arguments).
type EmptyTuple struct{}

// iter is a Python iterator over tokens; chained iterators are modelled by
// the next pointer (itertools.chain(..., it)).
type iter struct {
	toks []tok
	pos  int
	next *iter
}

func (it *iter) pop() (tok, bool) {
	for it != nil {
		if it.pos < len(it.toks) {
			t := it.toks[it.pos]
			it.pos++
			return t, true
		}
		if it.next == nil {
			return tok{}, false
		}
		// splice the continuation in so the iterator object stays the same
		*it = *it.next
	}
	return tok{}, false
}

func runeLen(s string) int { return len(Str(s).Runes()) }

// truncate is celery/utils/text.py: truncate(s, maxlen, suffix='...').
func truncate(s string, maxlen int) string {
	if maxlen != 0 {
		rs := Str(s).Runes()
		if len(rs) >= maxlen {
			head := string(FromRunes(rs[:maxlen]))
			if i := strings.LastIndexByte(head, ' '); i >= 0 {
				head = head[:i]
			}
			return head + "..."
		}
	}
	return s
}

// formatChars is _format_chars for str.
func formatChars(s Str, maxlen int, limited bool) string {
	v := string(s)
	if limited {
		v = truncate(v, maxlen)
	}
	return "'" + strings.ReplaceAll(v, "'", `\'`) + "'"
}

// SafeRepr is celery.utils.saferepr.saferepr(o, maxlen, maxlevels=3).
// maxlen < 0 means None (no limit).
func SafeRepr(o Value, maxlen int) string { return SafeReprLevels(o, maxlen, 3) }

// SafeReprLevels is saferepr with an explicit maxlevels
// (app.conf.task_repr_maxlevels); 0 disables the nesting limit.
func SafeReprLevels(o Value, maxlen, maxlevels int) string {
	limited := maxlen >= 0
	var sb strings.Builder
	stack := []*iter{{toks: []tok{{kind: tokValue, val: o}}}}
	level := 0
	emit := func(val string) {
		sb.WriteString(val)
		if limited {
			maxlen -= runeLen(val)
		}
	}
	var broke *iter
outer:
	for len(stack) > 0 {
		it := stack[0]
		stack = stack[1:]
		for {
			t, ok := it.pop()
			if !ok {
				break
			}
			// reprstream yields a token; _saferepr checks the budget
			// before rendering it.
			var val string
			switch t.kind {
			case tokDirty:
				continue
			case tokLiteral:
				level += t.direction
				if limited && maxlen <= 0 {
					if t.direction < 0 {
						// "the truncated value was the last one in its
						// container: nothing is left out, so just close it"
						sb.WriteString(t.lit)
						continue
					}
					broke = &iter{toks: []tok{t}, next: it}
					break outer
				}
				emit(t.lit)
				continue
			case tokKey:
				if limited && maxlen <= 0 {
					broke = &iter{toks: []tok{t}, next: it}
					break outer
				}
				if limited {
					val = SafeReprLevels(t.key, maxlen, maxlevels)
				} else {
					val = SafeReprLevels(t.key, -1, maxlevels)
				}
				emit(val)
				continue
			}
			var start, end tok
			var children []tok
			switch x := t.val.(type) {
			case nil:
				val = "None"
			case bool:
				if x {
					val = "True"
				} else {
					val = "False"
				}
			case *big.Int:
				val = x.String()
			case float64:
				val = FloatRepr(x)
			case Str:
				if limited && maxlen <= 0 {
					broke = &iter{toks: []tok{t}, next: it}
					break outer
				}
				emit(formatChars(x, maxlen, limited))
				continue
			case EmptyTuple:
				start, end = litTupStart, litTupEnd
			case *List:
				start, end = litListStart, litListEnd
				for i, v := range x.Items {
					children = append(children, tok{kind: tokValue, val: v})
					if i < len(x.Items)-1 {
						children = append(children, litListSep)
					}
				}
			case *Dict:
				start, end = litDictStart, litDictEnd
				for i, k := range x.Keys {
					children = append(children, tok{kind: tokKey, key: k}, litDictKVSep,
						tok{kind: tokValue, val: x.Vals[i]})
					if i < len(x.Keys)-1 {
						children = append(children, litListSep)
					}
				}
			default:
				panic("pyjson: unsupported value type in SafeRepr")
			}
			if start.kind == tokLiteral {
				if maxlevels != 0 && level >= maxlevels {
					// a literal: not truncated, never a "dangling paren"
					if limited && maxlen <= 0 {
						broke = &iter{toks: []tok{t}, next: it}
						break outer
					}
					emit(start.lit + "..." + end.lit)
					continue
				} else {
					toks := make([]tok, 0, len(children)+3)
					toks = append(toks, start)
					toks = append(toks, children...)
					toks = append(toks, tok{kind: tokDirty}, end)
					stack = append(stack, &iter{toks: toks, next: it})
					break
				}
			}
			if limited && maxlen <= 0 {
				broke = &iter{toks: []tok{t}, next: it}
				break outer
			}
			if limited {
				val = truncate(val, maxlen)
			}
			emit(val)
		}
	}
	if broke != nil {
		sb.WriteString(", ...")
		// "move rest back to stack, so that we can include dangling
		// parens. The token we just pulled goes back too: if it opens a
		// container, the pass below has to know."
		stack = append(stack, broke)
		depth := 0
		for _, rest := range stack {
			for {
				t, ok := rest.pop()
				if !ok {
					break
				}
				if t.kind == tokLiteral && !t.truncate {
					// "a container opened after the limit isn't shown,
					// so its closing paren is skipped as well"
					depth += t.direction
					if depth < 0 {
						depth = 0
						sb.WriteString(t.lit)
					}
				}
			}
		}
	}
	return sb.String()
}
