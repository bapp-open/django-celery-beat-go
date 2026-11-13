package message

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/bapp-open/django-celery-beat-go/internal/config"
	"github.com/bapp-open/django-celery-beat-go/internal/pyjson"
)

// RedisOptions are the kombu Redis transport options that affect where and
// how a message is stored (kombu/transport/redis.py: Channel).
type RedisOptions struct {
	PrioritySteps   []int  // priority_steps, default [0, 3, 6, 9]
	Sep             string // sep, default "\x06\x16"
	GlobalKeyPrefix string // global_keyprefix, default ""
}

// DefaultRedisOptions are kombu's defaults.
func DefaultRedisOptions() RedisOptions {
	return RedisOptions{PrioritySteps: []int{0, 3, 6, 9}, Sep: "\x06\x16"}
}

// RedisOptionsFrom reads broker_transport_options.
func RedisOptionsFrom(to map[string]any) (RedisOptions, error) {
	o := DefaultRedisOptions()
	if v, ok := to["sep"]; ok {
		s, ok := v.(string)
		if !ok {
			return o, fmt.Errorf("%w: broker_transport_options.sep", ErrUnsupported)
		}
		o.Sep = s
	}
	if v, ok := to["global_keyprefix"]; ok {
		s, ok := v.(string)
		if !ok {
			return o, fmt.Errorf("%w: broker_transport_options.global_keyprefix", ErrUnsupported)
		}
		o.GlobalKeyPrefix = s
	}
	if v, ok := to["priority_steps"]; ok {
		list, ok := v.([]any)
		if !ok || len(list) == 0 {
			return o, fmt.Errorf("%w: broker_transport_options.priority_steps", ErrUnsupported)
		}
		o.PrioritySteps = nil
		for _, it := range list {
			n, err := norm(it)
			i, isInt := n.(int64)
			if err != nil || !isInt {
				return o, fmt.Errorf("%w: broker_transport_options.priority_steps", ErrUnsupported)
			}
			o.PrioritySteps = append(o.PrioritySteps, int(i))
		}
	}
	return o, nil
}

// redisPriority is Channel._get_message_priority for the virtual
// transport: `priority or default_priority`, clamped to 0..9.
func (m *Message) redisPriority() int64 {
	if m.Priority == nil {
		return 0
	}
	return *m.Priority
}

// RedisQueueKey is Channel._q_for_pri(queue, pri) from
// kombu/transport/redis.py: the list a message for `queue` is LPUSHed to.
func (m *Message) RedisQueueKey(queue string, o RedisOptions) string {
	pri := m.redisPriority()
	if pri > 9 {
		pri = 9
	}
	if pri < 0 {
		pri = 0
	}
	// steps[bisect(steps, n) - 1]
	i := sort.Search(len(o.PrioritySteps), func(i int) bool { return int64(o.PrioritySteps[i]) > pri })
	step := o.PrioritySteps[(i-1+len(o.PrioritySteps))%len(o.PrioritySteps)]
	if step != 0 {
		return queue + o.Sep + strconv.Itoa(step)
	}
	return queue
}

// RedisPayload is the JSON document kombu's virtual transport stores:
// Channel.prepare_message + _inplace_augment_message
// (kombu/transport/virtual/base.py), serialised by kombu.utils.json.dumps.
func (m *Message) RedisPayload(deliveryTag string) []byte {
	b := make([]byte, 0, len(m.Body)*4/3+1024)
	b = append(b, `{"body": "`...)
	n := base64.StdEncoding.EncodedLen(len(m.Body))
	b = append(b, make([]byte, n)...)
	base64.StdEncoding.Encode(b[len(b)-n:], m.Body)
	b = append(b, `", "content-encoding": `...)
	b = pyjson.AppendString(b, pyjson.Str(m.ContentEncoding))
	b = append(b, `, "content-type": `...)
	b = pyjson.AppendString(b, pyjson.Str(m.ContentType))
	b = append(b, `, "headers": `...)
	b = pyjson.Append(b, m.Headers)
	b = append(b, `, "properties": {"correlation_id": `...)
	b = pyjson.AppendString(b, pyjson.Str(m.CorrelationID))
	b = append(b, `, "reply_to": `...)
	b = pyjson.AppendString(b, pyjson.Str(m.ReplyTo))
	b = append(b, `, "delivery_mode": `...)
	b = strconv.AppendInt(b, int64(m.DeliveryMode), 10)
	if m.Expiration != nil {
		b = append(b, `, "expiration": `...)
		b = pyjson.AppendString(b, pyjson.Str(*m.Expiration))
	}
	b = append(b, `, "delivery_info": {"exchange": `...)
	b = pyjson.AppendString(b, pyjson.Str(m.Exchange))
	b = append(b, `, "routing_key": `...)
	b = pyjson.AppendString(b, pyjson.Str(m.RoutingKey))
	b = append(b, `}, "priority": `...)
	b = strconv.AppendInt(b, m.redisPriority(), 10)
	b = append(b, `, "body_encoding": "base64", "delivery_tag": `...)
	b = pyjson.AppendString(b, pyjson.Str(deliveryTag))
	return append(b, "}}"...)
}

// RedisBinding is the member kombu SADDs to `_kombu.binding.<exchange>`
// when it declares a queue (kombu/transport/redis.py: Channel._queue_bind
// with the exchange type's prepare_bind).
func RedisBinding(q *config.Queue, o RedisOptions) (key, member string, err error) {
	rk := ""
	if q.RoutingKey != nil {
		rk = *q.RoutingKey
	}
	pattern := ""
	switch q.Exchange.Type {
	case "direct":
	case "topic":
		pattern = TopicPattern(rk)
	default:
		return "", "", fmt.Errorf("%w: %s exchange %q on Redis", ErrUnsupported, q.Exchange.Type, q.Exchange.Name)
	}
	return "_kombu.binding." + q.Exchange.Name, strings.Join([]string{rk, pattern, q.Name}, o.Sep), nil
}

// TopicPattern is kombu/transport/virtual/exchange.py:
// TopicExchange.key_to_pattern.
func TopicPattern(rkey string) string {
	// kombu.utils.text.escape_regex(rkey, '.#*')
	var esc strings.Builder
	for _, c := range rkey {
		switch {
		case unicode.IsLetter(c) || unicode.IsNumber(c) || c == '.' || c == '#' || c == '*':
			esc.WriteRune(c)
		case c == 0:
			esc.WriteString(`\000`)
		default:
			esc.WriteByte('\\')
			esc.WriteRune(c)
		}
	}
	words := strings.Split(esc.String(), ".")
	for i, w := range words {
		switch w {
		case "*":
			words[i] = `.*?[^\.]`
		case "#":
			words[i] = `.*?`
		}
	}
	return "^" + strings.Join(words, `\.`) + "$"
}

// RedisRoute returns the queues a message published to a named exchange
// is delivered to, given the members of `_kombu.binding.<exchange>`
// (kombu/transport/virtual/exchange.py: DirectExchange.lookup and
// TopicExchange.lookup). The result is sorted; Python iterates a set.
func RedisRoute(exchangeType string, members []string, routingKey string, o RedisOptions) ([]string, error) {
	seen := map[string]struct{}{}
	for _, mem := range members {
		parts := strings.Split(mem, o.Sep)
		if len(parts) != 3 {
			return nil, fmt.Errorf("%w: malformed binding %q", ErrUnsupported, mem)
		}
		rkey, pattern, queue := parts[0], parts[1], parts[2]
		switch exchangeType {
		case "direct":
			if rkey == routingKey {
				seen[queue] = struct{}{}
			}
		case "topic":
			re, err := regexp.Compile(pattern)
			if err != nil {
				return nil, fmt.Errorf("%w: binding pattern %q: %v", ErrUnsupported, pattern, err)
			}
			if loc := re.FindStringIndex(routingKey); loc != nil && loc[0] == 0 {
				seen[queue] = struct{}{}
			}
		default:
			return nil, fmt.Errorf("%w: %s exchange on Redis", ErrUnsupported, exchangeType)
		}
	}
	out := make([]string, 0, len(seen))
	for q := range seen {
		if q != "" {
			out = append(out, q)
		}
	}
	sort.Strings(out)
	return out, nil
}

// --- AMQP ------------------------------------------------------------------

func shortstr(b []byte, s string, what string) ([]byte, error) {
	if len(s) > 255 {
		return nil, fmt.Errorf("%w: %s longer than 255 bytes cannot be sent over AMQP", ErrUnsupported, what)
	}
	b = append(b, byte(len(s)))
	return append(b, s...), nil
}

// amqpItem is amqp/serialization.py: _write_item.
func amqpItem(b []byte, v pyjson.Value) ([]byte, error) {
	var err error
	switch x := v.(type) {
	case nil:
		return append(b, 'V'), nil
	case pyjson.Str:
		b = append(b, 'S')
		b = binary.BigEndian.AppendUint32(b, uint32(len(x)))
		return append(b, x...), nil
	case bool:
		if x {
			return append(b, 't', 1), nil
		}
		return append(b, 't', 0), nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, fmt.Errorf("%w: NaN/Infinity in headers is rejected by RabbitMQ", ErrUnsupported)
		}
		b = append(b, 'd')
		return binary.BigEndian.AppendUint64(b, math.Float64bits(x)), nil
	case *big.Int:
		if !x.IsInt64() {
			return nil, fmt.Errorf("%w: integer %s in headers does not fit an AMQP long long", ErrUnsupported, x)
		}
		n := x.Int64()
		if n > 2147483647 || n < -2147483647 {
			b = append(b, 'L')
			return binary.BigEndian.AppendUint64(b, uint64(n)), nil
		}
		b = append(b, 'I')
		return binary.BigEndian.AppendUint32(b, uint32(int32(n))), nil
	case *pyjson.Dict:
		b = append(b, 'F')
		return amqpTable(b, x)
	case *pyjson.List:
		b = append(b, 'A')
		start := len(b)
		b = append(b, 0, 0, 0, 0)
		for _, it := range x.Items {
			if b, err = amqpItem(b, it); err != nil {
				return nil, err
			}
		}
		binary.BigEndian.PutUint32(b[start:], uint32(len(b)-start-4))
		return b, nil
	}
	return nil, fmt.Errorf("%w: header value of type %T", ErrUnsupported, v)
}

// amqpTable is amqp/serialization.py: _write_table (keys in dict order).
func amqpTable(b []byte, d *pyjson.Dict) ([]byte, error) {
	var err error
	start := len(b)
	b = append(b, 0, 0, 0, 0)
	for i, k := range d.Keys {
		if b, err = shortstr(b, string(k), "header name"); err != nil {
			return nil, err
		}
		if b, err = amqpItem(b, d.Vals[i]); err != nil {
			return nil, err
		}
	}
	binary.BigEndian.PutUint32(b[start:], uint32(len(b)-start-4))
	return b, nil
}

// AMQPProperties returns the property flags and property list of the
// content header frame, byte-for-byte what py-amqp's
// GenericContent._serialize_properties emits for the amqp.Message kombu
// builds (kombu/transport/pyamqp.py: Channel.prepare_message).
func (m *Message) AMQPProperties() ([]byte, error) {
	var (
		flags uint16 = 1<<15 | 1<<14 | 1<<13 | 1<<12 | 1<<10 | 1<<9
		err   error
	)
	// Producer.publish(priority=0) by default; None omits the property.
	var prio *int64
	if !m.PrioritySet {
		zero := int64(0)
		prio = &zero
	} else {
		prio = m.Priority
	}
	if prio != nil {
		if *prio < 0 || *prio > 255 {
			return nil, fmt.Errorf("%w: priority %d does not fit an AMQP octet", ErrUnsupported, *prio)
		}
		flags |= 1 << 11
	}
	if m.Expiration != nil {
		flags |= 1 << 8
	}
	b := binary.BigEndian.AppendUint16(make([]byte, 0, 512), flags)
	if b, err = shortstr(b, m.ContentType, "content_type"); err != nil {
		return nil, err
	}
	if b, err = shortstr(b, m.ContentEncoding, "content_encoding"); err != nil {
		return nil, err
	}
	if b, err = amqpTable(b, m.Headers); err != nil {
		return nil, err
	}
	b = append(b, byte(m.DeliveryMode))
	if prio != nil {
		b = append(b, byte(*prio))
	}
	if b, err = shortstr(b, m.CorrelationID, "task id"); err != nil {
		return nil, err
	}
	if b, err = shortstr(b, m.ReplyTo, "reply_to"); err != nil {
		return nil, err
	}
	if m.Expiration != nil {
		if b, err = shortstr(b, *m.Expiration, "expiration"); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// AMQPTable encodes a plain argument table (queue/exchange/binding
// arguments from the settings document) the way py-amqp would.
func AMQPTable(args map[string]any) ([]byte, error) {
	d := pyjson.NewDict()
	keys := make([]string, 0, len(args))
	for k := range args {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v, err := plainToValue(args[k])
		if err != nil {
			return nil, err
		}
		d.Set(pyjson.Str(k), v)
	}
	return amqpTable(nil, d)
}

func plainToValue(v any) (pyjson.Value, error) {
	switch x := v.(type) {
	case nil, bool, float64:
		return x, nil
	case string:
		return pyjson.Str(x), nil
	case int64:
		return big.NewInt(x), nil
	case json.Number:
		n, err := norm(x)
		if err != nil {
			return nil, err
		}
		return plainToValue(n)
	case []any:
		l := &pyjson.List{}
		for _, it := range x {
			iv, err := plainToValue(it)
			if err != nil {
				return nil, err
			}
			l.Items = append(l.Items, iv)
		}
		return l, nil
	case map[string]any:
		d := pyjson.NewDict()
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			iv, err := plainToValue(x[k])
			if err != nil {
				return nil, err
			}
			d.Set(pyjson.Str(k), iv)
		}
		return d, nil
	}
	return nil, fmt.Errorf("%w: argument value %T", ErrUnsupported, v)
}

// --- journal form -------------------------------------------------------------

// stored is the JSON kept in django_gobeat_dispatch.message. Headers are
// kept as Python-JSON *text* because they may contain NaN/Infinity and
// key order matters; neither survives jsonb.
type stored struct {
	V               int           `json:"v"`
	TaskID          string        `json:"task_id"`
	Task            string        `json:"task"`
	Name            string        `json:"name"`
	Exchange        string        `json:"exchange"`
	RoutingKey      string        `json:"routing_key"`
	Queue           *config.Queue `json:"queue"`
	HeadersJSON     string        `json:"headers_json"`
	CorrelationID   string        `json:"correlation_id"`
	ReplyTo         string        `json:"reply_to"`
	DeliveryMode    int           `json:"delivery_mode"`
	PrioritySet     bool          `json:"priority_set"`
	Priority        *int64        `json:"priority"`
	Expiration      *string       `json:"expiration"`
	ContentType     string        `json:"content_type"`
	ContentEncoding string        `json:"content_encoding"`
	BodyB64         string        `json:"body_b64"`
}

// MarshalJSON renders the journal form.
func (m *Message) MarshalJSON() ([]byte, error) {
	return json.Marshal(stored{
		V: 1, TaskID: m.TaskID, Task: m.Task, Name: m.Name,
		Exchange: m.Exchange, RoutingKey: m.RoutingKey, Queue: m.Queue,
		HeadersJSON:   string(pyjson.Dumps(m.Headers)),
		CorrelationID: m.CorrelationID, ReplyTo: m.ReplyTo, DeliveryMode: m.DeliveryMode,
		PrioritySet: m.PrioritySet, Priority: m.Priority, Expiration: m.Expiration,
		ContentType: m.ContentType, ContentEncoding: m.ContentEncoding,
		BodyB64: base64.StdEncoding.EncodeToString(m.Body),
	})
}

// UnmarshalJSON restores a message from its journal form.
func (m *Message) UnmarshalJSON(data []byte) error {
	var s stored
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber() // queue arguments must keep int vs float
	if err := dec.Decode(&s); err != nil {
		return err
	}
	if s.V != 1 {
		return fmt.Errorf("journal message version %d is not supported", s.V)
	}
	hv, err := pyjson.Loads(s.HeadersJSON)
	if err != nil {
		return fmt.Errorf("journal headers: %w", err)
	}
	h, ok := hv.(*pyjson.Dict)
	if !ok {
		return fmt.Errorf("journal headers are not an object")
	}
	body, err := base64.StdEncoding.DecodeString(s.BodyB64)
	if err != nil {
		return fmt.Errorf("journal body: %w", err)
	}
	*m = Message{TaskID: s.TaskID, Task: s.Task, Name: s.Name, Exchange: s.Exchange,
		RoutingKey: s.RoutingKey, Queue: s.Queue, Headers: h, CorrelationID: s.CorrelationID,
		ReplyTo: s.ReplyTo, DeliveryMode: s.DeliveryMode, PrioritySet: s.PrioritySet,
		Priority: s.Priority, Expiration: s.Expiration, ContentType: s.ContentType,
		ContentEncoding: s.ContentEncoding, Body: body}
	return nil
}
