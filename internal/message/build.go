// Package message builds Celery task messages (protocol v2) from
// PeriodicTask rows exactly as Python beat does.
//
// The call chain reproduced here, with the pinned source files:
//
//	celery/beat.py            Scheduler.apply_async
//	celery/app/task.py        Task.apply_async          (option merge)
//	celery/app/base.py        Celery.send_task          (expires, routing)
//	celery/app/routes.py      Router.route, expand_destination
//	celery/app/amqp.py        AMQP.as_task_v2, send_task_message, Queues
//	kombu/messaging.py        Producer.publish, _publish
//	kombu/serialization.py    json serializer; kombu/compression.py
//
// Every case is checked against real Celery output in golden_test.go.
package message

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/bapp-open/django-celery-beat-go/internal/config"
	"github.com/bapp-open/django-celery-beat-go/internal/pyjson"
	"github.com/bapp-open/django-celery-beat-go/internal/pytime"
)

// ErrUnsupported marks inputs gobeat refuses to publish rather than guess.
var ErrUnsupported = errors.New("unsupported")

// ErrNoDestination is returned when Celery's own routing ends with no
// queue and no usable exchange/routing key (a route or override that sets
// only one of them on a direct exchange). Python then publishes to the
// queue named "" and the message is lost; gobeat refuses instead.
var ErrNoDestination = fmt.Errorf("%w: routing resolves to no destination queue (Python beat would publish to an empty queue name and lose the message)", ErrUnsupported)

// ErrQueueNotFound mirrors celery.exceptions.QueueNotFound.
var ErrQueueNotFound = errors.New("queue missing from task_queues")

// Input is the part of a PeriodicTask row that shapes the message
// (django_celery_beat/schedulers.py: ModelEntry.__init__).
type Input struct {
	ID            int64
	Name          string
	Task          string
	Args          string // raw JSON text; "" means '[]'
	Kwargs        string // raw JSON text; "" means '{}'
	Headers       *string
	Queue         *string
	Exchange      *string
	RoutingKey    *string
	Priority      *int64
	Expires       *time.Time
	ExpireSeconds *int64
}

// Message is a fully rendered task message plus its destination.
type Message struct {
	TaskID string
	Task   string
	Name   string

	Exchange   string
	RoutingKey string
	// Queue is the entity kombu would declare before publishing
	// (send_task_message: declare=[queue]); nil when there is none.
	Queue *config.Queue

	Headers       *pyjson.Dict
	CorrelationID string
	ReplyTo       string
	DeliveryMode  int
	// PrioritySet is false when Producer.publish receives its default
	// priority=0; true when an explicit value (possibly None == nil
	// Priority) is passed. AMQP omits the property only for None.
	PrioritySet bool
	Priority    *int64
	Expiration  *string

	ContentType     string
	ContentEncoding string
	Body            []byte
}

// Builder turns inputs into messages for one settings document.
type Builder struct {
	S     *config.Settings
	AppTZ *time.Location
	// Origin is the `origin` header (celery/utils/nodenames.py:
	// anon_nodename, "gen<pid>@<hostname>").
	Origin string
	// ReplyTo is the reply_to property (Celery.thread_oid).
	ReplyTo string

	queues map[string]*config.Queue
}

// NewBuilder validates the settings and returns a builder.
func NewBuilder(s *config.Settings, origin, replyTo string) (*Builder, error) {
	tz, err := s.AppTimezone()
	if err != nil {
		return nil, err
	}
	b := &Builder{S: s, AppTZ: tz, Origin: origin, ReplyTo: replyTo, queues: map[string]*config.Queue{}}
	for name, q := range s.Queues {
		b.queues[name] = q
	}
	return b, nil
}

// opts is a Python keyword dict: presence matters, nil is None.
type opts map[string]any

func (o opts) pop(k string) any {
	v := o[k]
	delete(o, k)
	return v
}

func (o opts) has(k string) bool { _, ok := o[k]; return ok }

func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case string:
		return x != ""
	case bool:
		return x
	case int64:
		return x != 0
	case float64:
		return x != 0
	}
	return true
}

// norm converts a decoded settings value to nil, string, bool, int64 or
// float64.
func norm(v any) (any, error) {
	switch x := v.(type) {
	case nil, string, bool, int64, float64:
		return x, nil
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i, nil
		}
		f, err := x.Float64()
		if err != nil {
			return nil, fmt.Errorf("%w: number %q", ErrUnsupported, x.String())
		}
		return f, nil
	case int:
		return int64(x), nil
	}
	return nil, fmt.Errorf("%w: option value %v (%T)", ErrUnsupported, v, v)
}

// queue is Queues.__getitem__ / __missing__ (celery/app/amqp.py).
func (b *Builder) queue(name string) (*config.Queue, error) {
	if q, ok := b.queues[name]; ok {
		if q.Unsupported != "" {
			return nil, fmt.Errorf("%w: %s", ErrUnsupported, q.Unsupported)
		}
		return q, nil
	}
	if !b.S.TaskCreateMissingQueues {
		return nil, fmt.Errorf("%w: %q", ErrQueueNotFound, name)
	}
	// Queues.new_missing: Queue(name, Exchange(name), name); _add applies
	// task_queue_max_priority.
	rk := name
	q := &config.Queue{
		Name: name, RoutingKey: &rk, Durable: true,
		QueueArguments: map[string]any{},
		Exchange:       config.Exchange{Name: name, Type: "direct", Durable: true},
	}
	if mp, _ := norm(b.S.TaskQueueMaxPriority); mp != nil {
		q.QueueArguments["x-max-priority"] = mp
	}
	b.queues[name] = q
	return q, nil
}

// expand is Router.expand_destination for an options dict.
func (b *Builder) expand(o opts) error {
	q := o.pop("queue")
	if !truthy(q) {
		return nil
	}
	switch x := q.(type) {
	case *config.Queue:
		o["queue"] = x
	case string:
		qo, err := b.queue(x)
		if err != nil {
			return err
		}
		o["queue"] = qo
	default:
		return fmt.Errorf("%w: queue option %v", ErrUnsupported, q)
	}
	return nil
}

// lpmerge is celery.utils.functional.lpmerge: keep L, overlay the
// non-None values of R.
func lpmerge(l, r opts) opts {
	for k, v := range r {
		if v != nil {
			l[k] = v
		}
	}
	return l
}

func deliveryMode(v any) (int, error) {
	// kombu/entity.py: maybe_delivery_mode
	switch x := v.(type) {
	case int64:
		return int(x), nil
	case string:
		switch x {
		case "persistent":
			return 2, nil
		case "transient":
			return 1, nil
		}
	}
	return 0, fmt.Errorf("%w: delivery_mode %v", ErrUnsupported, v)
}

func toValue(v any) pyjson.Value {
	switch x := v.(type) {
	case int64:
		return big.NewInt(x)
	case string:
		return pyjson.Str(x)
	}
	return v // nil, bool, float64
}

// Build renders the message Python beat would send for in at instant now
// with the given task id.
func (b *Builder) Build(in Input, taskID string, now time.Time) (*Message, error) {
	ti := b.S.Tasks[in.Task]
	if ti != nil && ti.Unsupported != "" {
		return nil, fmt.Errorf("%w: %s", ErrUnsupported, ti.Unsupported)
	}
	if ti == nil && b.S.HasRoutes {
		return nil, fmt.Errorf("%w: task %q is unknown to the settings row and task_routes is configured; re-run `manage.py gobeat_sync`", ErrUnsupported, in.Task)
	}
	registered := ti != nil && ti.Registered

	// --- ModelEntry.__init__ ------------------------------------------
	argsText, kwargsText, headersText := in.Args, in.Kwargs, "{}"
	if argsText == "" {
		argsText = "[]"
	}
	if kwargsText == "" {
		kwargsText = "{}"
	}
	if in.Headers != nil && *in.Headers != "" {
		headersText = *in.Headers
	}
	argsV, err := pyjson.Loads(argsText)
	if err != nil {
		return nil, fmt.Errorf("args: %w", err)
	}
	kwargsV, err := pyjson.Loads(kwargsText)
	if err != nil {
		return nil, fmt.Errorf("kwargs: %w", err)
	}
	headersV, err := pyjson.Loads(headersText)
	if err != nil {
		return nil, fmt.Errorf("headers: %w", err)
	}
	args, ok := argsV.(*pyjson.List)
	if !ok {
		return nil, fmt.Errorf("%w: args must be a JSON array", ErrUnsupported)
	}
	kwargs, ok := kwargsV.(*pyjson.Dict)
	if !ok {
		return nil, fmt.Errorf("%w: kwargs must be a JSON object", ErrUnsupported)
	}
	entryHeaders, ok := headersV.(*pyjson.Dict)
	if !ok {
		return nil, fmt.Errorf("%w: headers must be a JSON object", ErrUnsupported)
	}
	entryHeaders.Set("periodic_task_name", pyjson.Str(in.Name))
	// celery/beat.py: Scheduler.apply_async -
	// options.setdefault('headers', {})['celery_beat_task'] = True
	entryHeaders.Set("celery_beat_task", true)

	// --- Task.apply_async: dict(preopts, **options) ----------------------
	o := opts{}
	if registered {
		for _, k := range [...]string{"queue", "routing_key", "exchange", "priority", "expires",
			"serializer", "delivery_mode", "compression", "time_limit", "soft_time_limit",
			"immediate", "mandatory"} {
			v, err := norm(ti.Options[k])
			if err != nil {
				return nil, fmt.Errorf("task option %s: %w", k, err)
			}
			o[k] = v
		}
		o["ignore_result"] = ti.IgnoreResult
	}
	if in.Queue != nil {
		o["queue"] = *in.Queue
	}
	if in.Exchange != nil {
		o["exchange"] = *in.Exchange
	}
	if in.RoutingKey != nil {
		o["routing_key"] = *in.RoutingKey
	}
	if in.Priority != nil {
		o["priority"] = *in.Priority
	}
	// PeriodicTask.expires_: `self.expires or self.expire_seconds`
	if in.Expires != nil {
		o["expires"] = *in.Expires
	} else if in.ExpireSeconds != nil && *in.ExpireSeconds != 0 {
		o["expires"] = *in.ExpireSeconds
	}

	// --- Celery.send_task --------------------------------------------------
	expires := o.pop("expires")
	timeLimit := o.pop("time_limit")
	softTimeLimit := o.pop("soft_time_limit")
	// options.pop('ignore_result', None): None for a task that is not
	// registered, the task's own setting otherwise
	var ignoreResult pyjson.Value
	if v, ok := o.pop("ignore_result").(bool); ok {
		ignoreResult = v
	}

	// Router.route
	if err := b.expand(o); err != nil {
		return nil, err
	}
	routed := false
	if ti != nil && len(ti.Route) > 0 {
		r := opts{}
		for k, v := range ti.Route {
			nv, err := norm(v)
			if err != nil {
				return nil, fmt.Errorf("route %s: %w", k, err)
			}
			r[k] = nv
		}
		if err := b.expand(r); err != nil {
			return nil, err
		}
		o = lpmerge(r, o)
		routed = true
	}
	if !routed && !o.has("queue") {
		r := opts{"queue": b.S.TaskDefaultQueue}
		if err := b.expand(r); err != nil {
			return nil, err
		}
		o = lpmerge(r, o)
	}

	// expires -> expiration / header
	var expiresHeader pyjson.Value
	switch x := expires.(type) {
	case nil:
	case time.Time:
		// (maybe_make_aware(expires) - self.now()).total_seconds()
		s := pytime.TotalSeconds(x.Sub(now))
		if s < 0 {
			s = 0
		}
		o["expiration"] = s
		if b.S.UseTZ {
			expiresHeader = pyjson.Str(pytime.IsoFormat(x, time.UTC))
		} else {
			expiresHeader = pyjson.Str(pytime.IsoFormatNaive(pytime.InZone(x, time.UTC)))
		}
	case int64, float64:
		var secs float64
		var d time.Duration
		if i, ok := x.(int64); ok {
			secs = float64(i)
			if i > math.MaxInt64/int64(time.Second) || i < -math.MaxInt64/int64(time.Second) {
				return nil, fmt.Errorf("%w: expires=%d is out of range", ErrUnsupported, i)
			}
			d = time.Duration(i) * time.Second
			if i < 0 {
				o["expiration"] = int64(0)
			} else {
				o["expiration"] = i
			}
		} else {
			secs = x.(float64)
			if math.IsNaN(secs) || math.Abs(secs) > 1e11 {
				return nil, fmt.Errorf("%w: expires=%v is out of range", ErrUnsupported, secs)
			}
			// timedelta(seconds=float) rounds to microseconds, half-even
			d = time.Duration(math.RoundToEven(secs*1e6)) * time.Microsecond
			if secs < 0 {
				o["expiration"] = float64(0)
			} else {
				o["expiration"] = secs
			}
		}
		if secs < -2147483648 { // AMQP._verify_seconds
			return nil, fmt.Errorf("%w: expires is out of range", ErrUnsupported)
		}
		// add_seconds_to_datetime(app.now(), expires): elapsed time,
		// rendered in app.timezone.
		expiresHeader = pyjson.Str(pytime.IsoFormat(now.Add(d), b.AppTZ))
	default:
		return nil, fmt.Errorf("%w: expires=%v", ErrUnsupported, expires)
	}

	// --- AMQP.as_task_v2 ---------------------------------------------------
	levels := 3
	if b.S.TaskReprMaxLevels != nil {
		levels = *b.S.TaskReprMaxLevels
	}
	var argsRepr string
	if len(args.Items) == 0 {
		argsRepr = pyjson.SafeReprLevels(pyjson.EmptyTuple{}, 1024, levels) // `args or ()`
	} else {
		argsRepr = pyjson.SafeReprLevels(args, 1024, levels)
	}
	kwargsRepr := pyjson.SafeReprLevels(kwargs, 1024, levels)

	h := pyjson.NewDict()
	h.Set("lang", pyjson.Str("py"))
	h.Set("task", pyjson.Str(in.Task))
	h.Set("id", pyjson.Str(taskID))
	h.Set("shadow", nil)
	h.Set("eta", nil)
	h.Set("expires", expiresHeader)
	h.Set("group", nil)
	h.Set("group_index", nil)
	h.Set("retries", pyjson.Int(0))
	h.Set("timelimit", &pyjson.List{Items: []pyjson.Value{toValue(timeLimit), toValue(softTimeLimit)}})
	h.Set("root_id", pyjson.Str(taskID))
	h.Set("parent_id", nil)
	h.Set("argsrepr", pyjson.Str(argsRepr))
	h.Set("kwargsrepr", pyjson.Str(kwargsRepr))
	h.Set("origin", pyjson.Str(b.Origin))
	h.Set("ignore_result", ignoreResult)
	h.Set("replaced_task_nesting", pyjson.Int(0))
	h.Set("stamped_headers", nil)
	h.Set("stamps", pyjson.NewDict())

	embed := pyjson.NewDict()
	embed.Set("callbacks", nil)
	embed.Set("errbacks", nil)
	embed.Set("chain", nil)
	embed.Set("chord", nil)
	body := pyjson.Dumps(&pyjson.List{Items: []pyjson.Value{args, kwargs, embed}})

	// --- send_task_message ---------------------------------------------------
	// headers2.update(headers)
	for i, k := range entryHeaders.Keys {
		h.Set(k, entryHeaders.Vals[i])
	}
	var queue *config.Queue
	if q, ok := o["queue"].(*config.Queue); ok {
		queue = q
	}
	exchange, hasExchange := o["exchange"].(string)
	routingKey, _ := o["routing_key"].(string)
	if queue == nil && !hasExchange {
		if queue, err = b.queue(b.S.TaskDefaultQueue); err != nil {
			return nil, err
		}
	}
	dm := o["delivery_mode"]
	if dm == nil && queue != nil && queue.Exchange.DeliveryMode != nil {
		dm = int64(*queue.Exchange.DeliveryMode)
	}
	if !truthy(dm) {
		if dm, err = norm(b.S.TaskDefaultDeliveryMode); err != nil {
			return nil, err
		}
	}
	exchangeType, _ := o["exchange_type"].(string)
	if o["exchange_type"] == nil {
		exchangeType = "direct"
		if queue != nil {
			exchangeType = queue.Exchange.Type
		}
	}
	if (exchange == "" || routingKey == "") && exchangeType == "direct" {
		// "convert to anon-exchange, when exchange not set and direct ex."
		if queue == nil {
			return nil, ErrNoDestination
		}
		exchange, routingKey = "", queue.Name
	} else if !hasExchange {
		exchange = queue.Exchange.Name
		if exchange == "" && b.S.TaskDefaultExchange != nil {
			exchange = *b.S.TaskDefaultExchange
		}
		if routingKey == "" && queue.RoutingKey != nil {
			routingKey = *queue.RoutingKey
		}
		if routingKey == "" && b.S.TaskDefaultRoutingKey != nil {
			routingKey = *b.S.TaskDefaultRoutingKey
		}
	}

	serializer, _ := o["serializer"].(string)
	if serializer == "" {
		serializer = b.S.TaskSerializer
	}
	if serializer != "json" {
		return nil, fmt.Errorf("%w: serializer %q (only json is supported)", ErrUnsupported, serializer)
	}
	compression, _ := o["compression"].(string)
	if compression == "" && b.S.TaskCompression != nil {
		compression = *b.S.TaskCompression
	}

	// --- Producer.publish ------------------------------------------------------
	m := &Message{
		TaskID: taskID, Task: in.Task, Name: in.Name,
		Exchange: exchange, RoutingKey: routingKey, Queue: queue,
		Headers: h, CorrelationID: taskID, ReplyTo: b.ReplyTo,
		ContentType: "application/json", ContentEncoding: "utf-8",
	}
	if dm == nil {
		m.DeliveryMode = 2 // PERSISTENT_DELIVERY_MODE
	} else if m.DeliveryMode, err = deliveryMode(dm); err != nil {
		return nil, err
	}
	if o.has("priority") {
		m.PrioritySet = true
		switch p := o["priority"].(type) {
		case nil:
		case int64:
			m.Priority = &p
		default:
			return nil, fmt.Errorf("%w: priority %v is not an integer", ErrUnsupported, p)
		}
	}
	switch e := o["expiration"].(type) {
	case nil:
	case int64:
		if e > math.MaxInt64/1000 {
			return nil, fmt.Errorf("%w: expires is out of range", ErrUnsupported)
		}
		s := strconv.FormatInt(e*1000, 10)
		m.Expiration = &s
	case float64:
		s := strconv.FormatInt(int64(e*1000), 10) // str(int(expiration * 1000))
		m.Expiration = &s
	}
	switch compression {
	case "":
	case "gzip", "zlib", "application/x-gzip":
		// kombu/compression.py registers zlib.compress under all three
		// names. The deflate stream is not byte-identical to CPython's
		// but decompresses to the identical body.
		var buf bytes.Buffer
		zw := zlib.NewWriter(&buf)
		_, _ = zw.Write(body)
		_ = zw.Close()
		body = buf.Bytes()
		h.Set("compression", pyjson.Str("application/x-gzip"))
	default:
		return nil, fmt.Errorf("%w: compression %q", ErrUnsupported, compression)
	}
	m.Body = body
	return m, nil
}
