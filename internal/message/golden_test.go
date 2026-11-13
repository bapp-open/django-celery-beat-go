package message

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/bapp-open/django-celery-beat-go/internal/config"
)

type goldenCase struct {
	Profile     string
	Settings    json.RawMessage
	Origin      string
	ReplyTo     string `json:"reply_to"`
	Now         string
	TaskID      string `json:"task_id"`
	DeliveryTag string `json:"delivery_tag"`
	Model       struct {
		ID            int64
		Name, Task    string
		Args, Kwargs  string
		Headers       *string
		Queue         *string
		Exchange      *string
		RoutingKey    *string `json:"routing_key"`
		Priority      *int64
		Expires       *string
		ExpireSeconds *int64 `json:"expire_seconds"`
	}
	Redis struct {
		Error  string
		Pushed []struct{ Key, Payload string }
		// state of every _kombu.binding.* set after the case
		Bindings map[string][]string
	}
	AMQP struct {
		Error string
		Calls []struct {
			Call string
			Args map[string]json.RawMessage
		}
	} `json:"amqp"`
}

func unzlib(t *testing.T, b []byte) []byte {
	r, err := zlib.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("zlib: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("zlib: %v", err)
	}
	return out
}

// splitRedisBody cuts the base64 body out of a kombu payload.
func splitRedisBody(t *testing.T, payload string) (body []byte, rest string) {
	const pre = `{"body": "`
	if !strings.HasPrefix(payload, pre) {
		t.Fatalf("unexpected payload prefix: %.40s", payload)
	}
	end := strings.IndexByte(payload[len(pre):], '"')
	b, err := base64.StdEncoding.DecodeString(payload[len(pre) : len(pre)+end])
	if err != nil {
		t.Fatal(err)
	}
	return b, payload[len(pre)+end:]
}

func canon(v any) string {
	b, _ := json.Marshal(v)
	if string(b) == "null" {
		return "{}"
	}
	return string(b)
}

func rawCanon(t *testing.T, raw json.RawMessage) string {
	if len(raw) == 0 {
		return "{}"
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		t.Fatal(err)
	}
	return canon(v)
}

func rawString(raw json.RawMessage) string {
	var s string
	_ = json.Unmarshal(raw, &s)
	return s
}

func TestGoldenMessages(t *testing.T) {
	f, err := os.Open(filepath.Join("..", "..", "testdata", "golden", "messages.jsonl.gz"))
	if err != nil {
		t.Fatalf("golden file missing (run `make golden`): %v", err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 1<<20), 1<<27)

	var (
		b        *Builder
		ropts    RedisOptions
		exTypes  map[string]string
		n, bad   int
		nRedis   int
		nAMQP    int
		nRefused int
	)
	for sc.Scan() {
		var c goldenCase
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			t.Fatal(err)
		}
		if c.Settings != nil {
			s, err := config.ParseSettings(c.Settings)
			if err != nil {
				t.Fatal(err)
			}
			if b, err = NewBuilder(s, c.Origin, c.ReplyTo); err != nil {
				t.Fatal(err)
			}
			if ropts, err = RedisOptionsFrom(s.BrokerTransportOptions); err != nil {
				t.Fatal(err)
			}
			exTypes = map[string]string{}
			for _, q := range s.Queues {
				exTypes[q.Exchange.Name] = q.Exchange.Type
			}
			continue
		}
		n++
		fail := func(format string, a ...any) {
			if bad++; bad < 4 {
				msg := fmt.Sprintf(format, a...)
				line := sc.Text()
				if len(msg) > 1200 {
					msg = msg[:1200] + "..."
				}
				if len(line) > 500 {
					line = line[:500] + "..."
				}
				t.Errorf("%s\n   case: %s", msg, line)
			}
		}
		now, err := time.Parse("2006-01-02T15:04:05.000000Z", c.Now)
		if err != nil {
			t.Fatal(err)
		}
		in := Input{ID: c.Model.ID, Name: c.Model.Name, Task: c.Model.Task, Args: c.Model.Args,
			Kwargs: c.Model.Kwargs, Headers: c.Model.Headers, Queue: c.Model.Queue,
			Exchange: c.Model.Exchange, RoutingKey: c.Model.RoutingKey, Priority: c.Model.Priority,
			ExpireSeconds: c.Model.ExpireSeconds}
		if c.Model.Expires != nil {
			e, err := time.Parse("2006-01-02T15:04:05.000000Z", *c.Model.Expires)
			if err != nil {
				t.Fatal(err)
			}
			in.Expires = &e
		}
		m, err := b.Build(in, c.TaskID, now)

		// ---- Redis ------------------------------------------------------
		if c.Redis.Error != "" {
			if err == nil {
				fail("python raised %s, Go built a message", c.Redis.Error)
			}
			continue
		}
		pickled := c.Model.Task == "t.pickle"
		if pickled {
			// non-JSON serializer: gobeat must refuse, never guess
			if !errors.Is(err, ErrUnsupported) {
				fail("pickle task must be refused, got %v", err)
			}
			nRefused++
			continue
		}
		if errors.Is(err, ErrNoDestination) {
			// Known, deliberate difference: Python LPUSHes to the list
			// named "" (plus priority suffix); nothing consumes it.
			lost := len(c.Redis.Pushed) > 0
			for _, p := range c.Redis.Pushed {
				lost = lost && (p.Key == "" || strings.HasPrefix(p.Key, ropts.Sep))
			}
			if !lost {
				fail("Go found no destination but python delivered to %v", c.Redis.Pushed)
			}
			nRefused++
			continue
		}
		if err != nil {
			fail("Go refused: %v", err)
			continue
		}
		if m.Queue != nil {
			if _, known := exTypes[m.Queue.Exchange.Name]; !known {
				exTypes[m.Queue.Exchange.Name] = m.Queue.Exchange.Type
			}
			if m.Queue.Exchange.Name != "" {
				key, member, berr := RedisBinding(m.Queue, ropts)
				if berr != nil {
					fail("binding: %v", berr)
					continue
				}
				found := false
				for _, mem := range c.Redis.Bindings[key] {
					found = found || mem == member
				}
				if !found {
					fail("binding %q -> %q not created by kombu (%v)", key, member, c.Redis.Bindings[key])
					continue
				}
			}
		}
		var queues []string
		if m.Exchange == "" {
			queues = []string{m.RoutingKey}
		} else {
			typ := exTypes[m.Exchange]
			if typ == "" {
				typ = "direct"
			}
			var rerr error
			queues, rerr = RedisRoute(typ, c.Redis.Bindings["_kombu.binding."+m.Exchange], m.RoutingKey, ropts)
			if rerr != nil {
				fail("route: %v", rerr)
				continue
			}
		}
		var keys, wantKeys []string
		for _, q := range queues {
			keys = append(keys, m.RedisQueueKey(q, ropts))
		}
		for _, p := range c.Redis.Pushed {
			wantKeys = append(wantKeys, p.Key)
		}
		sort.Strings(keys)
		sort.Strings(wantKeys)
		if !reflect.DeepEqual(keys, wantKeys) {
			fail("redis keys %q, python %q (exchange %q rk %q)", keys, wantKeys, m.Exchange, m.RoutingKey)
			continue
		}
		payload := string(m.RedisPayload(c.DeliveryTag))
		compressed := false
		if _, ok := m.Headers.Get("compression"); ok {
			compressed = true
		}
		for _, p := range c.Redis.Pushed {
			if !compressed {
				if p.Payload != payload {
					fail("redis payload differs\n got  %s\n want %s", payload, p.Payload)
				}
				continue
			}
			gb, grest := splitRedisBody(t, payload)
			wb, wrest := splitRedisBody(t, p.Payload)
			if grest != wrest || !bytes.Equal(unzlib(t, gb), unzlib(t, wb)) {
				fail("compressed redis payload differs\n got  %s\n want %s", grest, wrest)
			}
		}
		nRedis++

		// ---- AMQP -------------------------------------------------------
		wire, werr := m.AMQPProperties()
		switch c.AMQP.Error {
		case "NotFound":
			continue // exchange did not exist on the broker; nothing to compare
		case "":
		default:
			if werr == nil {
				fail("py-amqp/RabbitMQ failed with %s, Go encoded the properties", c.AMQP.Error)
			}
			continue
		}
		if werr != nil {
			fail("AMQP properties: %v", werr)
			continue
		}
		for _, call := range c.AMQP.Calls {
			a := call.Args
			switch call.Call {
			case "publish":
				if rawString(a["exchange"]) != m.Exchange || rawString(a["routing_key"]) != m.RoutingKey {
					fail("amqp destination (%q, %q), python (%s, %s)", m.Exchange, m.RoutingKey, a["exchange"], a["routing_key"])
					continue
				}
				if got := base64.StdEncoding.EncodeToString(wire); got != rawString(a["wire"]) {
					w, _ := base64.StdEncoding.DecodeString(rawString(a["wire"]))
					fail("amqp content header differs\n got  %q\n want %q", wire, w)
					continue
				}
				wb, _ := base64.StdEncoding.DecodeString(rawString(a["body_b64"]))
				gb := m.Body
				if compressed {
					wb, gb = unzlib(t, wb), unzlib(t, gb)
				}
				if !bytes.Equal(gb, wb) {
					fail("amqp body differs\n got  %s\n want %s", gb, wb)
				}
			case "queue_declare":
				q := m.Queue
				if q == nil || rawString(a["queue"]) != q.Name ||
					string(a["durable"]) != fmt.Sprint(q.Durable) || string(a["exclusive"]) != fmt.Sprint(q.Exclusive) ||
					string(a["auto_delete"]) != fmt.Sprint(q.AutoDelete) || rawCanon(t, a["arguments"]) != canon(q.QueueArguments) {
					fail("queue_declare differs: python %v, Go %+v", a, q)
				}
			case "exchange_declare":
				q := m.Queue
				if q == nil || rawString(a["exchange"]) != q.Exchange.Name || rawString(a["type"]) != q.Exchange.Type ||
					string(a["durable"]) != fmt.Sprint(q.Exchange.Durable) || string(a["auto_delete"]) != fmt.Sprint(q.Exchange.AutoDelete) ||
					rawCanon(t, a["arguments"]) != canon(q.Exchange.Arguments) {
					fail("exchange_declare differs: python %v, Go %+v", a, q)
				}
			case "queue_bind":
				q := m.Queue
				rk := ""
				if q != nil && q.RoutingKey != nil {
					rk = *q.RoutingKey
				}
				if q == nil || rawString(a["queue"]) != q.Name || rawString(a["exchange"]) != q.Exchange.Name ||
					rawString(a["routing_key"]) != rk || rawCanon(t, a["arguments"]) != canon(q.BindingArguments) {
					fail("queue_bind differs: python %v, Go %+v", a, q)
				}
			}
		}
		nAMQP++

		// journal round trip must not change a single byte
		js, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		var back Message
		if err := json.Unmarshal(js, &back); err != nil {
			t.Fatal(err)
		}
		wire2, _ := back.AMQPProperties()
		if string(back.RedisPayload(c.DeliveryTag)) != payload || !bytes.Equal(wire2, wire) {
			fail("journal round trip changed the message")
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if bad > 0 || nRedis < 1000 || nAMQP < 1000 {
		t.Fatalf("%d of %d cases differ from Celery (redis ok %d, amqp ok %d)", bad, n, nRedis, nAMQP)
	}
	t.Logf("%d cases: %d Redis payloads byte-identical, %d AMQP headers+bodies byte-identical, %d non-JSON tasks refused", n, nRedis, nAMQP, nRefused)
}
