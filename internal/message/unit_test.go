package message

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/bapp-open/django-celery-beat-go/internal/config"
)

const unitSettings = `{"schema": 1, "timezone": "UTC", "enable_utc": true, "use_tz": true, "tz_aware": true,
 "task_default_queue": "celery", "task_default_exchange": "celery", "task_default_exchange_type": "direct",
 "task_default_routing_key": "celery", "task_default_delivery_mode": 2, "task_serializer": "json",
 "task_create_missing_queues": true, "task_protocol": 2,
 "queues": {"celery": {"name": "celery", "routing_key": "celery", "durable": true, "queue_arguments": {},
   "exchange": {"name": "celery", "type": "direct", "durable": true}}},
 "tasks": {}}`

func unitBuilder(t testing.TB) *Builder {
	s, err := config.ParseSettings([]byte(unitSettings))
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewBuilder(s, "gen1@host", "reply-id")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestBuildDefaultMessage(t *testing.T) {
	b := unitBuilder(t)
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	secs := int64(30)
	m, err := b.Build(Input{ID: 1, Name: "nightly", Task: "app.add", Args: `[1, 2]`, Kwargs: `{"x": null}`, ExpireSeconds: &secs}, "the-id", now)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"body": "W1sxLCAyXSwgeyJ4IjogbnVsbH0sIHsiY2FsbGJhY2tzIjogbnVsbCwgImVycmJhY2tzIjogbnVsbCwgImNoYWluIjogbnVsbCwgImNob3JkIjogbnVsbH1d", ` +
		`"content-encoding": "utf-8", "content-type": "application/json", "headers": {"lang": "py", "task": "app.add", "id": "the-id", ` +
		`"shadow": null, "eta": null, "expires": "2024-01-02T03:04:35+00:00", "group": null, "group_index": null, "retries": 0, ` +
		`"timelimit": [null, null], "root_id": "the-id", "parent_id": null, "argsrepr": "[1, 2]", "kwargsrepr": "{'x': None}", ` +
		`"origin": "gen1@host", "ignore_result": false, "replaced_task_nesting": 0, "stamped_headers": null, "stamps": {}, ` +
		`"periodic_task_name": "nightly"}, "properties": {"correlation_id": "the-id", "reply_to": "reply-id", "delivery_mode": 2, ` +
		`"expiration": "30000", "delivery_info": {"exchange": "", "routing_key": "celery"}, "priority": 0, ` +
		`"body_encoding": "base64", "delivery_tag": "tag"}}`
	if got := string(m.RedisPayload("tag")); got != want {
		t.Fatalf("payload:\n got  %s\n want %s", got, want)
	}
	if m.Queue == nil || m.Queue.Name != "celery" || m.Exchange != "" || m.RoutingKey != "celery" {
		t.Fatalf("destination: %+v", m)
	}
}

func TestRedisPriorityQueues(t *testing.T) {
	// kombu/transport/redis.py: priority_steps [0, 3, 6, 9], key
	// "<queue>\x06\x16<step>", plain queue name for step 0.
	o := DefaultRedisOptions()
	for prio, want := range map[int64]string{0: "q", 1: "q", 2: "q", 3: "q\x06\x163", 5: "q\x06\x163", 6: "q\x06\x166", 9: "q\x06\x169", 200: "q\x06\x169"} {
		p := prio
		m := &Message{PrioritySet: true, Priority: &p}
		if got := m.RedisQueueKey("q", o); got != want {
			t.Errorf("priority %d -> %q, want %q", prio, got, want)
		}
	}
	if got := (&Message{}).RedisQueueKey("q", o); got != "q" {
		t.Errorf("no priority -> %q", got)
	}
}

func TestAMQPPriorityPresence(t *testing.T) {
	b := unitBuilder(t)
	m, _ := b.Build(Input{Name: "n", Task: "t"}, "id", time.Unix(0, 0))
	props, err := m.AMQPProperties()
	if err != nil {
		t.Fatal(err)
	}
	// default routing: Producer.publish(priority=0) -> property present, 0
	if props[0]&0x08 == 0 {
		t.Fatal("priority flag must be set for the default priority 0")
	}
	q := "celery"
	m, _ = b.Build(Input{Name: "n", Task: "t", Queue: &q}, "id", time.Unix(0, 0))
	props, _ = m.AMQPProperties()
	if props[0]&0x08 == 0 {
		t.Fatal("explicit queue, unregistered task: publish default priority 0")
	}
	big := int64(256)
	m, _ = b.Build(Input{Name: "n", Task: "t", Priority: &big}, "id", time.Unix(0, 0))
	if _, err := m.AMQPProperties(); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("priority 256 cannot be an AMQP octet: %v", err)
	}
}

func TestTopicPatternAndRoute(t *testing.T) {
	if got, want := TopicPattern("ev.*.created.#"), `^ev\..*?[^\.]\.created\..*?$`; got != want {
		t.Fatalf("pattern %s, want %s", got, want)
	}
	o := DefaultRedisOptions()
	members := []string{"ev.#" + o.Sep + TopicPattern("ev.#") + o.Sep + "events", "x" + o.Sep + TopicPattern("x") + o.Sep + "other"}
	got, err := RedisRoute("topic", members, "ev.user.created", o)
	if err != nil || len(got) != 1 || got[0] != "events" {
		t.Fatalf("route: %v %v", got, err)
	}
	if _, err := RedisRoute("fanout", members, "x", o); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("fanout on redis must be refused: %v", err)
	}
}

func TestRefusals(t *testing.T) {
	b := unitBuilder(t)
	now := time.Unix(1700000000, 0)
	for name, in := range map[string]Input{
		"args not a list":    {Name: "n", Task: "t", Args: `{"a": 1}`},
		"kwargs not a dict":  {Name: "n", Task: "t", Kwargs: `[1]`},
		"invalid json":       {Name: "n", Task: "t", Args: `[1,`},
		"typed json":         {Name: "n", Task: "t", Kwargs: `{"d": {"__type__": "decimal", "__value__": "1.5"}}`},
		"headers not a dict": {Name: "n", Task: "t", Headers: ptr(`[1]`)},
	} {
		if _, err := b.Build(in, "id", now); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	b.S.TaskCreateMissingQueues = false
	q := "nope"
	if _, err := b.Build(Input{Name: "n", Task: "t", Queue: &q}, "id", now); !errors.Is(err, ErrQueueNotFound) {
		t.Errorf("missing queue: %v", err)
	}
}

func ptr[T any](v T) *T { return &v }

func FuzzBuild(f *testing.F) {
	f.Add(`[1, "a"]`, `{"k": 1.5}`, `{"h": [null]}`, int64(5), int64(60))
	f.Add(``, ``, ``, int64(-1), int64(0))
	f.Add(`["\ud83d"]`, `{"a": NaN}`, `{"task": "x", "id": 18446744073709551616}`, int64(300), int64(-7))
	b := unitBuilder(f)
	now := time.Date(2024, 3, 10, 6, 59, 59, 0, time.UTC)
	f.Fuzz(func(t *testing.T, args, kwargs, headers string, prio, expire int64) {
		in := Input{ID: 1, Name: "fuzz", Task: "app.t", Args: args, Kwargs: kwargs, Headers: &headers}
		if prio >= 0 {
			in.Priority = &prio
		}
		if expire != 0 {
			in.ExpireSeconds = &expire
		}
		m, err := b.Build(in, "id", now)
		if err != nil {
			return
		}
		payload := m.RedisPayload("tag")
		if !bytes.HasPrefix(payload, []byte(`{"body": "`)) || !bytes.HasSuffix(payload, []byte(`"delivery_tag": "tag"}}`)) {
			t.Fatalf("malformed payload %q", payload)
		}
		if strings.ContainsAny(string(payload), "\n\r\x00") {
			t.Fatalf("payload contains raw control characters")
		}
		// the journal form must reproduce the message exactly
		js, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		var back Message
		if err := json.Unmarshal(js, &back); err != nil {
			t.Fatalf("journal round trip: %v", err)
		}
		if !bytes.Equal(back.RedisPayload("tag"), payload) {
			t.Fatal("journal round trip changed the payload")
		}
		p1, e1 := m.AMQPProperties()
		p2, e2 := back.AMQPProperties()
		if (e1 == nil) != (e2 == nil) || !bytes.Equal(p1, p2) {
			t.Fatal("journal round trip changed the AMQP properties")
		}
	})
}
