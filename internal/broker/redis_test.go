package broker

import (
	"bufio"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bapp-open/django-celery-beat-go/internal/config"
	"github.com/bapp-open/django-celery-beat-go/internal/message"
)

// fakeRedis is a tiny RESP server: lists and sets, enough for kombu's
// publishing commands.
type fakeRedis struct {
	ln    net.Listener
	mu    sync.Mutex
	lists map[string][]string
	sets  map[string]map[string]bool
	cmds  []string
	wg    sync.WaitGroup
}

func newFakeRedis(t *testing.T) *fakeRedis {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeRedis{ln: ln, lists: map[string][]string{}, sets: map[string]map[string]bool{}}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.wg.Add(1)
			go func() { defer f.wg.Done(); f.serve(c) }()
		}
	}()
	t.Cleanup(func() { ln.Close(); f.wg.Wait() })
	return f
}

func (f *fakeRedis) serve(c net.Conn) {
	defer c.Close()
	rd := bufio.NewReader(c)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			return
		}
		n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
		args := make([]string, n)
		for i := range args {
			l, _ := rd.ReadString('\n')
			size, _ := strconv.Atoi(strings.TrimSpace(l[1:]))
			buf := make([]byte, size+2)
			if _, err := io.ReadFull(rd, buf); err != nil {
				return
			}
			args[i] = string(buf[:size])
		}
		f.mu.Lock()
		f.cmds = append(f.cmds, args[0])
		var reply string
		switch strings.ToUpper(args[0]) {
		case "LPUSH":
			f.lists[args[1]] = append([]string{args[2]}, f.lists[args[1]]...)
			reply = ":" + strconv.Itoa(len(f.lists[args[1]])) + "\r\n"
		case "SADD":
			if f.sets[args[1]] == nil {
				f.sets[args[1]] = map[string]bool{}
			}
			f.sets[args[1]][args[2]] = true
			reply = ":1\r\n"
		case "SMEMBERS":
			reply = "*" + strconv.Itoa(len(f.sets[args[1]])) + "\r\n"
			for m := range f.sets[args[1]] {
				reply += "$" + strconv.Itoa(len(m)) + "\r\n" + m + "\r\n"
			}
		case "SELECT", "AUTH":
			reply = "+OK\r\n"
		default:
			reply = "-ERR unknown command\r\n"
		}
		f.mu.Unlock()
		if _, err := c.Write([]byte(reply)); err != nil {
			return
		}
	}
}

func testSettings(t *testing.T, url string) *config.Settings {
	s, err := config.ParseSettings([]byte(`{"schema": 1, "timezone": "UTC", "enable_utc": true, "use_tz": true, "tz_aware": true,
	 "task_default_queue": "celery", "task_default_exchange": "celery", "task_default_exchange_type": "direct",
	 "task_default_routing_key": "celery", "task_default_delivery_mode": 2, "task_serializer": "json",
	 "task_create_missing_queues": true, "task_protocol": 2,
	 "queues": {"celery": {"name": "celery", "routing_key": "celery", "durable": true, "queue_arguments": {},
	   "exchange": {"name": "celery", "type": "direct", "durable": true}},
	  "events": {"name": "events", "routing_key": "ev.#", "durable": true, "queue_arguments": {},
	   "exchange": {"name": "evx", "type": "topic", "durable": true}}},
	 "tasks": {}}`))
	if err != nil {
		t.Fatal(err)
	}
	s.BrokerURL = &url
	return s
}

func TestRedisPublishLikeKombu(t *testing.T) {
	f := newFakeRedis(t)
	s := testSettings(t, "redis://:secret@"+f.ln.Addr().String()+"/3")
	pub, err := New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	b, _ := message.NewBuilder(s, "gen1@h", "r")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	prio := int64(6)
	m, _ := b.Build(message.Input{Name: "a", Task: "t", Priority: &prio}, "id-1", time.Unix(0, 0))
	if err := pub.Publish(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish(ctx, m); err != nil {
		t.Fatal(err)
	}
	q, ex, rk := "events", "evx", "ev.user.created"
	m2, _ := b.Build(message.Input{Name: "b", Task: "t", Queue: &q, Exchange: &ex, RoutingKey: &rk}, "id-2", time.Unix(0, 0))
	if err := pub.Publish(ctx, m2); err != nil {
		t.Fatal(err)
	}
	rk2 := "nothing.bound"
	m3, _ := b.Build(message.Input{Name: "c", Task: "t", Queue: &q, Exchange: &ex, RoutingKey: &rk2}, "id-3", time.Unix(0, 0))
	if err := pub.Publish(ctx, m3); err == nil || !strings.Contains(err.Error(), "no queue bound") {
		t.Fatalf("an undeliverable message must be an error, not silently dropped: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if got := f.lists["celery\x06\x166"]; len(got) != 2 || !strings.Contains(got[0], `"priority": 6`) {
		t.Fatalf("priority queue: %v", f.lists)
	}
	if len(f.lists["events"]) != 1 {
		t.Fatalf("topic delivery: %v", f.lists)
	}
	if !f.sets["_kombu.binding.celery"]["celery\x06\x16\x06\x16celery"] ||
		!f.sets["_kombu.binding.evx"]["ev.#\x06\x16"+message.TopicPattern("ev.#")+"\x06\x16events"] {
		t.Fatalf("bindings: %v", f.sets)
	}
	// AUTH, SELECT, then one SADD per queue (declarations are cached)
	if strings.Join(f.cmds[:4], " ") != "AUTH SELECT SADD LPUSH" {
		t.Fatalf("commands: %v", f.cmds)
	}
	sadd := 0
	for _, c := range f.cmds {
		if c == "SADD" {
			sadd++
		}
	}
	if sadd != 2 {
		t.Fatalf("%d SADD commands, want 2", sadd)
	}
}

func TestRedisReconnectsAfterConnectionLoss(t *testing.T) {
	f := newFakeRedis(t)
	s := testSettings(t, "redis://"+f.ln.Addr().String()+"/0")
	pub, _ := New(s)
	defer pub.Close()
	b, _ := message.NewBuilder(s, "o", "r")
	m, _ := b.Build(message.Input{Name: "a", Task: "t"}, "id", time.Unix(0, 0))
	ctx := context.Background()
	if err := pub.Publish(ctx, m); err != nil {
		t.Fatal(err)
	}
	pub.(*redisPub).conn.Close() // the server went away
	if err := pub.Publish(ctx, m); err == nil {
		t.Fatal("publish on a dead connection must fail")
	}
	if err := pub.Publish(ctx, m); err != nil {
		t.Fatalf("the next publish must reconnect: %v", err)
	}
	f.mu.Lock()
	n := len(f.lists["celery"])
	f.mu.Unlock()
	if n != 2 {
		t.Fatalf("%d messages delivered, want 2", n)
	}
}

func TestBrokerURLValidation(t *testing.T) {
	for url, want := range map[string]string{
		"":                           "no broker URL",
		"sqs://":                     "not supported",
		"redis://h/notanumber":       "must be an integer",
		"redis://a:1//;redis://b:2/": "single broker",
	} {
		s := testSettings(t, url)
		if _, err := New(s); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: %v (want %q)", url, err, want)
		}
	}
	a, err := New(testSettings(t, "amqp://user:pw@rabbit:5673/my%2Fvhost"))
	if err != nil {
		t.Fatal(err)
	}
	p := a.(*amqpPub)
	if p.addr != "rabbit:5673" || p.username != "user" || p.password != "pw" || p.vhost != "my/vhost" {
		t.Fatalf("%+v", p)
	}
	a, _ = New(testSettings(t, "amqps://rabbit//"))
	if p := a.(*amqpPub); p.addr != "rabbit:5671" || p.vhost != "/" || p.tls == nil || p.username != "guest" {
		t.Fatalf("%+v", p)
	}
}
