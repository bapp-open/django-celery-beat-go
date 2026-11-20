package broker

import (
	"bufio"
	"context"
	"fmt"
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

// fakeSentinel answers SENTINEL MASTERS with one configurable master entry.
type fakeSentinel struct {
	ln       net.Listener
	mu       sync.Mutex
	name     string
	master   string // host:port
	flags    string
	others   int
	password string // required AUTH password ("" = none)
	auths    []string
	queries  int
	wg       sync.WaitGroup
}

func newFakeSentinel(t *testing.T, name, master string) *fakeSentinel {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeSentinel{ln: ln, name: name, master: master, flags: "master", others: 2}
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
	t.Cleanup(f.stop)
	return f
}

func (f *fakeSentinel) addr() string { return f.ln.Addr().String() }

func (f *fakeSentinel) stop() { f.ln.Close(); f.wg.Wait() }

func (f *fakeSentinel) set(master, flags string) {
	f.mu.Lock()
	f.master, f.flags = master, flags
	f.mu.Unlock()
}

func bulk(s string) string { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }

func (f *fakeSentinel) serve(c net.Conn) {
	defer c.Close()
	rd := bufio.NewReader(c)
	authed := false
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
		var reply string
		switch strings.ToUpper(strings.Join(args, " ")) {
		case "SENTINEL MASTERS":
			f.queries++
			if f.password != "" && !authed {
				reply = "-NOAUTH Authentication required.\r\n"
				break
			}
			host, port, _ := net.SplitHostPort(f.master)
			kv := []string{"name", f.name, "ip", host, "port", port, "flags", f.flags,
				"num-other-sentinels", strconv.Itoa(f.others), "quorum", "2"}
			// a second, unrelated master that must be ignored
			other := []string{"name", "other", "ip", "10.9.9.9", "port", "1", "flags", "master", "num-other-sentinels", "2"}
			reply = "*2\r\n"
			for _, e := range [][]string{other, kv} {
				reply += "*" + strconv.Itoa(len(e)) + "\r\n"
				for _, x := range e {
					reply += bulk(x)
				}
			}
		default:
			if strings.ToUpper(args[0]) == "AUTH" {
				f.auths = append(f.auths, strings.Join(args[1:], " "))
				authed = args[len(args)-1] == f.password
				reply = "+OK\r\n"
				if !authed {
					reply = "-WRONGPASS invalid password\r\n"
				}
				break
			}
			reply = "-ERR unknown command\r\n"
		}
		f.mu.Unlock()
		if _, err := c.Write([]byte(reply)); err != nil {
			return
		}
	}
}

func sentinelSettings(t *testing.T, url string, to string) *config.Settings {
	s := testSettings(t, url)
	full, err := config.ParseSettings([]byte(`{"schema": 1, "broker_transport_options": ` + to + `}`))
	if err != nil {
		t.Fatal(err)
	}
	s.BrokerTransportOptions = full.BrokerTransportOptions
	return s
}

func publishOne(t *testing.T, pub Publisher, s *config.Settings, id string) error {
	t.Helper()
	b, _ := message.NewBuilder(s, "o", "r")
	m, _ := b.Build(message.Input{Name: "a", Task: "t"}, id, time.Unix(0, 0))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return pub.Publish(ctx, m)
}

func deadAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// The broker URL bapp_framework builds: one sentinel:// per node, the
// master's password and database on the first one, master_name in the
// transport options. The first node is down; the second one answers.
func TestSentinelPublishesToDiscoveredMaster(t *testing.T) {
	master := newFakeRedis(t)
	sent := newFakeSentinel(t, "valkeyha", master.ln.Addr().String())
	url := fmt.Sprintf("sentinel://:secret@%s/2;sentinel://%s", deadAddr(t), sent.addr())
	s := sentinelSettings(t, url, `{"master_name": "valkeyha", "sentinels": [["ignored", 26379]], "socket_timeout": 2.0, "retry_on_timeout": true}`)
	pub, err := New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := publishOne(t, pub, s, "id-1"); err != nil {
		t.Fatal(err)
	}
	if err := publishOne(t, pub, s, "id-2"); err != nil {
		t.Fatal(err)
	}
	if got := pub.(*redisPub).sentinel.addrs[0]; got != sent.addr() {
		t.Fatalf("the Sentinel that answered must move to the front: %v", pub.(*redisPub).sentinel.addrs)
	}
	master.mu.Lock()
	defer master.mu.Unlock()
	if len(master.lists["celery"]) != 2 {
		t.Fatalf("master got %v", master.lists)
	}
	// the master's credentials and database come from the first URL
	if strings.Join(master.cmds[:4], " ") != "AUTH SELECT SADD LPUSH" {
		t.Fatalf("master commands: %v", master.cmds)
	}
	sent.mu.Lock()
	defer sent.mu.Unlock()
	if len(sent.auths) != 0 {
		t.Fatalf("Sentinels are contacted without the master's password (sentinel_kwargs only): %v", sent.auths)
	}
	if sent.queries != 1 {
		t.Fatalf("one discovery per connection, got %d", sent.queries)
	}
}

// A failover: the old master is demoted (READONLY), Sentinel now reports
// the replica. The failed attempt drops the connection; the next one
// rediscovers and delivers to the new master.
func TestSentinelFollowsFailover(t *testing.T) {
	a, b := newFakeRedis(t), newFakeRedis(t)
	sent := newFakeSentinel(t, "mymaster", a.ln.Addr().String())
	s := sentinelSettings(t, "sentinel://"+sent.addr(), `{"master_name": "mymaster"}`)
	pub, err := New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := publishOne(t, pub, s, "before"); err != nil {
		t.Fatal(err)
	}

	a.mu.Lock()
	a.readonly = true
	a.mu.Unlock()
	sent.set(b.ln.Addr().String(), "master")

	err = publishOne(t, pub, s, "during")
	if err == nil || !strings.Contains(err.Error(), "READONLY") || !strings.Contains(err.Error(), "asks Sentinel") {
		t.Fatalf("a write to a demoted master must fail with READONLY: %v", err)
	}
	if pub.(*redisPub).conn != nil {
		t.Fatal("READONLY must drop the connection")
	}
	if err := publishOne(t, pub, s, "after"); err != nil {
		t.Fatalf("the retry must reach the new master: %v", err)
	}
	a.mu.Lock()
	na := len(a.lists["celery"])
	a.mu.Unlock()
	b.mu.Lock()
	nb := len(b.lists["celery"])
	b.mu.Unlock()
	if na != 1 || nb != 1 {
		t.Fatalf("old master %d, new master %d; want 1 and 1", na, nb)
	}
}

// check_master_state: a master flagged s_down/o_down, or seen by fewer
// than min_other_sentinels other Sentinels, is not used.
func TestSentinelMasterState(t *testing.T) {
	good := newFakeRedis(t)
	down := newFakeSentinel(t, "m", deadAddr(t))
	down.set(deadAddr(t), "master,s_down")
	ok := newFakeSentinel(t, "m", good.ln.Addr().String())
	url := "sentinel://" + down.addr() + ";sentinel://" + ok.addr()

	s := sentinelSettings(t, url, `{"master_name": "m"}`)
	pub, _ := New(s)
	if err := publishOne(t, pub, s, "x"); err != nil {
		t.Fatalf("an s_down master must be skipped for the next Sentinel's: %v", err)
	}
	pub.Close()

	ok.mu.Lock()
	ok.others = 0
	ok.mu.Unlock()
	s = sentinelSettings(t, url, `{"master_name": "m", "min_other_sentinels": 1}`)
	pub, _ = New(s)
	defer pub.Close()
	if err := publishOne(t, pub, s, "y"); err == nil || !strings.Contains(err.Error(), `no master found for "m"`) {
		t.Fatalf("min_other_sentinels must be enforced: %v", err)
	}

	s = sentinelSettings(t, "sentinel://"+ok.addr(), `{"master_name": "unknown"}`)
	pub2, _ := New(s)
	defer pub2.Close()
	if err := publishOne(t, pub2, s, "z"); err == nil || !strings.Contains(err.Error(), `no master found for "unknown"`) {
		t.Fatalf("an unknown master name: %v", err)
	}
}

func TestSentinelKwargsAuth(t *testing.T) {
	master := newFakeRedis(t)
	sent := newFakeSentinel(t, "m", master.ln.Addr().String())
	sent.password = "sentpw"
	s := sentinelSettings(t, "sentinel://"+sent.addr(), `{"master_name": "m", "sentinel_kwargs": {"password": "sentpw", "socket_timeout": 0.5}}`)
	pub, err := New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if got := pub.(*redisPub).sentinel.timeout; got != 500*time.Millisecond {
		t.Fatalf("socket_timeout: %v", got)
	}
	if err := publishOne(t, pub, s, "x"); err != nil {
		t.Fatal(err)
	}
	sent.mu.Lock()
	defer sent.mu.Unlock()
	if len(sent.auths) != 1 || sent.auths[0] != "sentpw" {
		t.Fatalf("sentinel AUTH: %v", sent.auths)
	}
	master.mu.Lock()
	defer master.mu.Unlock()
	if master.cmds[0] == "AUTH" {
		t.Fatal("no password on the URL: the master gets no AUTH")
	}
}

func TestSentinelURLValidation(t *testing.T) {
	for _, c := range []struct{ url, to, want string }{
		{"sentinel://h:26379", `{}`, "'master_name' transport option must be specified"},
		{"sentinel://a;redis://b", `{"master_name": "m"}`, "single broker URL, or a list of sentinel"},
		{"redis://a;sentinel://b", `{"master_name": "m"}`, "single broker URL, or a list of sentinel"},
		{"sentinel://a", `{"master_name": "m", "sentinel_kwargs": {"ssl": true, "password": "x"}}`, "sentinel_kwargs ssl not supported"},
		{"sentinel://a", `{"master_name": "m", "min_other_sentinels": -1}`, "min_other_sentinels"},
		{"sentinel://a/x", `{"master_name": "m"}`, "must be an integer"},
		{"sentinel://", `{"master_name": "m"}`, "has no host"},
	} {
		if _, err := New(sentinelSettings(t, c.url, c.to)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%q %s: %v (want %q)", c.url, c.to, err, c.want)
		}
	}
	s := sentinelSettings(t, "sentinel://a", `{"master_name": "m"}`)
	s.BrokerUseSSL = true
	if _, err := New(s); err == nil || !strings.Contains(err.Error(), "broker_use_ssl") {
		t.Errorf("broker_use_ssl: %v", err)
	}

	pub, err := New(sentinelSettings(t, "sentinel://user:pw@s1/4;sentinel://s2:26380;sentinel://[::1]", `{"master_name": "m", "min_other_sentinels": 1}`))
	if err != nil {
		t.Fatal(err)
	}
	r := pub.(*redisPub)
	if got := strings.Join(r.sentinel.addrs, " "); got != "s1:26379 s2:26380 [::1]:26379" {
		t.Fatalf("sentinel addresses (default port 26379): %s", got)
	}
	if r.username != "user" || r.password != "pw" || r.db != 4 || r.addr != "" || r.sentinel.minOther != 1 || r.sentinel.masterName != "m" {
		t.Fatalf("%+v", r)
	}
}
