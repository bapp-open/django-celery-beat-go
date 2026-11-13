package broker

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bapp-open/django-celery-beat-go/internal/config"
	"github.com/bapp-open/django-celery-beat-go/internal/message"
)

// redisPub publishes the way kombu's Redis transport does
// (kombu/transport/redis.py: Channel._put, _queue_bind, get_table and
// kombu/transport/virtual/base.py: Channel.basic_publish).
type redisPub struct {
	addr     string
	username string
	password string
	db       int
	tls      *tls.Config
	opts     message.RedisOptions

	conn net.Conn
	rd   *bufio.Reader
	// declared mirrors kombu's per-connection declaration cache and the
	// virtual transport's exchange table (state.exchanges).
	declared map[string]bool
	exTypes  map[string]string
}

func newRedis(u *url.URL, s *config.Settings) (Publisher, error) {
	opts, err := message.RedisOptionsFrom(s.BrokerTransportOptions)
	if err != nil {
		return nil, err
	}
	r := &redisPub{opts: opts}
	host, port := u.Hostname(), u.Port()
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "6379"
	}
	r.addr = net.JoinHostPort(host, port)
	if u.User != nil {
		r.username = u.User.Username()
		r.password, _ = u.User.Password()
	}
	// kombu/transport/redis.py: Channel._prepare_virtual_host
	vhost := strings.TrimPrefix(u.Path, "/")
	if vhost != "" {
		db, err := strconv.Atoi(vhost)
		if err != nil {
			return nil, fmt.Errorf("redis broker URL: database must be an integer, got %q", vhost)
		}
		r.db = db
	}
	if u.Scheme == "rediss" {
		cfg := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
		q := u.Query()
		switch strings.ToLower(q.Get("ssl_cert_reqs")) {
		case "none", "cert_none":
			cfg.InsecureSkipVerify = true
		}
		if ca := q.Get("ssl_ca_certs"); ca != "" {
			pem, err := os.ReadFile(ca)
			if err != nil {
				return nil, fmt.Errorf("ssl_ca_certs: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("ssl_ca_certs: no certificates in %s", ca)
			}
			cfg.RootCAs = pool
		}
		if cert, key := q.Get("ssl_certfile"), q.Get("ssl_keyfile"); cert != "" && key != "" {
			pair, err := tls.LoadX509KeyPair(cert, key)
			if err != nil {
				return nil, fmt.Errorf("ssl_certfile/ssl_keyfile: %w", err)
			}
			cfg.Certificates = []tls.Certificate{pair}
		}
		r.tls = cfg
	}
	return r, nil
}

func (r *redisPub) Close() error {
	if r.conn == nil {
		return nil
	}
	err := r.conn.Close()
	r.conn, r.rd, r.declared, r.exTypes = nil, nil, nil, nil
	return err
}

func deadline(ctx context.Context, def time.Duration) time.Time {
	if d, ok := ctx.Deadline(); ok {
		return d
	}
	return time.Now().Add(def)
}

func (r *redisPub) connect(ctx context.Context) error {
	if r.conn != nil {
		return nil
	}
	d := net.Dialer{KeepAlive: 30 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", r.addr)
	if err != nil {
		return err
	}
	if r.tls != nil {
		tc := tls.Client(conn, r.tls)
		if err := tc.HandshakeContext(ctx); err != nil {
			conn.Close()
			return err
		}
		conn = tc
	}
	r.conn, r.rd = conn, bufio.NewReader(conn)
	r.declared, r.exTypes = map[string]bool{}, map[string]string{}
	_ = conn.SetDeadline(deadline(ctx, 10*time.Second))
	if r.password != "" {
		var err error
		if r.username != "" {
			_, err = r.do("AUTH", r.username, r.password)
		} else {
			_, err = r.do("AUTH", r.password)
		}
		if err != nil {
			r.Close()
			return fmt.Errorf("redis AUTH: %w", err)
		}
	}
	if r.db != 0 {
		if _, err := r.do("SELECT", strconv.Itoa(r.db)); err != nil {
			r.Close()
			return fmt.Errorf("redis SELECT %d: %w", r.db, err)
		}
	}
	return nil
}

// do sends one command and reads its reply (RESP2).
func (r *redisPub) do(args ...string) (any, error) {
	buf := make([]byte, 0, 64)
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, a := range args {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(a)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, a...)
		buf = append(buf, '\r', '\n')
	}
	if _, err := r.conn.Write(buf); err != nil {
		return nil, err
	}
	return r.reply()
}

type redisError string

func (e redisError) Error() string { return "redis: " + string(e) }

func (r *redisPub) reply() (any, error) {
	line, err := r.rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 {
		return nil, errors.New("redis: short reply")
	}
	body := line[1 : len(line)-2]
	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return nil, redisError(body)
	case ':':
		return strconv.ParseInt(body, 10, 64)
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		b := make([]byte, n+2)
		if _, err := io.ReadFull(r.rd, b); err != nil {
			return nil, err
		}
		return string(b[:n]), nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, max(n, 0))
		for i := 0; i < n; i++ {
			v, err := r.reply()
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	}
	return nil, fmt.Errorf("redis: unexpected reply %q", line)
}

func uuid4() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (r *redisPub) Publish(ctx context.Context, m *message.Message) (err error) {
	if err := r.connect(ctx); err != nil {
		return err
	}
	defer func() {
		// A protocol-level error reply leaves the connection usable; any
		// other error means its state is unknown.
		var re redisError
		if err != nil && !errors.As(err, &re) && !errors.Is(err, message.ErrUnsupported) && !errors.Is(err, errUndeliverable) {
			r.Close()
		}
	}()
	_ = r.conn.SetDeadline(deadline(ctx, 10*time.Second))
	prefix := r.opts.GlobalKeyPrefix

	// maybe_declare(queue): the only effect on Redis is the binding.
	if q := m.Queue; q != nil && !q.NoDeclare && q.Exchange.Name != "" && !r.declared[q.Name] {
		typ := q.Exchange.Type
		if q.Exchange.NoDeclare || strings.HasPrefix(q.Exchange.Name, "amq.") {
			typ = "direct" // never registered: virtual Channel.typeof default
		} else {
			r.exTypes[q.Exchange.Name] = typ
		}
		qq := *q
		qq.Exchange.Type = typ
		key, member, err := message.RedisBinding(&qq, r.opts)
		if err != nil {
			return err
		}
		if _, err := r.do("SADD", prefix+key, member); err != nil {
			return err
		}
		if !q.AutoDelete {
			r.declared[q.Name] = true
		}
	}

	payload := string(m.RedisPayload(uuid4()))
	if m.Exchange == "" {
		// "anon exchange: routing_key is the destination queue"
		_, err := r.do("LPUSH", prefix+m.RedisQueueKey(m.RoutingKey, r.opts), payload)
		return err
	}
	typ := r.exTypes[m.Exchange]
	if typ == "" {
		typ = "direct"
	}
	rep, err := r.do("SMEMBERS", prefix+"_kombu.binding."+m.Exchange)
	if err != nil {
		return err
	}
	var members []string
	if list, ok := rep.([]any); ok {
		for _, it := range list {
			if s, ok := it.(string); ok {
				members = append(members, s)
			}
		}
	}
	queues, err := message.RedisRoute(typ, members, m.RoutingKey, r.opts)
	if err != nil {
		return err
	}
	if len(queues) == 0 {
		// kombu would warn and drop the message.
		return fmt.Errorf("%w: exchange %q has no queue bound for routing key %q", errUndeliverable, m.Exchange, m.RoutingKey)
	}
	for _, q := range queues {
		if _, err := r.do("LPUSH", prefix+m.RedisQueueKey(q, r.opts), payload); err != nil {
			return err
		}
	}
	return nil
}

var errUndeliverable = errors.New("undeliverable")
