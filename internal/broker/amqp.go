package broker

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/bapp-open/django-celery-beat-go/internal/config"
	"github.com/bapp-open/django-celery-beat-go/internal/message"
)

// amqpPub is a small AMQP 0-9-1 publisher. It exists instead of a general
// client library so that the content header (properties and header table)
// is exactly what py-amqp sends for the kombu message, including explicit
// priority 0 and table key order - see message.AMQPProperties.
//
// It is strictly request/response on one channel, uses publisher confirms
// and the mandatory flag, and negotiates heartbeats off (liveness is
// checked on each publish; TCP keep-alive covers idle periods), so it
// needs no background goroutine.
type amqpPub struct {
	addr     string
	host     string
	vhost    string
	username string
	password string
	tls      *tls.Config

	conn     net.Conn
	rd       *bufio.Reader
	frameMax int
	seq      uint64 // publish sequence number in confirm mode
	declared map[string]bool
}

func newAMQP(u *url.URL, s *config.Settings) (Publisher, error) {
	a := &amqpPub{username: "guest", password: "guest", vhost: "/"}
	host, port := u.Hostname(), u.Port()
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "5672"
		if u.Scheme == "amqps" {
			port = "5671"
		}
	}
	a.host, a.addr = host, net.JoinHostPort(host, port)
	if u.User != nil {
		if n := u.User.Username(); n != "" {
			a.username = n
		}
		if p, ok := u.User.Password(); ok {
			a.password = p
		}
	}
	// kombu.utils.url.parse_url: vhost is the path without its leading
	// slash, URL-decoded; an empty one means "/".
	path := u.Path
	if u.RawPath != "" {
		if p, err := url.PathUnescape(u.RawPath); err == nil {
			path = p
		}
	}
	if v := strings.TrimPrefix(path, "/"); v != "" {
		a.vhost = v
	}
	if u.Scheme == "amqps" || (s != nil && s.BrokerUseSSL) {
		a.tls = &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
	}
	return a, nil
}

func (a *amqpPub) Close() error {
	if a.conn == nil {
		return nil
	}
	// best-effort Connection.Close
	_ = a.conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
	w := newWriter()
	w.short(200)
	w.shortstr("gobeat shutdown")
	w.short(0)
	w.short(0)
	_ = a.sendMethod(0, 10, 50, w.b)
	err := a.conn.Close()
	a.conn, a.rd, a.declared = nil, nil, nil
	return err
}

func (a *amqpPub) drop() {
	if a.conn != nil {
		a.conn.Close()
	}
	a.conn, a.rd, a.declared = nil, nil, nil
}

// --- wire helpers ----------------------------------------------------------

type writer struct{ b []byte }

func newWriter() *writer { return &writer{b: make([]byte, 0, 128)} }

func (w *writer) octet(v byte)      { w.b = append(w.b, v) }
func (w *writer) short(v uint16)    { w.b = binary.BigEndian.AppendUint16(w.b, v) }
func (w *writer) long(v uint32)     { w.b = binary.BigEndian.AppendUint32(w.b, v) }
func (w *writer) longlong(v uint64) { w.b = binary.BigEndian.AppendUint64(w.b, v) }
func (w *writer) shortstr(s string) {
	w.b = append(w.b, byte(len(s)))
	w.b = append(w.b, s...)
}
func (w *writer) longstr(s string) {
	w.long(uint32(len(s)))
	w.b = append(w.b, s...)
}
func (w *writer) raw(b []byte) { w.b = append(w.b, b...) }

const (
	frameMethod    = 1
	frameHeader    = 2
	frameBody      = 3
	frameHeartbeat = 8
	frameEnd       = 0xCE
)

func (a *amqpPub) writeFrame(typ byte, channel uint16, payload []byte) error {
	buf := make([]byte, 0, len(payload)+8)
	buf = append(buf, typ)
	buf = binary.BigEndian.AppendUint16(buf, channel)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(payload)))
	buf = append(buf, payload...)
	buf = append(buf, frameEnd)
	_, err := a.conn.Write(buf)
	return err
}

func (a *amqpPub) sendMethod(channel uint16, class, method uint16, args []byte) error {
	p := make([]byte, 0, len(args)+4)
	p = binary.BigEndian.AppendUint16(p, class)
	p = binary.BigEndian.AppendUint16(p, method)
	p = append(p, args...)
	return a.writeFrame(frameMethod, channel, p)
}

type frame struct {
	typ     byte
	channel uint16
	payload []byte
}

func (a *amqpPub) readFrame() (frame, error) {
	var hdr [7]byte
	if _, err := io.ReadFull(a.rd, hdr[:]); err != nil {
		return frame{}, err
	}
	size := binary.BigEndian.Uint32(hdr[3:])
	if size > 16<<20 {
		return frame{}, fmt.Errorf("amqp: oversized frame (%d bytes)", size)
	}
	payload := make([]byte, size+1)
	if _, err := io.ReadFull(a.rd, payload); err != nil {
		return frame{}, err
	}
	if payload[size] != frameEnd {
		return frame{}, errors.New("amqp: bad frame end")
	}
	return frame{hdr[0], binary.BigEndian.Uint16(hdr[1:]), payload[:size]}, nil
}

// amqpError is a channel- or connection-level exception from the broker.
type amqpError struct {
	Code       uint16
	Text       string
	Connection bool
}

func (e *amqpError) Error() string {
	scope := "channel"
	if e.Connection {
		scope = "connection"
	}
	return fmt.Sprintf("amqp: %s closed by broker: (%d) %s", scope, e.Code, e.Text)
}

func parseClose(args []byte, conn bool) error {
	e := &amqpError{Connection: conn, Text: "?"}
	if len(args) >= 3 {
		e.Code = binary.BigEndian.Uint16(args)
		n := int(args[2])
		if len(args) >= 3+n {
			e.Text = string(args[3 : 3+n])
		}
	}
	return e
}

// readMethod returns the next method frame, transparently answering
// broker-initiated closes with an error and skipping heartbeats.
func (a *amqpPub) readMethod() (class, method uint16, args []byte, err error) {
	for {
		f, err := a.readFrame()
		if err != nil {
			return 0, 0, nil, err
		}
		switch f.typ {
		case frameHeartbeat:
			continue
		case frameMethod:
			if len(f.payload) < 4 {
				return 0, 0, nil, errors.New("amqp: short method frame")
			}
			class, method = binary.BigEndian.Uint16(f.payload), binary.BigEndian.Uint16(f.payload[2:])
			args = f.payload[4:]
			switch {
			case class == 10 && method == 50: // Connection.Close
				_ = a.sendMethod(0, 10, 51, nil)
				return 0, 0, nil, parseClose(args, true)
			case class == 20 && method == 40: // Channel.Close
				_ = a.sendMethod(f.channel, 20, 41, nil)
				return 0, 0, nil, parseClose(args, false)
			case class == 10 && (method == 60 || method == 61): // blocked/unblocked
				continue
			}
			return class, method, args, nil
		default:
			// header/body frames of a Basic.Return are consumed by the caller
			return 0, uint16(f.typ), f.payload, nil
		}
	}
}

func (a *amqpPub) expect(class, method uint16) ([]byte, error) {
	c, m, args, err := a.readMethod()
	if err != nil {
		return nil, err
	}
	if c != class || m != method {
		return nil, fmt.Errorf("amqp: expected method %d.%d, got %d.%d", class, method, c, m)
	}
	return args, nil
}

func (a *amqpPub) connect(ctx context.Context) error {
	if a.conn != nil {
		return nil
	}
	d := net.Dialer{KeepAlive: 30 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", a.addr)
	if err != nil {
		return err
	}
	if a.tls != nil {
		tc := tls.Client(conn, a.tls)
		if err := tc.HandshakeContext(ctx); err != nil {
			conn.Close()
			return err
		}
		conn = tc
	}
	a.conn, a.rd = conn, bufio.NewReaderSize(conn, 16<<10)
	a.declared, a.seq = map[string]bool{}, 0
	_ = conn.SetDeadline(deadline(ctx, 15*time.Second))
	if err := a.handshake(); err != nil {
		a.drop()
		return err
	}
	return nil
}

func (a *amqpPub) handshake() error {
	if _, err := a.conn.Write([]byte("AMQP\x00\x00\x09\x01")); err != nil {
		return err
	}
	if _, err := a.expect(10, 10); err != nil { // Connection.Start
		return fmt.Errorf("amqp handshake: %w", err)
	}
	// Start-Ok: client-properties, mechanism, response, locale
	props := newWriter()
	tableStr := func(k, v string) {
		props.shortstr(k)
		props.octet('S')
		props.longstr(v)
	}
	tableStr("product", "gobeat")
	tableStr("platform", "Go")
	caps := newWriter()
	for _, c := range []string{"publisher_confirms", "basic.nack"} {
		caps.shortstr(c)
		caps.octet('t')
		caps.octet(1)
	}
	props.shortstr("capabilities")
	props.octet('F')
	props.long(uint32(len(caps.b)))
	props.raw(caps.b)
	w := newWriter()
	w.long(uint32(len(props.b)))
	w.raw(props.b)
	w.shortstr("PLAIN")
	w.longstr("\x00" + a.username + "\x00" + a.password)
	w.shortstr("en_US")
	if err := a.sendMethod(0, 10, 11, w.b); err != nil {
		return err
	}
	tune, err := a.expect(10, 30) // Connection.Tune
	if err != nil {
		return fmt.Errorf("amqp login as %q failed: %w", a.username, err)
	}
	if len(tune) < 8 {
		return errors.New("amqp: short Tune")
	}
	channelMax := binary.BigEndian.Uint16(tune)
	frameMax := binary.BigEndian.Uint32(tune[2:])
	if frameMax == 0 || frameMax > 131072 {
		frameMax = 131072
	}
	a.frameMax = int(frameMax)
	w = newWriter()
	w.short(channelMax)
	w.long(frameMax)
	w.short(0) // heartbeat off
	if err := a.sendMethod(0, 10, 31, w.b); err != nil {
		return err
	}
	w = newWriter()
	w.shortstr(a.vhost)
	w.shortstr("")
	w.octet(0)
	if err := a.sendMethod(0, 10, 40, w.b); err != nil { // Connection.Open
		return err
	}
	if _, err := a.expect(10, 41); err != nil {
		return fmt.Errorf("amqp open vhost %q: %w", a.vhost, err)
	}
	return a.openChannel()
}

func (a *amqpPub) openChannel() error {
	w := newWriter()
	w.shortstr("")
	if err := a.sendMethod(1, 20, 10, w.b); err != nil { // Channel.Open
		return err
	}
	if _, err := a.expect(20, 11); err != nil {
		return err
	}
	if err := a.sendMethod(1, 85, 10, []byte{0}); err != nil { // Confirm.Select
		return err
	}
	if _, err := a.expect(85, 11); err != nil {
		return err
	}
	a.seq = 0
	return nil
}

// declare is kombu.entity.Queue.declare: exchange, queue, binding.
func (a *amqpPub) declare(q *config.Queue) error {
	ex := q.Exchange
	// Exchange._can_declare
	if ex.Name != "" && !ex.NoDeclare && !strings.HasPrefix(ex.Name, "amq.") {
		args, err := message.AMQPTable(ex.Arguments)
		if err != nil {
			return err
		}
		w := newWriter()
		w.short(0)
		w.shortstr(ex.Name)
		w.shortstr(ex.Type)
		var bits byte
		if ex.Passive {
			bits |= 1
		}
		if ex.Durable {
			bits |= 2
		}
		if ex.AutoDelete {
			bits |= 4
		}
		w.octet(bits)
		w.raw(args)
		if err := a.sendMethod(1, 40, 10, w.b); err != nil {
			return err
		}
		if _, err := a.expect(40, 11); err != nil {
			return fmt.Errorf("declare exchange %q: %w", ex.Name, err)
		}
	}
	args, err := message.AMQPTable(q.QueueArguments)
	if err != nil {
		return err
	}
	w := newWriter()
	w.short(0)
	w.shortstr(q.Name)
	var bits byte
	if q.Durable {
		bits |= 2
	}
	if q.Exclusive {
		bits |= 4
	}
	if q.AutoDelete {
		bits |= 8
	}
	w.octet(bits)
	w.raw(args)
	if err := a.sendMethod(1, 50, 10, w.b); err != nil {
		return err
	}
	if _, err := a.expect(50, 11); err != nil {
		return fmt.Errorf("declare queue %q: %w", q.Name, err)
	}
	if ex.Name != "" {
		bargs, err := message.AMQPTable(q.BindingArguments)
		if err != nil {
			return err
		}
		rk := ""
		if q.RoutingKey != nil {
			rk = *q.RoutingKey
		}
		w := newWriter()
		w.short(0)
		w.shortstr(q.Name)
		w.shortstr(ex.Name)
		w.shortstr(rk)
		w.octet(0)
		w.raw(bargs)
		if err := a.sendMethod(1, 50, 20, w.b); err != nil {
			return err
		}
		if _, err := a.expect(50, 21); err != nil {
			return fmt.Errorf("bind queue %q: %w", q.Name, err)
		}
	}
	return nil
}

func cacheable(q *config.Queue) bool {
	// kombu/entity.py: Queue.can_cache_declaration
	_, expiring := q.QueueArguments["x-expires"]
	return !expiring && !q.AutoDelete
}

func (a *amqpPub) Publish(ctx context.Context, m *message.Message) (err error) {
	props, err := m.AMQPProperties()
	if err != nil {
		return err
	}
	if len(m.Exchange) > 255 || len(m.RoutingKey) > 255 {
		return fmt.Errorf("%w: exchange or routing key longer than 255 bytes", message.ErrUnsupported)
	}
	if err := a.connect(ctx); err != nil {
		return err
	}
	defer func() {
		if err == nil {
			return
		}
		var ae *amqpError
		if errors.As(err, &ae) && !ae.Connection {
			// channel exception: the connection survives, reopen the channel
			a.declared = map[string]bool{}
			if rerr := a.openChannel(); rerr == nil {
				return
			}
		}
		if !errors.Is(err, message.ErrUnsupported) {
			a.drop()
		}
	}()
	_ = a.conn.SetDeadline(deadline(ctx, 15*time.Second))

	if q := m.Queue; q != nil && !q.NoDeclare && !a.declared[q.Name] {
		if len(q.Name) > 255 {
			return fmt.Errorf("%w: queue name longer than 255 bytes", message.ErrUnsupported)
		}
		if err := a.declare(q); err != nil {
			return err
		}
		if cacheable(q) {
			a.declared[q.Name] = true
		}
	}

	// Basic.Publish (mandatory) + content header + body, in one write.
	w := newWriter()
	w.short(0)
	w.shortstr(m.Exchange)
	w.shortstr(m.RoutingKey)
	w.octet(1) // mandatory: an unroutable message comes back as Basic.Return
	method := make([]byte, 0, len(w.b)+4)
	method = binary.BigEndian.AppendUint16(method, 60)
	method = binary.BigEndian.AppendUint16(method, 40)
	method = append(method, w.b...)

	hdr := make([]byte, 0, len(props)+12)
	hdr = binary.BigEndian.AppendUint16(hdr, 60) // class
	hdr = binary.BigEndian.AppendUint16(hdr, 0)  // weight
	hdr = binary.BigEndian.AppendUint64(hdr, uint64(len(m.Body)))
	hdr = append(hdr, props...)
	if len(hdr)+8 > a.frameMax {
		return fmt.Errorf("%w: message headers (%d bytes) exceed the AMQP frame size %d", message.ErrUnsupported, len(hdr), a.frameMax)
	}

	out := make([]byte, 0, len(method)+len(hdr)+len(m.Body)+64)
	appendFrame := func(typ byte, payload []byte) {
		out = append(out, typ)
		out = binary.BigEndian.AppendUint16(out, 1)
		out = binary.BigEndian.AppendUint32(out, uint32(len(payload)))
		out = append(out, payload...)
		out = append(out, frameEnd)
	}
	appendFrame(frameMethod, method)
	appendFrame(frameHeader, hdr)
	chunk := a.frameMax - 8
	for off := 0; off < len(m.Body); off += chunk {
		end := min(off+chunk, len(m.Body))
		appendFrame(frameBody, m.Body[off:end])
	}
	if _, err := a.conn.Write(out); err != nil {
		return err
	}
	a.seq++

	// Wait for the confirm for this sequence number.
	returned := ""
	for {
		class, method, args, err := a.readMethod()
		if err != nil {
			return err
		}
		switch {
		case class == 0: // header/body frame belonging to a Basic.Return
			continue
		case class == 60 && method == 50: // Basic.Return
			returned = "unroutable"
			if len(args) >= 3 {
				n := int(args[2])
				if len(args) >= 3+n {
					returned = string(args[3 : 3+n])
				}
			}
		case class == 60 && (method == 80 || method == 120): // Ack / Nack
			if len(args) < 9 {
				return errors.New("amqp: short confirm")
			}
			tag := binary.BigEndian.Uint64(args)
			multiple := args[8]&1 == 1
			if tag != a.seq && !(multiple && tag >= a.seq) {
				continue // confirm for an earlier publish
			}
			if method == 120 {
				return errors.New("amqp: broker nacked the message")
			}
			if returned != "" {
				return fmt.Errorf("%w: broker returned the message: %s (exchange %q, routing key %q)",
					errUndeliverable, returned, m.Exchange, m.RoutingKey)
			}
			return nil
		default:
			return fmt.Errorf("amqp: unexpected method %d.%d while waiting for confirm", class, method)
		}
	}
}
