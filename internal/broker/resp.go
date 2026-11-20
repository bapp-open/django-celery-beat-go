package broker

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

// respConn is one connection speaking RESP2: enough of the protocol for
// kombu's publishing commands and for asking a Sentinel for its master.
type respConn struct {
	conn net.Conn
	rd   *bufio.Reader
}

// do sends one command and reads its reply (RESP2).
func (r *respConn) do(args ...string) (any, error) {
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

func (r *respConn) reply() (any, error) {
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
