package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// sentinelDefaultPort is kombu's SentinelTransport.default_port.
const sentinelDefaultPort = "26379"

// sentinelQueryTimeout bounds one Sentinel node (dial + AUTH + SENTINEL
// MASTERS) when sentinel_kwargs sets no socket timeout, so that a dead
// node leaves time to ask the next one within one publish attempt.
const sentinelQueryTimeout = 2 * time.Second

// sentinel finds the current Redis master the way kombu's SentinelChannel
// (kombu/transport/redis.py: SentinelChannel._sentinel_managed_pool) and
// redis-py (redis/sentinel.py: Sentinel.discover_master,
// check_master_state) do:
//
//   - the Sentinel nodes are every sentinel:// entry of the
//     ;-separated broker URL, port 26379 when none is given;
//   - the nodes are asked in order with SENTINEL MASTERS; the first one
//     reporting master_name as a master that is neither s_down nor o_down
//     and that sees at least min_other_sentinels other Sentinels wins and
//     moves to the front of the list;
//   - the nodes are contacted with sentinel_kwargs only (no password unless
//     it is there); the master's credentials and database come from the
//     first URL.
//
// It is asked again on every (re)connection; redis-py does the same in
// SentinelManagedConnection._connect_retry.
type sentinel struct {
	addrs      []string
	masterName string
	minOther   int64
	username   string
	password   string
	timeout    time.Duration
}

// sentinelOptionKeys are the sentinel_kwargs gobeat reproduces; anything
// else (ssl, client_name, ...) is refused rather than ignored.
var sentinelOptionKeys = map[string]bool{
	"username": true, "password": true,
	"socket_timeout": true, "socket_connect_timeout": true,
}

func newSentinel(urls []*url.URL, to map[string]any) (*sentinel, error) {
	s := &sentinel{timeout: sentinelQueryTimeout}
	for _, u := range urls {
		host, port := u.Hostname(), u.Port()
		if host == "" {
			return nil, fmt.Errorf("sentinel broker URL %q has no host", u.Redacted())
		}
		if port == "" {
			port = sentinelDefaultPort
		}
		s.addrs = append(s.addrs, net.JoinHostPort(host, port))
	}

	name, ok := to["master_name"].(string)
	if !ok || name == "" {
		// kombu: ValueError("'master_name' transport option must be specified.")
		return nil, errors.New("sentinel broker: the 'master_name' transport option must be specified (broker_transport_options)")
	}
	s.masterName = name

	if v, ok := to["min_other_sentinels"]; ok && v != nil {
		n, err := optInt(v)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("sentinel broker: min_other_sentinels must be a non-negative integer, got %v", v)
		}
		s.minOther = n
	}

	if v, ok := to["sentinel_kwargs"]; ok && v != nil {
		kw, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("sentinel broker: sentinel_kwargs must be an object, got %T", v)
		}
		var unknown []string
		for k := range kw {
			if !sentinelOptionKeys[k] {
				unknown = append(unknown, k)
			}
		}
		if len(unknown) > 0 {
			sort.Strings(unknown)
			return nil, fmt.Errorf("sentinel broker: sentinel_kwargs %s not supported (supported: username, password, socket_timeout, socket_connect_timeout)", strings.Join(unknown, ", "))
		}
		for _, k := range []string{"username", "password"} {
			if v, ok := kw[k]; ok && v != nil {
				str, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("sentinel broker: sentinel_kwargs.%s must be a string", k)
				}
				if k == "username" {
					s.username = str
				} else {
					s.password = str
				}
			}
		}
		// one bound for the whole query: the larger of the two, when given
		var given time.Duration
		for _, k := range []string{"socket_timeout", "socket_connect_timeout"} {
			if v, ok := kw[k]; ok && v != nil {
				sec, err := optFloat(v)
				if err != nil || sec <= 0 {
					return nil, fmt.Errorf("sentinel broker: sentinel_kwargs.%s must be a positive number of seconds, got %v", k, v)
				}
				if d := time.Duration(sec * float64(time.Second)); d > given {
					given = d
				}
			}
		}
		if given > 0 {
			s.timeout = given
		}
	}
	return s, nil
}

// discover returns the address of the current master.
func (s *sentinel) discover(ctx context.Context) (string, error) {
	var errs []string
	for i, addr := range s.addrs {
		state, err := s.query(ctx, addr)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", addr, err))
			if ctx.Err() != nil {
				break
			}
			continue
		}
		if state == nil || !s.usable(state) {
			continue
		}
		s.addrs[0], s.addrs[i] = s.addrs[i], s.addrs[0]
		return net.JoinHostPort(state.ip, state.port), nil
	}
	msg := fmt.Sprintf("sentinel: no master found for %q", s.masterName)
	if len(errs) > 0 {
		msg += ": " + strings.Join(errs, ", ")
	}
	return "", errors.New(msg)
}

// masterState is the part of redis-py's parse_sentinel_state that
// check_master_state reads.
type masterState struct {
	ip, port   string
	flags      map[string]bool
	numOthers  int64
	parseError error
}

// usable is redis-py's Sentinel.check_master_state.
func (s *sentinel) usable(st *masterState) bool {
	if st.parseError != nil || !st.flags["master"] || st.flags["s_down"] || st.flags["o_down"] {
		return false
	}
	return st.numOthers >= s.minOther
}

// query asks one Sentinel node for master_name (SENTINEL MASTERS, as
// redis-py's sentinel_masters does). A nil state means the node does not
// know the name.
func (s *sentinel) query(ctx context.Context, addr string) (*masterState, error) {
	qctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	d := net.Dialer{}
	conn, err := d.DialContext(qctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(deadline(qctx, s.timeout))
	c := &respConn{conn: conn, rd: bufio.NewReader(conn)}
	if s.password != "" {
		if s.username != "" {
			_, err = c.do("AUTH", s.username, s.password)
		} else {
			_, err = c.do("AUTH", s.password)
		}
		if err != nil {
			return nil, fmt.Errorf("AUTH: %w", err)
		}
	}
	rep, err := c.do("SENTINEL", "MASTERS")
	if err != nil {
		return nil, err
	}
	masters, ok := rep.([]any)
	if !ok {
		return nil, fmt.Errorf("SENTINEL MASTERS: unexpected reply %T", rep)
	}
	for _, m := range masters {
		fields, ok := m.([]any)
		if !ok {
			continue
		}
		kv := map[string]string{}
		for i := 0; i+1 < len(fields); i += 2 {
			k, _ := fields[i].(string)
			v, _ := fields[i+1].(string)
			kv[k] = v
		}
		if kv["name"] != s.masterName {
			continue
		}
		st := &masterState{ip: kv["ip"], port: kv["port"], flags: map[string]bool{}}
		for _, f := range strings.Split(kv["flags"], ",") {
			st.flags[f] = true
		}
		if v, ok := kv["num-other-sentinels"]; ok {
			st.numOthers, st.parseError = strconv.ParseInt(v, 10, 64)
		}
		if st.ip == "" || st.port == "" {
			st.parseError = errors.New("no ip/port")
		}
		return st, nil
	}
	return nil, nil
}

func optInt(v any) (int64, error) {
	switch n := v.(type) {
	case json.Number:
		return n.Int64()
	case float64:
		if n != float64(int64(n)) {
			return 0, fmt.Errorf("not an integer: %v", n)
		}
		return int64(n), nil
	case int:
		return int64(n), nil
	case int64:
		return n, nil
	}
	return 0, fmt.Errorf("not a number: %T", v)
}

func optFloat(v any) (float64, error) {
	switch n := v.(type) {
	case json.Number:
		return n.Float64()
	case float64:
		return n, nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	}
	return 0, fmt.Errorf("not a number: %T", v)
}
