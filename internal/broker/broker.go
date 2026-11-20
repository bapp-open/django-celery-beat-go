// Package broker publishes rendered task messages to Redis or RabbitMQ in
// the format kombu uses.
package broker

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/bapp-open/django-celery-beat-go/internal/config"
	"github.com/bapp-open/django-celery-beat-go/internal/message"
)

// Publisher delivers one message; it returns nil only when the broker has
// accepted it (Redis: LPUSH reply; RabbitMQ: publisher confirm).
// Implementations reconnect lazily and are not safe for concurrent use.
type Publisher interface {
	Publish(ctx context.Context, m *message.Message) error
	Close() error
}

// New returns the publisher for the broker URL in the settings.
func New(s *config.Settings) (Publisher, error) {
	if s.BrokerURL == nil || *s.BrokerURL == "" {
		return nil, fmt.Errorf("no broker URL: the settings row has none (gobeat_sync --no-broker-url?) and GOBEAT_BROKER_URL is not set")
	}
	raw := *s.BrokerURL
	if strings.Contains(raw, ";") || strings.HasPrefix(raw, "sentinel:") {
		return newFromList(raw, s)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("broker URL: %w", err)
	}
	switch u.Scheme {
	case "redis", "rediss":
		return newRedis(u, s)
	case "amqp", "amqps", "pyamqp":
		return newAMQP(u, s)
	}
	return nil, fmt.Errorf("broker transport %q is not supported (supported: redis, rediss, amqp, amqps)", u.Scheme)
}

// newFromList handles a ;-separated broker URL. kombu treats it as
// failover alternates (kombu/connection.py: Connection.__init__); the only
// form gobeat reproduces is a list of Redis Sentinel nodes, where the
// alternates are the Sentinels and the master is discovered from them
// (kombu/transport/redis.py: SentinelChannel).
func newFromList(raw string, s *config.Settings) (Publisher, error) {
	var urls []*url.URL
	for _, part := range strings.Split(raw, ";") {
		u, err := url.Parse(part)
		if err != nil {
			return nil, fmt.Errorf("broker URL: %w", err)
		}
		if u.Scheme != "sentinel" {
			return nil, fmt.Errorf("broker URL lists several brokers (failover); gobeat supports a single broker URL, or a list of sentinel:// URLs (got %q)", u.Scheme)
		}
		urls = append(urls, u)
	}
	if s.BrokerUseSSL {
		return nil, fmt.Errorf("broker_use_ssl with a Redis Sentinel broker is not supported")
	}
	sent, err := newSentinel(urls, s.BrokerTransportOptions)
	if err != nil {
		return nil, err
	}
	// The master's credentials and database are the first URL's
	// (kombu: Connection.hostname = alt[0]; Channel._connparams).
	r, err := newRedis(urls[0], s)
	if err != nil {
		return nil, err
	}
	rp := r.(*redisPub)
	rp.addr, rp.sentinel = "", sent
	return rp, nil
}
