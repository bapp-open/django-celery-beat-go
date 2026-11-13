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
	if strings.Contains(raw, ";") {
		return nil, fmt.Errorf("broker URL lists several brokers (failover); gobeat supports a single broker URL")
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
