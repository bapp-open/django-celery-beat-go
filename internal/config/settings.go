// Package config holds the settings document written by
// `manage.py gobeat_sync` (django_gobeat/resolve.py) and the process
// configuration derived from it and from the environment.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// SchemaVersion is the settings document version this binary understands.
const SchemaVersion = 1

// Exchange mirrors kombu.Exchange as used by Queue.declare.
type Exchange struct {
	Name         string         `json:"name"`
	Type         string         `json:"type"`
	Durable      bool           `json:"durable"`
	AutoDelete   bool           `json:"auto_delete"`
	DeliveryMode *int           `json:"delivery_mode"`
	Arguments    map[string]any `json:"arguments"`
	NoDeclare    bool           `json:"no_declare"`
	Passive      bool           `json:"passive"`
}

// Queue mirrors kombu.Queue as used by Queue.declare.
type Queue struct {
	Name             string         `json:"name"`
	RoutingKey       *string        `json:"routing_key"`
	Durable          bool           `json:"durable"`
	Exclusive        bool           `json:"exclusive"`
	AutoDelete       bool           `json:"auto_delete"`
	NoDeclare        bool           `json:"no_declare"`
	QueueArguments   map[string]any `json:"queue_arguments"`
	BindingArguments map[string]any `json:"binding_arguments"`
	Exchange         Exchange       `json:"exchange"`
	Unsupported      string         `json:"unsupported"`
}

// Resolved is what Celery itself computed for a task published without
// any PeriodicTask overrides. gobeat recomputes it at load time and
// refuses the task if the two disagree.
type Resolved struct {
	// Error is set when Celery itself failed to publish the task with its
	// default options (e.g. QueueNotFound).
	Error        string   `json:"error"`
	Exchange     string   `json:"exchange"`
	RoutingKey   *string  `json:"routing_key"`
	Declare      []string `json:"declare"`
	Serializer   *string  `json:"serializer"`
	Compression  *string  `json:"compression"`
	DeliveryMode any      `json:"delivery_mode"`
	HasPriority  bool     `json:"has_priority"`
	Priority     any      `json:"priority"`
	Expiration   any      `json:"expiration"`
	IgnoreResult bool     `json:"ignore_result"`
	HasExpires   bool     `json:"has_expires"`
	TimeLimit    []any    `json:"timelimit"`
}

// Task is one entry of the task map.
type Task struct {
	Registered   bool           `json:"registered"`
	Options      map[string]any `json:"options"`
	IgnoreResult bool           `json:"ignore_result"`
	Route        map[string]any `json:"route"`
	Resolved     *Resolved      `json:"resolved"`
	Unsupported  string         `json:"unsupported"`
}

// Settings is the JSON document in django_gobeat_settings.data.
type Settings struct {
	Schema                 int               `json:"schema"`
	Versions               map[string]string `json:"versions"`
	BrokerURL              *string           `json:"broker_url"`
	BrokerTransportOptions map[string]any    `json:"broker_transport_options"`
	BrokerUseSSL           bool              `json:"broker_use_ssl"`
	Timezone               *string           `json:"timezone"`
	EnableUTC              bool              `json:"enable_utc"`
	UseTZ                  bool              `json:"use_tz"`
	TZAware                bool              `json:"tz_aware"`
	BeatMaxLoopInterval    float64           `json:"beat_max_loop_interval"`
	BeatCronDeadline       *float64          `json:"beat_cron_starting_deadline"`

	TaskDefaultQueue        string  `json:"task_default_queue"`
	TaskDefaultExchange     *string `json:"task_default_exchange"`
	TaskDefaultExchangeType string  `json:"task_default_exchange_type"`
	TaskDefaultRoutingKey   *string `json:"task_default_routing_key"`
	TaskDefaultDeliveryMode any     `json:"task_default_delivery_mode"`
	TaskDefaultPriority     any     `json:"task_default_priority"`
	TaskSerializer          string  `json:"task_serializer"`
	TaskCompression         *string `json:"task_compression"`
	TaskCreateMissingQueues bool    `json:"task_create_missing_queues"`
	TaskQueueMaxPriority    any     `json:"task_queue_max_priority"`
	TaskProtocol            int     `json:"task_protocol"`
	TaskAlwaysEager         bool    `json:"task_always_eager"`
	TaskSendSentEvent       bool    `json:"task_send_sent_event"`
	HasRoutes               bool    `json:"has_routes"`
	DynamicRoutes           bool    `json:"dynamic_routes"`

	Queues   map[string]*Queue `json:"queues"`
	Tasks    map[string]*Task  `json:"tasks"`
	Warnings []string          `json:"warnings"`
}

// ParseSettings decodes the settings document. Numbers are kept as
// json.Number so that 60 and 60.0 stay distinguishable (Python's
// str(int(expires * 1000)) and timedelta maths care).
func ParseSettings(data []byte) (*Settings, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var s Settings
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("settings row is not valid JSON: %w", err)
	}
	if s.Schema != SchemaVersion {
		return nil, fmt.Errorf("settings schema %d is not supported by this gobeat (want %d); upgrade django_gobeat and gobeat together and re-run gobeat_sync", s.Schema, SchemaVersion)
	}
	if s.Queues == nil {
		s.Queues = map[string]*Queue{}
	}
	if s.Tasks == nil {
		s.Tasks = map[string]*Task{}
	}
	return &s, nil
}

// AppTimezone resolves app.timezone (celery/app/base.py: Celery.timezone):
// conf.timezone when set, UTC when unset and enable_utc is on. With
// enable_utc off and no timezone Python uses the host's local zone, which
// gobeat refuses to guess.
func (s *Settings) AppTimezone() (*time.Location, error) {
	if s.Timezone == nil || *s.Timezone == "" {
		if s.EnableUTC {
			return time.UTC, nil
		}
		return nil, fmt.Errorf("enable_utc is False and no timezone is configured: Python beat would use the host's local zone; set CELERY_TIMEZONE (or GOBEAT_TIMEZONE)")
	}
	if *s.Timezone == "UTC" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(*s.Timezone)
	if err != nil {
		return nil, fmt.Errorf("unknown timezone %q: %w", *s.Timezone, err)
	}
	return loc, nil
}
