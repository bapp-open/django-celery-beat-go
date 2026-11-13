package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env is the process configuration. Only DATABASE_URL is required; the
// rest has defaults or comes from the settings row.
type Env struct {
	DatabaseURL string
	HTTPAddr    string
	LogLevel    string
	LogFormat   string

	DryRun                   bool
	MissedRunPolicy          string
	MissedRunGrace           time.Duration
	PublishTimeout           time.Duration
	PublishMaxAge            time.Duration
	Retention                time.Duration
	HealthMaxAge             time.Duration
	StandbyInterval          time.Duration
	LockKey                  int64
	SettingsKey              string
	AllowUnsupportedVersions bool

	// overrides of the settings row
	BrokerURL              string
	BrokerTransportOptions string
	Timezone               string
	MaxLoopInterval        string
	CronStartingDeadline   string
	SettingsOverride       string
}

func getenv(lookup func(string) (string, bool), key, def string) string {
	if v, ok := lookup(key); ok && v != "" {
		return v
	}
	return def
}

func parseBool(v string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "", "0", "false", "no", "off":
		return false, nil
	}
	return false, fmt.Errorf("not a boolean: %q", v)
}

// LoadEnv reads the environment (lookup is os.LookupEnv in production).
func LoadEnv(lookup func(string) (string, bool)) (*Env, error) {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	e := &Env{
		DatabaseURL:            getenv(lookup, "DATABASE_URL", ""),
		HTTPAddr:               getenv(lookup, "GOBEAT_HTTP_ADDR", ":9099"),
		LogLevel:               getenv(lookup, "GOBEAT_LOG_LEVEL", "info"),
		LogFormat:              getenv(lookup, "GOBEAT_LOG_FORMAT", "json"),
		MissedRunPolicy:        getenv(lookup, "GOBEAT_MISSED_RUN_POLICY", "catch_up_once"),
		SettingsKey:            getenv(lookup, "GOBEAT_SETTINGS_KEY", "default"),
		BrokerURL:              getenv(lookup, "GOBEAT_BROKER_URL", ""),
		BrokerTransportOptions: getenv(lookup, "GOBEAT_BROKER_TRANSPORT_OPTIONS", ""),
		Timezone:               getenv(lookup, "GOBEAT_TIMEZONE", ""),
		MaxLoopInterval:        getenv(lookup, "GOBEAT_MAX_LOOP_INTERVAL", ""),
		CronStartingDeadline:   getenv(lookup, "GOBEAT_CRON_STARTING_DEADLINE", ""),
		SettingsOverride:       getenv(lookup, "GOBEAT_SETTINGS_OVERRIDE", ""),
	}
	if e.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required (postgres://user:password@host:5432/dbname)")
	}
	if e.MissedRunPolicy != "catch_up_once" && e.MissedRunPolicy != "skip" {
		return nil, fmt.Errorf("GOBEAT_MISSED_RUN_POLICY must be catch_up_once or skip, got %q", e.MissedRunPolicy)
	}
	var err error
	if e.DryRun, err = parseBool(getenv(lookup, "GOBEAT_DRY_RUN", "")); err != nil {
		return nil, fmt.Errorf("GOBEAT_DRY_RUN: %w", err)
	}
	if e.AllowUnsupportedVersions, err = parseBool(getenv(lookup, "GOBEAT_ALLOW_UNSUPPORTED_VERSIONS", "")); err != nil {
		return nil, fmt.Errorf("GOBEAT_ALLOW_UNSUPPORTED_VERSIONS: %w", err)
	}
	for _, d := range []struct {
		key string
		def string
		dst *time.Duration
	}{
		{"GOBEAT_MISSED_RUN_GRACE", "60s", &e.MissedRunGrace},
		{"GOBEAT_PUBLISH_TIMEOUT", "10s", &e.PublishTimeout},
		{"GOBEAT_PUBLISH_MAX_AGE", "1h", &e.PublishMaxAge},
		{"GOBEAT_JOURNAL_RETENTION", "24h", &e.Retention},
		{"GOBEAT_HEALTH_MAX_AGE", "30s", &e.HealthMaxAge},
		{"GOBEAT_STANDBY_INTERVAL", "5s", &e.StandbyInterval},
	} {
		v, err := time.ParseDuration(getenv(lookup, d.key, d.def))
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("%s: invalid duration %q", d.key, getenv(lookup, d.key, d.def))
		}
		*d.dst = v
	}
	if v := getenv(lookup, "GOBEAT_LOCK_KEY", ""); v != "" {
		if e.LockKey, err = strconv.ParseInt(v, 10, 64); err != nil {
			return nil, fmt.Errorf("GOBEAT_LOCK_KEY: %w", err)
		}
	}
	return e, nil
}

// Override applies the environment overrides to a settings document;
// environment variables win over the settings row.
func (e *Env) Override(s *Settings) error {
	if e.SettingsOverride != "" {
		// generic escape hatch: a JSON object merged over the document
		raw, err := json.Marshal(s)
		if err != nil {
			return err
		}
		var doc, over map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			return err
		}
		if err := json.Unmarshal([]byte(e.SettingsOverride), &over); err != nil {
			return fmt.Errorf("GOBEAT_SETTINGS_OVERRIDE: %w", err)
		}
		for k, v := range over {
			doc[k] = v
		}
		merged, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		ns, err := ParseSettings(merged)
		if err != nil {
			return fmt.Errorf("GOBEAT_SETTINGS_OVERRIDE: %w", err)
		}
		*s = *ns
	}
	if e.BrokerURL != "" {
		s.BrokerURL = &e.BrokerURL
	}
	if e.BrokerTransportOptions != "" {
		var to map[string]any
		if err := json.Unmarshal([]byte(e.BrokerTransportOptions), &to); err != nil {
			return fmt.Errorf("GOBEAT_BROKER_TRANSPORT_OPTIONS: %w", err)
		}
		s.BrokerTransportOptions = to
	}
	if e.Timezone != "" {
		s.Timezone = &e.Timezone
	}
	if e.MaxLoopInterval != "" {
		v, err := strconv.ParseFloat(e.MaxLoopInterval, 64)
		if err != nil || v <= 0 {
			return fmt.Errorf("GOBEAT_MAX_LOOP_INTERVAL: invalid seconds %q", e.MaxLoopInterval)
		}
		s.BeatMaxLoopInterval = v
	}
	if e.CronStartingDeadline != "" {
		v, err := strconv.ParseFloat(e.CronStartingDeadline, 64)
		if err != nil {
			return fmt.Errorf("GOBEAT_CRON_STARTING_DEADLINE: invalid seconds %q", e.CronStartingDeadline)
		}
		s.BeatCronDeadline = &v
	}
	return nil
}
