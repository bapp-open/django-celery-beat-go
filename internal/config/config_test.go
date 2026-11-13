package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestLoadEnv(t *testing.T) {
	if _, err := LoadEnv(env(nil)); err == nil || !strings.Contains(err.Error(), "DATABASE_URL is required") {
		t.Fatalf("missing DATABASE_URL: %v", err)
	}
	e, err := LoadEnv(env(map[string]string{"DATABASE_URL": "postgres://x"}))
	if err != nil {
		t.Fatal(err)
	}
	if e.DryRun || e.MissedRunPolicy != "catch_up_once" || e.HTTPAddr != ":9099" || e.PublishMaxAge != time.Hour || e.HealthMaxAge != 30*time.Second {
		t.Fatalf("defaults: %+v", e)
	}
	e, err = LoadEnv(env(map[string]string{"DATABASE_URL": "postgres://x", "GOBEAT_DRY_RUN": "true",
		"GOBEAT_MISSED_RUN_POLICY": "skip", "GOBEAT_MISSED_RUN_GRACE": "5m", "GOBEAT_LOCK_KEY": "42"}))
	if err != nil || !e.DryRun || e.MissedRunPolicy != "skip" || e.MissedRunGrace != 5*time.Minute || e.LockKey != 42 {
		t.Fatalf("%+v %v", e, err)
	}
	for _, bad := range []map[string]string{
		{"GOBEAT_MISSED_RUN_POLICY": "replay_all"}, {"GOBEAT_DRY_RUN": "maybe"},
		{"GOBEAT_PUBLISH_TIMEOUT": "soon"}, {"GOBEAT_LOCK_KEY": "x"}, {"GOBEAT_HEALTH_MAX_AGE": "-1s"},
	} {
		bad["DATABASE_URL"] = "postgres://x"
		if _, err := LoadEnv(env(bad)); err == nil {
			t.Errorf("%v must be rejected", bad)
		}
	}
}

const doc = `{"schema": 1, "broker_url": "redis://from-row/0", "timezone": "Europe/Bucharest", "enable_utc": true,
 "beat_max_loop_interval": 5, "task_protocol": 2}`

func TestEnvOverridesSettingsRow(t *testing.T) {
	s, err := ParseSettings([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	e, _ := LoadEnv(env(map[string]string{"DATABASE_URL": "x", "GOBEAT_BROKER_URL": "amqp://from-env//",
		"GOBEAT_TIMEZONE": "Asia/Tokyo", "GOBEAT_MAX_LOOP_INTERVAL": "2.5", "GOBEAT_CRON_STARTING_DEADLINE": "60",
		"GOBEAT_BROKER_TRANSPORT_OPTIONS": `{"priority_steps": [0, 5]}`,
		"GOBEAT_SETTINGS_OVERRIDE":        `{"task_default_queue": "other"}`}))
	if err := e.Override(s); err != nil {
		t.Fatal(err)
	}
	tz, _ := s.AppTimezone()
	if *s.BrokerURL != "amqp://from-env//" || tz.String() != "Asia/Tokyo" || s.BeatMaxLoopInterval != 2.5 ||
		*s.BeatCronDeadline != 60 || s.TaskDefaultQueue != "other" || len(s.BrokerTransportOptions) != 1 {
		t.Fatalf("overrides not applied: %+v", s)
	}
	e, _ = LoadEnv(env(map[string]string{"DATABASE_URL": "x", "GOBEAT_MAX_LOOP_INTERVAL": "zero"}))
	if err := e.Override(s); err == nil {
		t.Fatal("bad override must fail")
	}
}

func TestSettingsValidation(t *testing.T) {
	if _, err := ParseSettings([]byte(`{"schema": 7}`)); err == nil || !strings.Contains(err.Error(), "schema 7") {
		t.Fatalf("schema: %v", err)
	}
	if _, err := ParseSettings([]byte(`not json`)); err == nil {
		t.Fatal("garbage accepted")
	}
	s, _ := ParseSettings([]byte(`{"schema": 1, "enable_utc": false}`))
	if _, err := s.AppTimezone(); err == nil {
		t.Fatal("enable_utc=False without a timezone means host-local time; must be refused")
	}
	s, _ = ParseSettings([]byte(`{"schema": 1, "enable_utc": true}`))
	if tz, err := s.AppTimezone(); err != nil || tz != time.UTC {
		t.Fatalf("default timezone: %v %v", tz, err)
	}
}
