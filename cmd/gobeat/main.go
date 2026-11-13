// gobeat replaces the `celery beat` process for Django projects that use
// django-celery-beat's DatabaseScheduler.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata" // the image has no system zoneinfo

	"github.com/bapp-open/django-celery-beat-go/internal/broker"
	"github.com/bapp-open/django-celery-beat-go/internal/clock"
	"github.com/bapp-open/django-celery-beat-go/internal/config"
	"github.com/bapp-open/django-celery-beat-go/internal/httpd"
	"github.com/bapp-open/django-celery-beat-go/internal/metrics"
	"github.com/bapp-open/django-celery-beat-go/internal/scheduler"
	"github.com/bapp-open/django-celery-beat-go/internal/shadow"
	"github.com/bapp-open/django-celery-beat-go/internal/store"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "shadow-diff":
			os.Exit(shadow.Main(os.Args[2:], os.Stdout, os.Stderr))
		case "version", "--version", "-v":
			fmt.Println("gobeat", version)
			return
		case "healthcheck":
			os.Exit(healthcheck())
		case "run":
		default:
			fmt.Fprintf(os.Stderr, "usage: gobeat [run | shadow-diff | healthcheck | version]\n")
			os.Exit(2)
		}
	}
	os.Exit(run())
}

func newLogger(env *config.Env) *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(env.LogLevel)); err != nil {
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(env.LogFormat, "text") {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

func run() int {
	env, err := config.LoadEnv(nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gobeat: "+err.Error())
		return 2
	}
	log := newLogger(env)
	reg := metrics.New()
	mode := "live"
	if env.DryRun {
		mode = "dry_run"
	}
	reg.Set("gobeat_info", 1, "version", version, "mode", mode)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	health := httpd.NewHealth(env.HealthMaxAge, time.Now)
	httpErr := make(chan error, 1)
	httpDone := make(chan struct{})
	go func() {
		defer close(httpDone)
		if env.HTTPAddr == "" || env.HTTPAddr == "off" {
			return
		}
		if err := httpd.Serve(ctx, env.HTTPAddr, httpd.Handler(health, reg)); err != nil {
			httpErr <- err
		}
	}()

	hostname, _ := os.Hostname()
	sched := scheduler.New(scheduler.Options{
		Clock:        clock.System{},
		Opener:       &store.PostgresOpener{URL: env.DatabaseURL, LockKey: env.LockKey, SettingsKey: env.SettingsKey, ConnectTimeout: 10 * time.Second},
		Log:          log,
		Metrics:      reg,
		NewPublisher: broker.New,
		Override:     env.Override,

		DryRun:          env.DryRun,
		MissedRunPolicy: env.MissedRunPolicy,
		MissedRunGrace:  env.MissedRunGrace,
		PublishTimeout:  env.PublishTimeout,
		PublishMaxAge:   env.PublishMaxAge,
		Retention:       env.Retention,
		StandbyInterval: env.StandbyInterval,
		// celery/utils/nodenames.py: anon_nodename
		Origin:                   fmt.Sprintf("gen%d@%s", os.Getpid(), hostname),
		ReplyTo:                  scheduler.NewReplyTo(),
		AllowUnsupportedVersions: env.AllowUnsupportedVersions,
		Beat:                     health.Beat,
	})
	log.Info("gobeat starting", "version", version, "dry_run", env.DryRun, "http", env.HTTPAddr)

	runErr := make(chan error, 1)
	go func() { runErr <- sched.Run(ctx) }()

	code := 0
	select {
	case err := <-runErr:
		var fatal *scheduler.FatalError
		if errors.As(err, &fatal) {
			log.Error("gobeat refuses to start", "error", err.Error())
			fmt.Fprintln(os.Stderr, "gobeat: refusing to start: "+err.Error())
			code = 1
		} else if err != nil {
			log.Error("scheduler stopped", "error", err.Error())
			code = 1
		}
		stop()
	case err := <-httpErr:
		log.Error("http server failed", "error", err.Error())
		code = 1
		stop()
		<-runErr
	}
	<-httpDone
	log.Info("gobeat stopped")
	return code
}

// healthcheck is used as the container HEALTHCHECK (the image has no shell).
func healthcheck() int {
	addr := os.Getenv("GOBEAT_HTTP_ADDR")
	if addr == "" {
		addr = ":9099"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	ok, err := httpd.Probe("http://" + addr + "/healthz")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if !ok {
		return 1
	}
	return 0
}
