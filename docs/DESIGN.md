# gobeat design

gobeat replaces the `celery beat` process for Django projects that use
django-celery-beat's `DatabaseScheduler`. Workers, tasks and the admin are
untouched. The Python implementation is the specification; every behaviour
copied from it cites the file it came from (see "Source map" below and the
comments in the Go code).

Pinned reference versions: **celery 5.7.0, kombu 5.7.0, django-celery-beat
2.9.0** - the latest stable Celery is the only supported one; the golden
files are regenerated whenever the pin moves (`tests/differential`).

## Components

```
Django project                          gobeat container
┌──────────────────────────┐            ┌─────────────────────────────────┐
│ django_gobeat            │            │ config   (settings row + env)   │
│  manage.py gobeat_sync ──┼─ JSON row ─▶ store    (Postgres, one conn)   │
│  trigger → NOTIFY        │            │ schedule (crontab/interval/...) │
│ django_celery_beat tables│◀── CAS ────│ scheduler(tick loop, fake-able) │
└──────────────────────────┘            │ message  (Celery v2 / kombu)    │
                                        │ broker   (Redis RESP / AMQP)    │
                                        │ httpd    (/healthz, /metrics)   │
                                        └─────────────────────────────────┘
```

* `gobeat_sync` (Python) does everything that needs the Celery app: it
  resolves `task_routes`, decorator options and queue declarations, syncs
  `beat_schedule` into `PeriodicTask` exactly as `DatabaseScheduler.setup_schedule`
  does, and writes one JSON row plus a version hash.
* gobeat (Go) never imports project code. It refuses to start without the
  settings row, on an unknown django-celery-beat schema, or on an
  unsupported Celery/django-celery-beat version.

## Schedule sources

gobeat schedules rows from two tables through the same loop:

* `django_gobeat_job` - gobeat's own table. `gobeat_sync` fills it from
  the `CronInterface` classes found in every app's `cron.py`
  (`django_gobeat/cron.py`: `autodiscover_modules('cron')` in
  `AppConfig.ready`, a registry filled by `__init_subclass__`); rows can
  also be created in the admin. Crontab jobs are evaluated with the same
  code as `TzAwareCrontab`, in the job's zone or `CELERY_TIMEZONE`.
* `django_celery_beat_*` - only when django-celery-beat is installed in
  the project (the settings row then records its version). Behaviour is
  `DatabaseScheduler`'s.

A task is identified by `(source, primary key)`; the journal records the
source, and task ids are derived from that pair plus the scheduled time.

## Scheduling model

Python beat keeps a heap of `(next_check, entry)`; the top entry is
re-evaluated with `entry.is_due()` and, when due, `last_run_at = now`,
`total_run_count += 1`, the message is sent, and the entry is re-queued at
`now + next` (`celery/beat.py: Scheduler.tick`, `django_celery_beat/schedulers.py:
ModelEntry.__next__`).

gobeat keeps the same quantities:

* `is_due(entry, now) -> (due, next)` is a pure port of
  `ModelEntry.is_due` + `schedule.is_due` / `TzAwareCrontab.is_due` /
  `crontab.is_due` / `clocked.is_due`. It is verified case-by-case against
  golden files produced by real Celery.
* every entry has `nextCheck`; on (re)load `nextCheck = now + (0 if due else next)`
  (`Scheduler.populate_heap`), after a dispatch `nextCheck = now + next`.
* the schedule is reloaded when the `PeriodicTasks` marker changes and at
  least every 300 s (`SCHEDULE_SYNC_MAX_INTERVAL`), and the loop never
  sleeps longer than `beat_max_loop_interval` (default 5 s).

Deliberate differences from the Python loop (all listed in the README):

1. **State lives in the database, not in memory.** Python writes
   `last_run_at` lazily (every 3 min); gobeat writes it as part of the claim.
2. **Claim before publish.** One transaction does
   `UPDATE periodictask SET last_run_at=$now, total_run_count=total_run_count+1
   WHERE id=$id AND last_run_at IS NOT DISTINCT FROM $seen` and inserts a
   row into `django_gobeat_dispatch` (the dispatch journal, holding the
   fully rendered message). The message is published only if the UPDATE
   matched one row; the journal row is then marked published. A crash
   between claim and publish leaves an unpublished journal row which the
   next leader publishes first — a claimed run is never dropped, and after a
   crash may be delivered twice with the *same* task id (at-least-once).
3. **Deterministic task id**: UUIDv5(namespace, "<periodic task id>:<scheduled
   time in µs>") where scheduled time is the instant the run became due.
4. **No crontab hour-window / clocked look-ahead filter** on load
   (`DatabaseScheduler.enabled_models_qs`); those are load optimisations in
   Python with a side effect on catch-up timing that we do not copy.
5. **Missed-run policy** is explicit: `catch_up_once` (default, what Python
   does: one run, then schedule from now) or `skip` (advance `last_run_at`
   without publishing when a run is later than `GOBEAT_MISSED_RUN_GRACE`).

## Leadership

A session-level `pg_try_advisory_lock` is taken on the *same* connection
that executes claims. A replica that cannot take the lock stands by and
retries. If the connection drops the lock is gone and so is the ability to
claim, so two leaders cannot both claim; the CAS is the second line of
defence.

## Time

`clock.Clock` is injected everywhere; scheduling code never calls
`time.Now()`. Crontab arithmetic follows Python's `datetime` semantics
(naive field arithmetic, then conversion with `fold=0`, PEP 495) because
`ffwd.__radd__` relies on it; this is what decides DST gap/overlap
behaviour. Intervals are elapsed time (`add_seconds_to_datetime`). `internal/pytime` implements it on top
of `time.Time.ZoneBounds`.

## Messages

`internal/message` ports `Task.apply_async` → `Celery.send_task` →
`Router.route` → `AMQP.as_task_v2` → `send_task_message` →
`kombu.Producer.publish`. Redis payloads are byte-identical to kombu's
(`kombu/transport/virtual/base.py`, `kombu/transport/redis.py`); for AMQP
the body is byte-identical and the properties/headers are field- and
type-identical (py-amqp's table type tags are reproduced; table key order
on the wire is not observable by consumers).

Unsupported inputs are never guessed: solar schedules, non-JSON
serializers, unknown compression, kombu typed-JSON markers, fan-out
exchanges on Redis, etc. log an error naming the task and skip it.

## Testing strategy

| # | What | Where |
|---|------|-------|
| 1 | unit | `internal/*/*_test.go` |
| 2 | differential vs real Celery | `tests/differential/gen_golden.py` → `testdata/golden`, `internal/*/golden_test.go` |
| 3 | fuzz | `Fuzz*` in `internal/schedule`, `internal/pyjson`, `internal/message` |
| 4 | integration | `tests/integration` (compose: Postgres, Redis, RabbitMQ, Django, workers) |
| 5 | failure injection | `tests/integration/test_failures.py` + in-memory fault tests in `internal/scheduler` |
| 6 | soak | `tests/differential/gen_soak.py` (real Python beat, fake clock) vs `internal/scheduler/soak_test.go` |
| 7 | race + goroutine leaks | `go test -race`, `internal/leakcheck` in every package's `TestMain` |
| 8 | version matrix | `tests/differential/matrix.sh`, schema fingerprint in `internal/store` |
| 9 | shadow diff | `gobeat shadow-diff`, `tests/integration/test_shadow.py` |

## Source map (pinned versions)

| Behaviour | Python source |
|-----------|---------------|
| crontab parsing | `celery/schedules.py: crontab_parser, crontab._expand_cronspec` |
| crontab next run, dow vs dom | `celery/schedules.py: crontab.remaining_delta, _delta_to_next` |
| wall arithmetic, remaining | `celery/utils/time.py: remaining, ffwd, maybe_make_aware, localize` |
| tz-aware crontab due | `django_celery_beat/tzcrontab.py: TzAwareCrontab.is_due` |
| interval due | `celery/schedules.py: schedule.is_due`; `django_celery_beat/models.py: IntervalSchedule.schedule` |
| clocked due | `django_celery_beat/clockedschedule.py` |
| enabled/start_time/expires/one_off, null last_run_at | `django_celery_beat/schedulers.py: ModelEntry.__init__, is_due, __next__` |
| reload, change marker, 300 s resync | `django_celery_beat/schedulers.py: DatabaseScheduler.schedule, schedule_changed` |
| beat_schedule sync | `django_celery_beat/schedulers.py: update_from_dict, install_default_entries, ModelEntry.from_entry` |
| tick / next check | `celery/beat.py: Scheduler.tick, populate_heap, apply_async` |
| options merge | `celery/app/task.py: Task.apply_async`; `celery/app/base.py: Celery.send_task` |
| routing | `celery/app/routes.py: Router.route`; `celery/app/amqp.py: Queues, send_task_message` |
| v2 message | `celery/app/amqp.py: AMQP.as_task_v2`; `celery/utils/saferepr.py` |
| JSON body | `kombu/utils/json.py`, `kombu/serialization.py` |
| publish | `kombu/messaging.py: Producer.publish, _publish` |
| Redis envelope, priority queues | `kombu/transport/virtual/base.py: Channel.prepare_message, basic_publish`; `kombu/transport/redis.py: Channel._put, _q_for_pri` |
| Redis exchange routing | `kombu/transport/virtual/exchange.py`; `kombu/transport/redis.py: get_table, _queue_bind` |
| AMQP properties | `kombu/transport/pyamqp.py: Channel.prepare_message`; `amqp/basic_message.py`, `amqp/serialization.py` |
| queue declaration | `kombu/entity.py: Queue.declare, Exchange.declare`; `kombu/common.py: maybe_declare` |
