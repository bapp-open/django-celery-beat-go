# gobeat

A Go service that replaces the `celery beat` process for Django projects.
Workers and tasks stay exactly as they are; only the beat process changes.
Periodic jobs come from two places:

* **`cron.py` in your apps** — classes extending `CronInterface`, picked
  up automatically the way Django picks up `models.py` (stored in
  gobeat's own table; needs nothing but Celery), and
* **django-celery-beat's tables** — optional: when that app is installed
  its `PeriodicTask` rows and admin keep working unchanged, with
  behaviour identical to `DatabaseScheduler`.

* **Small**: one static binary on an empty image (16 MB), ~12 MB RSS in
  the integration environment (target: under 20 MB).
* **Safer**: every run is claimed in Postgres (compare-and-swap on
  `last_run_at`) *before* it is published, the rendered message is kept in
  a journal until the broker confirms it, and only one replica is active
  (advisory lock).
* **Same behaviour**: schedule maths and message bytes are checked
  against real Celery, case by case (see [Testing](#testing)).

Reference versions: **Celery 5.7.0, kombu 5.7.0, django-celery-beat 2.9.0**
(latest stable). gobeat tracks the latest stable Celery only and refuses
to start on anything else — see [Version support](#version-support).

Design notes and the source map (which Python file each behaviour was
copied from) are in [docs/DESIGN.md](docs/DESIGN.md).

## How it fits together

```
manage.py migrate && manage.py gobeat_sync      (Django project, at deploy)
        │ writes one JSON row: broker, timezone, queues, per-task routing
        ▼
   Postgres  ◀── LISTEN/NOTIFY + polling ──  gobeat container  ──▶ Redis / RabbitMQ ──▶ workers
   (django_celery_beat tables, settings row, dispatch journal)
```

gobeat never imports your code. Everything that needs the Celery app
(`task_routes`, decorator options, queue declarations, `beat_schedule`)
is resolved by Celery itself inside `gobeat_sync`.

## Periodic jobs in `cron.py`

```python
# myapp/cron.py  - discovered automatically for every app in INSTALLED_APPS
from datetime import timedelta
from django_gobeat import CronInterface, crontab

class NightlyCleanup(CronInterface):
    schedule = crontab(minute=0, hour=4)      # celery.schedules.crontab, a timedelta, or seconds
    timezone = "Europe/Bucharest"             # crontab zone; default: CELERY_TIMEZONE
    queue = "maintenance"                     # also: exchange, routing_key, priority, expires, headers

    def run(self):                            # executed by a Celery worker
        ...

class Heartbeat(CronInterface):
    schedule = timedelta(seconds=30)
    task = "myapp.tasks.ping"                 # schedule an existing task instead of run()
    kwargs = {"source": "cron"}
```

* A class with `run()` is registered as a Celery task named after the job
  (`myapp.cron.NightlyCleanup`, or `name = "..."`); workers find it
  because discovery happens in `AppConfig.ready()`.
* Project conventions go into your own base class: mark it
  `abstract = True` and override `get_schedule()`, `get_args()`,
  `get_kwargs()`, `get_headers()`, `is_enabled()` or `run()`.
* `manage.py gobeat_sync` writes the discovered jobs to
  `django_gobeat_job` (creating, updating and removing rows to match the
  code; run counts and the admin's *paused* switch are kept). Jobs can
  also be created by hand in the admin (`source = admin`).
* Without django-celery-beat, entries of `app.conf.beat_schedule` are
  written to the same table.
* Semantics are Celery's: crontabs are evaluated exactly like
  `celery.schedules.crontab` in the job's zone; an interval job first
  runs one interval after it was defined and then every interval.

## Migration steps

1. Install the companion app in the Django project (`django-celery-beat`
   may stay installed or not — gobeat uses its tables only when it is):

   ```python
   INSTALLED_APPS += ["django_gobeat"]
   # optional: GOBEAT_CELERY_APP = "proj.celery:app"  (default: celery.current_app)
   ```

2. Run, on every deploy, after `migrate`:

   ```sh
   python manage.py migrate
   python manage.py gobeat_sync        # prints warnings for anything it cannot resolve exactly
   ```

   `gobeat_sync` writes the `cron.py` jobs, and code-defined
   `beat_schedule` entries into `PeriodicTask` as Python beat does on
   start-up (`DatabaseScheduler.setup_schedule`).

3. **Shadow first.** Keep Python beat running and start gobeat beside it
   in dry-run mode; it logs what it would dispatch and writes nothing:

   ```sh
   docker run -e DATABASE_URL=postgres://… -e GOBEAT_DRY_RUN=1 gobeat
   ```

   After a day, compare the two logs:

   ```sh
   gobeat shadow-diff --python-log beat.log --gobeat-log gobeat.log \
       --python-tz Europe/Bucharest --tolerance 2s
   ```

   (`beat.log` is `celery beat --loglevel=INFO` output in Celery's default
   format; `--python-tz` is the zone of its timestamps, normally Django's
   `TIME_ZONE`.) Exit status 0 means every dispatch has a partner.

4. Cut over: stop Python beat, then start gobeat without `GOBEAT_DRY_RUN`.
   Do not run both live — Python beat does not take part in the claim.

## Rollback

Stop gobeat, start `celery beat` again. gobeat keeps `last_run_at` and
`total_run_count` in the django-celery-beat tables up to date, so Python
beat continues from where gobeat stopped. Jobs that exist only in
`cron.py` / `django_gobeat_job` are not known to Python beat: list them in
`beat_schedule` if you need them during a rollback. The `django_gobeat_*`
tables can stay or be dropped with `manage.py migrate django_gobeat zero`.

## Configuration

One container per project. Only `DATABASE_URL` is required; everything
else comes from the settings row and environment variables override it.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | – (required) | Postgres URL of the Django database |
| `GOBEAT_DRY_RUN` | `0` | log dispatches, write and publish nothing (shadow mode) |
| `GOBEAT_MISSED_RUN_POLICY` | `catch_up_once` | after downtime: one run for the gap (what Python beat does), or `skip` |
| `GOBEAT_MISSED_RUN_GRACE` | `60s` | lateness beyond which `skip` skips a run |
| `GOBEAT_PUBLISH_TIMEOUT` | `10s` | per publish attempt |
| `GOBEAT_PUBLISH_MAX_AGE` | `1h` | keep retrying a claimed run this long, then abandon it loudly |
| `GOBEAT_JOURNAL_RETENTION` | `24h` | keep published journal rows this long |
| `GOBEAT_HTTP_ADDR` | `:9099` | `/healthz` and `/metrics` (`off` disables) |
| `GOBEAT_HEALTH_MAX_AGE` | `30s` | `/healthz` fails when the loop has not ticked for this long |
| `GOBEAT_STANDBY_INTERVAL` | `5s` | how often a standby replica retries the lock |
| `GOBEAT_LOCK_KEY` | `0x676f62656174` | advisory lock key |
| `GOBEAT_SETTINGS_KEY` | `default` | settings row to use |
| `GOBEAT_LOG_LEVEL` / `GOBEAT_LOG_FORMAT` | `info` / `json` | |
| `GOBEAT_ALLOW_UNSUPPORTED_VERSIONS` | `0` | run against untested Celery/django-celery-beat versions |
| **overrides of the settings row** | | |
| `GOBEAT_BROKER_URL` | row | `redis://`, `rediss://`, `amqp://`, `amqps://` |
| `GOBEAT_BROKER_TRANSPORT_OPTIONS` | row | JSON (`priority_steps`, `sep`, `global_keyprefix`) |
| `GOBEAT_TIMEZONE` | row | `CELERY_TIMEZONE` |
| `GOBEAT_MAX_LOOP_INTERVAL` | row (5) | seconds, `beat_max_loop_interval` |
| `GOBEAT_CRON_STARTING_DEADLINE` | row | seconds, `beat_cron_starting_deadline` |
| `GOBEAT_SETTINGS_OVERRIDE` | – | JSON object merged over the settings document |

The broker URL (with its password) is part of the settings row; use
`gobeat_sync --no-broker-url` and `GOBEAT_BROKER_URL` to keep it out of
the database.

gobeat **refuses to start**, with an explanatory error, when the settings
row is missing, the django-celery-beat schema is unknown, the versions are
unsupported or the configuration cannot be reproduced (see below). Once
started it never exits on database or broker failures; it retries with
backoff.

### Endpoints

* `GET /healthz` – 200 while the loop ticks (leader, standby or retrying
  a dead database), 503 when it has not ticked for `GOBEAT_HEALTH_MAX_AGE`.
  The image's `HEALTHCHECK` uses `gobeat healthcheck`.
* `GET /metrics` – Prometheus text: `gobeat_dispatch_total`,
  `gobeat_dispatch_lateness_seconds` (histogram), `gobeat_errors_total{kind}`,
  `gobeat_reloads_total{reason}`, `gobeat_claim_conflicts_total`,
  `gobeat_skipped_runs_total`, `gobeat_abandoned_total`,
  `gobeat_publish_retries_total`, `gobeat_pending_publishes`,
  `gobeat_leader`, `gobeat_tasks`, `gobeat_tasks_refused`,
  `gobeat_last_tick_timestamp_seconds`.

## Delivery guarantees

* **Claim before publish.** One SQL statement updates `last_run_at` /
  `total_run_count` only if `last_run_at` is still what gobeat saw, and
  inserts the rendered message into `django_gobeat_dispatch`. Only then is
  the message published; the row is marked when the broker accepted it
  (Redis reply / RabbitMQ publisher confirm).
* **No lost claimed runs.** Broker down: runs keep being claimed and wait
  in the journal, published in order when the broker is back. Crash
  between claim and publish: the next leader publishes the orphan first.
  A run is given up only after `GOBEAT_PUBLISH_MAX_AGE`, with an error log
  and `gobeat_abandoned_total`.
* **At-least-once, one edge.** If the process dies after the broker
  accepted a message but before the journal row was marked, that one run
  is published again after restart — with the same task id.
* **Deterministic task ids**: UUIDv5 of (periodic task id, scheduled
  instant), so a duplicate is recognisable; the journal's unique task id
  also stops the same slot from being claimed twice.
* **One leader.** A second replica stands by on the advisory lock and
  takes over within `GOBEAT_STANDBY_INTERVAL` of the leader's connection
  ending.
* **After downtime** (gobeat, database, or a forward clock jump): one
  catch-up run per task, then the normal cadence (`catch_up_once`), or no
  catch-up (`skip`). A clock stepping backwards delays runs; it never
  repeats one.

## Known differences from Python beat

Deliberate:

1. **State is in the database.** Python writes `last_run_at` every three
   minutes; gobeat writes it with every claim.
2. **One-off tasks are disabled right after their run**, not at the next
   evaluation; the resulting row is the same (`enabled=False`,
   `total_run_count=0`).
3. **No crontab hour window on load.** django-celery-beat 2.8 only loads
   numeric-hour crontabs within ±2 h of the current hour. A side effect:
   a run missed during downtime is caught up when its hour window opens
   the next day. gobeat catches up at start-up.
4. **Publish failures are retried** instead of being logged and dropped,
   and messages are published with publisher confirms and `mandatory`
   (RabbitMQ) — an unroutable message is an error, not a silent drop.
5. **Refused, never guessed** (an error names the task; it is not
   scheduled): solar schedules; non-JSON serializers; compression other
   than gzip/zlib; `args`/`kwargs`/`headers` that are not a JSON
   array/object or contain kombu `__type__` markers; non-positive
   intervals; crontabs that never match (February 31st, a `ValueError`
   in Celery too); routing that ends on an empty queue name (Python
   publishes to a queue called `""`); fan-out exchanges on Redis.
   Python beat *disables* a task whose JSON does not parse; gobeat only
   skips it.
6. **Refused at start-up**: `USE_TZ` ≠ `DJANGO_CELERY_BEAT_TZ_AWARE`,
   `enable_utc=False` without `CELERY_TIMEZONE`, `task_always_eager`,
   task protocol 1, several broker URLs, other transports.

Not reproduced (things only a Python process can do):

* `before_task_publish` / `after_task_publish` signal handlers
  (Sentry/OpenTelemetry header injection), `task-sent` events, custom
  `Task.apply_async` / `shadow_name`, argument checking (`typing=True`).
* Router *functions* in `task_routes` are evaluated once per task by
  `gobeat_sync` with empty arguments (a warning is printed); routes that
  depend on call arguments are not reproduced.
* `reply_to` is a random id; no reply queue is declared for the RPC
  result backend.
* gzip/zlib bodies decompress to the identical bytes but the compressed
  stream itself is produced by Go's zlib.
* AMQP: content header and body are byte-identical; the `Basic.Publish`
  frame additionally sets `mandatory`.

Inherited from Celery on purpose (the reference version does this):

* Crontabs are wall-clock in their own time zone: a time that does not
  exist on the spring-forward day runs an hour later, a repeated time
  runs once, and an every-minute crontab in a DST zone skips the repeated
  hour in autumn. Intervals are elapsed time and are not affected by DST.
* A crontab with day-of-month *and* day-of-week needs both to match.
* A never-run task counts from `date_changed`; with `start_time` it first
  runs at `start_time`.

## Version support

gobeat checks the versions recorded by `gobeat_sync` and the applied
django-celery-beat migrations before doing anything.

| | status |
|---|---|
| Celery 5.7.x | supported (reference: 5.7.0) |
| django-celery-beat 2.8.x, 2.9.x | supported: goldens regenerated with 2.8.1 and 2.9.0 are byte-identical |
| Celery ≤ 5.6 | refused: crontab arithmetic around DST, interval arithmetic, `saferepr`, the message headers and beat's tick differ from 5.7 |
| django-celery-beat ≤ 2.7 | refused: a never-run task starts from "now" instead of `date_changed` |

When a new Celery release appears: bump `tests/requirements.txt`, run
`make golden soak-golden` and `make ci`; every behavioural change shows up
as a failing golden case.

## Testing

`make ci` runs everything; the pieces:

| | command | what |
|---|---|---|
| unit | `make test` | schedule maths, crontab parsing, time zones, message encoding, config, HTTP, shadow tool — with `-race` and a goroutine-leak check in every package |
| differential | `make test` (replay), `make golden` (regenerate) | 70,000 schedule cases, 6,000 JSON/repr cases and 6,000 messages produced by real Celery/kombu/django-celery-beat (`tests/differential`); messages are captured from real Redis and RabbitMQ publishes |
| fuzz | `make fuzz` | crontab parser, crontab evaluation, JSON loader, message encoder |
| soak | `make test`, `make soak-golden` | real `DatabaseScheduler.tick()` on a fake clock vs gobeat's simulation: identical dispatch counts per task; 10,000 tasks × 30 days with a flat heap |
| version matrix | `make matrix` | goldens regenerated under each supported version must be byte-identical |
| Django app | `make pytest` | `cron.py` discovery, `CronInterface`, job sync |
| integration | `make integration` | docker compose: Postgres, Redis, RabbitMQ, a Django project, real workers — on both brokers, plus a pass with django-celery-beat not installed |
| failure injection | `make integration` | database down/up, broker down/up, SIGKILL between claim and publish and between publish and journal mark, network partition, two instances with failover, missed-run policies; clock jumps on a fake clock in `internal/scheduler` |
| shadow | `make integration` | Python beat and gobeat dry-run side by side, logs compared with `gobeat shadow-diff` |

## Development

```sh
make build            # bin/gobeat (static)
make test             # needs only Go; golden files are committed
make docker           # the image
```
