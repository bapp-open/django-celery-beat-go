#!/usr/bin/env python
"""Soak reference: real Python beat, fake clock, 30 simulated days.

Runs django_celery_beat.schedulers.DatabaseScheduler (the real class, on
an in-memory SQLite database) through its own tick() loop with the clock
replaced by time-machine, and records how often every periodic task was
dispatched. internal/scheduler/soak_test.go replays the same task set
through gobeat and must arrive at exactly the same counts.

The task set uses intervals, crontabs in ten time zones (two DST changes
fall inside the period), start_time, null and non-null last_run_at and
clocked one-off tasks. It deliberately contains no `expires` and no
one-off *interval* tasks: when such an entry is finished Python beat keeps
it at the top of its heap, "not due", and re-checks only that entry
(celery/beat.py: Scheduler.tick) until the next schedule reload - 5 s
later for a one-off, up to 5 minutes later for an expiry - delaying every
other task meanwhile. gobeat does not copy that head-of-line blocking
(README, "Known differences"), so counts would differ by design.

Only two things differ from a production beat:
  * apply_entry counts instead of publishing;
  * Scheduler.adjust's 10 ms timer slack is zero (the simulated sleep is
    exact, so there is nothing to compensate).

    python gen_soak.py --out ../../testdata/soak [--tasks 1000] [--days 30]
"""
import argparse
import collections
import gzip
import json
import math
import os
import random
import sys
import time as _time
from datetime import datetime, timedelta, timezone as dt_timezone

ap = argparse.ArgumentParser()
ap.add_argument('--out', required=True)
ap.add_argument('--tasks', type=int, default=1000)
ap.add_argument('--days', type=float, default=30)
ap.add_argument('--seed', type=int, default=20241020)
ARGS = ap.parse_args()

os.environ['TZ'] = 'UTC'
_time.tzset()

import django  # noqa: E402
from django.conf import settings  # noqa: E402

settings.configure(
    USE_TZ=True, TIME_ZONE='UTC',
    INSTALLED_APPS=['django.contrib.contenttypes', 'django_celery_beat'],
    DATABASES={'default': {'ENGINE': 'django.db.backends.sqlite3',
                           'NAME': ':memory:'}},
    DEFAULT_AUTO_FIELD='django.db.models.AutoField',
)
django.setup()

import time_machine  # noqa: E402
from zoneinfo import ZoneInfo  # noqa: E402

from celery import Celery  # noqa: E402
from django.core.management import call_command  # noqa: E402
from django_celery_beat.models import (ClockedSchedule, CrontabSchedule,  # noqa: E402
                                       IntervalSchedule, PeriodicTask)
from django_celery_beat.schedulers import DatabaseScheduler  # noqa: E402

UTC = dt_timezone.utc
T0 = datetime(2024, 10, 20, tzinfo=UTC)       # EU and US leave DST inside
END = T0 + timedelta(days=ARGS.days)

ZONES = ['UTC', 'Europe/Bucharest', 'Europe/London', 'America/New_York',
         'America/Los_Angeles', 'Asia/Tokyo', 'Asia/Kolkata',
         'Australia/Sydney', 'America/Sao_Paulo', 'Pacific/Auckland']
INTERVALS = [(60, 2), (90, 1), (300, 6), (600, 8), (900, 8), (1800, 10),
             (3600, 14), (7200, 10), (21600, 10), (43200, 8), (86400, 12),
             (172800, 5), (604800, 4), (1000000, 2)]
CRONS = [
    ('*/15', '*', '*', '*', '*'), ('0', '*', '*', '*', '*'),
    ('30', '*/2', '*', '*', '*'), ('0', '0', '*', '*', '*'),
    ('30', '1', '*', '*', '*'), ('30', '2', '*', '*', '*'),
    ('0', '3', '*', '*', '*'), ('15', '4', '*', '*', 'mon-fri'),
    ('0', '9', '*', '*', '1'), ('0', '0', '1', '*', '*'),
    ('0', '12', '1,15', '*', '*'), ('45', '23', '*', '*', '0'),
    ('0', '6', '*', '11', '*'), ('*/20', '8-17', '*', '*', 'mon-fri'),
    ('5', '0', '*', '*', '*'), ('0', '*/6', '*', '*', '*'),
    ('59', '23', '31', '*', '*'), ('0', '0', '29', '2', '*'),
    ('10', '1,2,3', '*', '*', 'sun'), ('0', '2', '27', '10', '*'),
]


def iso(dt):
    return None if dt is None else dt.astimezone(UTC).strftime(
        '%Y-%m-%dT%H:%M:%S.%fZ')


def make_specs():
    rng = random.Random(ARGS.seed)
    span = (END - T0).total_seconds()
    specs = []
    for i in range(1, ARGS.tasks + 1):
        s = {'id': i, 'name': 't%05d' % i, 'one_off': False,
             'start_time': None, 'expires': None, 'last_run_at': None}
        r = rng.random()
        if r < 0.55:
            every = rng.choices([e for e, _ in INTERVALS],
                                [w for _, w in INTERVALS])[0]
            if rng.random() < 0.15:
                every = rng.randint(120, 200000)
            s.update(kind='interval', every=every, period='seconds')
        elif r < 0.93:
            m, h, dom, moy, dow = rng.choice(CRONS)
            s.update(kind='crontab', m=m, h=h, dom=dom, moy=moy, dow=dow,
                     tz=rng.choice(ZONES))
        else:
            when = T0 + timedelta(seconds=round(rng.uniform(-3600, span), 3))
            s.update(kind='clocked', clocked_time=iso(when), one_off=True)
        changed = T0 - timedelta(microseconds=rng.randrange(2 * 86400 * 10 ** 6))
        s['date_changed'] = iso(changed)
        if s['kind'] != 'clocked':
            if rng.random() < 0.6:
                back = rng.uniform(0, s.get('every', 86400))
                s['last_run_at'] = iso(T0 - timedelta(
                    microseconds=int(back * 10 ** 6)))
            if rng.random() < 0.10:
                s['start_time'] = iso(T0 + timedelta(
                    seconds=round(rng.uniform(0, span * 0.7), 3)))
        specs.append(s)
    return specs


def parse(s):
    return None if s is None else datetime.strptime(
        s, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=UTC)


def load(specs):
    call_command('migrate', verbosity=0)
    for s in specs:
        kw = {}
        if s['kind'] == 'interval':
            kw['interval'], _ = IntervalSchedule.objects.get_or_create(
                every=s['every'], period=s['period'])
        elif s['kind'] == 'crontab':
            kw['crontab'], _ = CrontabSchedule.objects.get_or_create(
                minute=s['m'], hour=s['h'], day_of_week=s['dow'],
                day_of_month=s['dom'], month_of_year=s['moy'],
                timezone=ZoneInfo(s['tz']))
        else:
            kw['clocked'] = ClockedSchedule.objects.create(
                clocked_time=parse(s['clocked_time']))
        PeriodicTask.objects.bulk_create([PeriodicTask(
            id=s['id'], name=s['name'], task='soak.task', one_off=s['one_off'],
            start_time=parse(s['start_time']), expires=parse(s['expires']),
            last_run_at=parse(s['last_run_at']), **kw)])
        # auto_now would stamp "now"; the row was last edited at date_changed
        PeriodicTask.objects.filter(pk=s['id']).update(
            date_changed=parse(s['date_changed']))


def main():
    os.makedirs(ARGS.out, exist_ok=True)
    specs = make_specs()
    counts = collections.Counter()
    started = _time.perf_counter()
    t0_us = int(T0.timestamp()) * 10 ** 6
    end_us = t0_us + int(ARGS.days * 86400 * 10 ** 6)

    def dest(t_us):
        # half a microsecond past the instant keeps float timestamps from
        # truncating to the previous microsecond
        return (t_us * 1000 + 500) / 1e9

    with time_machine.travel(datetime.fromtimestamp(dest(t0_us), ZoneInfo('UTC')),
                             tick=False) as traveller:
        load(specs)
        app = Celery('soak', set_as_current=True)
        app.conf.timezone = 'UTC'
        app.conf.result_expires = None      # no celery.backend_cleanup entry

        class Sim(DatabaseScheduler):
            producer = None

            def adjust(self, n, drift=0.0):
                return n

            def apply_entry(self, entry, producer=None):
                counts[entry.model.pk] += 1
                self._tasks_since_sync += 1
                if self.should_sync():
                    self._do_sync()

        sched = Sim(app=app)
        t_us = t0_us
        ticks = 0
        while t_us <= end_us:
            interval = sched.tick()
            ticks += 1
            if sched.should_sync():
                sched._do_sync()
            if interval and interval > 0:
                t_us += max(1, round(interval * 1e6))
                traveller.move_to(dest(t_us))
        sched.sync()
        final = {str(pk): [enabled, n] for pk, enabled, n in
                 PeriodicTask.objects.values_list('id', 'enabled',
                                                  'total_run_count')}

    doc = {'t0': iso(T0), 'end': iso(END), 'app_timezone': 'UTC',
           'tasks': ARGS.tasks, 'days': ARGS.days, 'seed': ARGS.seed,
           'total_dispatches': sum(counts.values()),
           'counts': {str(s['id']): counts.get(s['id'], 0) for s in specs},
           'final_enabled': {k: bool(v[0]) for k, v in sorted(final.items(), key=lambda kv: int(kv[0]))},
           'specs': specs}
    name = 'soak_%d_%gd.json.gz' % (ARGS.tasks, ARGS.days)
    with open(os.path.join(ARGS.out, name), 'wb') as fh:
        with gzip.GzipFile(filename='', mode='wb', fileobj=fh, mtime=0) as gz:
            gz.write(json.dumps(doc, sort_keys=True).encode())
    print('%s: %d tasks, %g days, %d dispatches, %d ticks, %.0fs' % (
        name, ARGS.tasks, ARGS.days, doc['total_dispatches'], ticks,
        _time.perf_counter() - started))


if __name__ == '__main__':
    main()
