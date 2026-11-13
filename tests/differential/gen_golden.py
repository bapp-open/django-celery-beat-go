#!/usr/bin/env python
"""Generate golden files from *real* Celery / kombu / django-celery-beat.

The Go test-suite replays every case and must match exactly.  Nothing in
this file re-implements scheduling logic: every expected value comes from
calling the library code, with only the wall clock replaced (time-machine).

Usage:
    python gen_golden.py --out ../../testdata/golden [--use-tz 1|0] [--only NAME]

``--use-tz 0`` generates the USE_TZ=False / DJANGO_CELERY_BEAT_TZ_AWARE=False
variant (naive datetimes, plain ``celery.schedules.crontab``).
"""
import argparse
import gzip
import io
import json
import math
import os
import random
import sys
from datetime import datetime, timedelta, timezone as dt_timezone

ap = argparse.ArgumentParser()
ap.add_argument('--out', required=True)
ap.add_argument('--use-tz', type=int, default=1)
ap.add_argument('--only', default='')
ap.add_argument('--scale', type=float, default=1.0)
ARGS = ap.parse_args()
USE_TZ = bool(ARGS.use_tz)

DJANGO_TIME_ZONE = 'Europe/Bucharest'
# django.conf.Settings.__init__ does exactly this for a settings module:
os.environ['TZ'] = DJANGO_TIME_ZONE
import time  # noqa: E402
time.tzset()

import django  # noqa: E402
from django.conf import settings  # noqa: E402

settings.configure(
    DEBUG=False,
    USE_TZ=USE_TZ,
    TIME_ZONE=DJANGO_TIME_ZONE,
    DJANGO_CELERY_BEAT_TZ_AWARE=USE_TZ,
    INSTALLED_APPS=['django.contrib.contenttypes', 'django_celery_beat'],
    DATABASES={'default': {'ENGINE': 'django.db.backends.sqlite3',
                           'NAME': ':memory:'}},
    DEFAULT_AUTO_FIELD='django.db.models.AutoField',
)
django.setup()

import time_machine  # noqa: E402
from zoneinfo import ZoneInfo  # noqa: E402

import celery  # noqa: E402
import kombu  # noqa: E402
import django_celery_beat  # noqa: E402
from celery import Celery, schedules  # noqa: E402
from django_celery_beat.models import (ClockedSchedule, CrontabSchedule,  # noqa: E402
                                       IntervalSchedule, PeriodicTask)
from django_celery_beat.schedulers import ModelEntry  # noqa: E402
from django_celery_beat.tzcrontab import TzAwareCrontab  # noqa: E402

UTC = dt_timezone.utc

ZONES = [
    'UTC', 'Europe/Bucharest', 'Europe/London', 'Europe/Berlin',
    'Europe/Dublin', 'Europe/Lisbon', 'Europe/Moscow', 'Europe/Istanbul',
    'America/New_York', 'America/Chicago', 'America/Denver',
    'America/Los_Angeles', 'America/Phoenix', 'America/Sao_Paulo',
    'America/St_Johns', 'America/Havana', 'America/Santiago',
    'America/Asuncion', 'America/Caracas', 'America/Mexico_City',
    'Asia/Tokyo', 'Asia/Kolkata', 'Asia/Kathmandu', 'Asia/Tehran',
    'Asia/Shanghai', 'Asia/Dubai', 'Asia/Jerusalem', 'Asia/Amman',
    'Australia/Sydney', 'Australia/Lord_Howe', 'Australia/Adelaide',
    'Australia/Perth', 'Pacific/Auckland', 'Pacific/Chatham',
    'Pacific/Apia', 'Pacific/Kiritimati', 'Pacific/Honolulu',
    'Africa/Cairo', 'Africa/Casablanca', 'Africa/Johannesburg',
    'Atlantic/Azores', 'Antarctica/Troll', 'Etc/GMT+12', 'Etc/GMT-14',
]

# Golden instants stay inside this window so that results do not depend on
# tzdata rules announced after either implementation's database was built.
T_MIN = datetime(2012, 1, 1, tzinfo=UTC)
T_MAX = datetime(2025, 6, 1, tzinfo=UTC)

_apps = {}


def app_for(tz, deadline=None, enable_utc=True):
    key = (tz, deadline, enable_utc)
    app = _apps.get(key)
    if app is None:
        app = Celery('golden-%d' % len(_apps), set_as_current=False)
        app.conf.timezone = tz
        app.conf.enable_utc = enable_utc
        app.conf.beat_cron_starting_deadline = deadline
        _apps[key] = app
    app.set_current()
    return app


_transitions = {}


def transitions(zone):
    """UTC instants where ``zone`` changes offset inside the window."""
    if zone in _transitions:
        return _transitions[zone]
    tz = ZoneInfo(zone)
    out = []
    t = T_MIN
    prev = t.astimezone(tz).utcoffset()
    step = timedelta(days=1)
    while t < T_MAX:
        n = t + step
        off = n.astimezone(tz).utcoffset()
        if off != prev:
            lo, hi = t, n
            while hi - lo > timedelta(seconds=1):
                mid = lo + (hi - lo) / 2
                if mid.astimezone(tz).utcoffset() == prev:
                    lo = mid
                else:
                    hi = mid
            out.append(hi.replace(microsecond=0))
            prev = off
        t = n
    _transitions[zone] = out
    return out


def travel(now):
    """Freeze the clock at ``now``.

    time-machine resets the process zone to UTC unless the destination
    carries a ZoneInfo; a Django process runs with TZ=settings.TIME_ZONE.
    """
    return time_machine.travel(now.astimezone(ZoneInfo(DJANGO_TIME_ZONE)),
                               tick=False)


def iso(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
    return dt.astimezone(UTC).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def rand_now(rng, zone):
    trs = transitions(zone)
    r = rng.random()
    if trs and r < 0.45:
        base = rng.choice(trs)
        t = base + timedelta(seconds=rng.uniform(-3 * 86400, 3 * 86400))
        if rng.random() < 0.3:
            t = base + timedelta(seconds=rng.uniform(-7200, 7200))
    else:
        span = (T_MAX - T_MIN).total_seconds()
        t = T_MIN + timedelta(seconds=rng.uniform(0, span))
    r = rng.random()
    if r < 0.25:
        t = t.replace(second=0, microsecond=0)
    elif r < 0.35:
        t = t.replace(microsecond=0)
    else:
        t = t.replace(microsecond=rng.randrange(1000000))
    return t


def rand_last(rng, now):
    r = rng.random()
    if r < 0.04:
        d = -rng.uniform(0, 7200)           # last run in the future
    elif r < 0.10:
        d = 0.0
    else:
        d = math.exp(rng.uniform(math.log(0.5), math.log(400 * 86400)))
    t = now - timedelta(seconds=d)
    r = rng.random()
    if r < 0.25:
        t = t.replace(second=0, microsecond=0)
    elif r < 0.35:
        t = t.replace(microsecond=0)
    return t


DOW_NAMES = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'Monday',
             'SUNDAY', 'Fri', 'saturday']
MOY_NAMES = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep',
             'oct', 'nov', 'dec', 'January', 'DEC', 'March']


def rand_field(rng, lo, hi, names=(), star_p=0.4):
    """Mostly-valid cron field for the range [lo, hi]."""
    r = rng.random()
    if r < star_p:
        return '*'

    def num():
        if names and rng.random() < 0.15:
            return rng.choice(names)
        return str(rng.randint(lo, hi))

    def expr():
        r = rng.random()
        if r < 0.45:
            return num()
        if r < 0.65:
            return '*/%d' % rng.randint(1, max(2, (hi - lo) // 2 + 2))
        if r < 0.90:
            return '%s-%s' % (num(), num())
        return '%s-%s/%d' % (num(), num(), rng.randint(1, 7))

    return ','.join(expr() for _ in range(rng.choice([1, 1, 1, 2, 3, 5])))


JUNK = ['', ' ', '*', '*/', '*/0', '/', '-', '1-', '-1', '1--2', '1-2-3',
        '1,,2', ',', '1,', 'a', 'abc', 'mon-fri', 'fri-mon', 'jan-mar',
        '*/x', '*x', '**', '1/2', '1-5/0', '5-1/2', '60', '61', '0', '00',
        '007', '1_0', ' 5', '5 ', '+5', '1.5', '1e1', '0x10', '٣', '１２',
        '1-٣', '*/٣', 'é', 'mar', 'marzipan', 'sunday', 'sunday-tuesday',
        '1-5!!', '1-5/2xyz', '*/5 ', '* ', ' *', '1 - 5', '1-5,*/20',
        '99999999999999999999', '-0', '0-0', '7', '0-7', '1-31/31', '31',
        '32', '12', '13', '23', '24', '59', 'L', '?', '1#2', '1W', '@daily',
        '1-5/-1', '1-5/+2', '1-5/ 2', '*/ 2', '*/+2', '_', '1-_', '__1',
        '1__0', '1_', '_1', '１-５', 'x-5', '5-x', 'mon,tue,wed', 'MON-FRI/2',
        '\t5', '5\n', '1\x000']


def rand_junk(rng):
    r = rng.random()
    if r < 0.5:
        return rng.choice(JUNK)
    alphabet = '0123456789*/-,_ amonfrijSU٣+.'
    return ''.join(rng.choice(alphabet) for _ in range(rng.randint(0, 9)))


class Out:
    def __init__(self, name):
        self.name = name
        self.buf = io.StringIO()
        self.n = 0

    def add(self, rec):
        self.buf.write(json.dumps(rec, ensure_ascii=True, sort_keys=True))
        self.buf.write('\n')
        self.n += 1

    def close(self):
        path = os.path.join(ARGS.out, self.name + '.jsonl.gz')
        raw = self.buf.getvalue().encode('utf-8')
        with open(path, 'wb') as fh:
            with gzip.GzipFile(filename='', mode='wb', fileobj=fh,
                               compresslevel=9, mtime=0) as gz:
                gz.write(raw)
        print('%-28s %7d cases  %8d bytes' % (self.name, self.n,
                                              os.path.getsize(path)))


def N(n):
    return max(1, int(n * ARGS.scale))


FIELDS = [('minute', 60, 0, ()), ('hour', 24, 0, ()),
          ('day_of_week', 7, 0, DOW_NAMES), ('day_of_month', 31, 1, ()),
          ('month_of_year', 12, 1, MOY_NAMES)]


# --------------------------------------------------------------------------
# 1. crontab field parsing: celery.schedules.crontab._expand_cronspec
# --------------------------------------------------------------------------
def gen_crontab_parse():
    rng = random.Random(1001)
    out = Out('crontab_parse')
    for i in range(N(6000)):
        name, max_, min_, names = rng.choice(FIELDS)
        if rng.random() < 0.45:
            spec = rand_junk(rng)
        else:
            spec = rand_field(rng, min_, max_ + min_ - 1, tuple(names) + tuple(
                DOW_NAMES if rng.random() < 0.1 else ()), star_p=0.05)
            if rng.random() < 0.1:   # push out of range now and then
                spec = spec.replace(str(max_ + min_ - 1), str(max_ + min_))
        rec = {'max': max_, 'min': min_, 'spec': spec}
        try:
            vals = schedules.crontab._expand_cronspec(spec, max_, min_)
            rec['ok'] = True
            rec['values'] = sorted(vals)
        except Exception as exc:  # noqa: BLE001 - any failure is "invalid"
            rec['ok'] = False
            rec['error'] = type(exc).__name__
        out.add(rec)
    out.close()


def rand_cron(rng):
    """Return (minute, hour, dow, dom, moy) biased towards firing specs."""
    r = rng.random()
    if r < 0.15:   # common admin patterns
        return rng.choice([
            ('*', '*', '*', '*', '*'), ('*/5', '*', '*', '*', '*'),
            ('0', '*', '*', '*', '*'), ('0', '0', '*', '*', '*'),
            ('30', '2', '*', '*', '*'), ('0', '4', '*', '*', '*'),
            ('0', '9', 'mon-fri', '*', '*'), ('0', '0', '*', '1', '*'),
            ('15', '3', '0', '*', '*'), ('0', '0', '*', '1', '1'),
            ('0', '12', '*', '31', '*'), ('0', '0', '*', '29', '2'),
            ('30', '1', '*', '*', '*'), ('30', '2', '*', '*', '3,10,11'),
            ('*/15', '1-3', '*', '*', '*'), ('59', '23', '*', '*', '*'),
            ('0', '0', '1', '1-7', '*'), ('0', '0', '5', '13', '*'),
        ])
    return (
        rand_field(rng, 0, 59, star_p=0.25),
        rand_field(rng, 0, 23, star_p=0.35),
        rand_field(rng, 0, 6, DOW_NAMES, star_p=0.6),
        rand_field(rng, 1, 31, star_p=0.6),
        rand_field(rng, 1, 12, MOY_NAMES, star_p=0.7),
    )


# --------------------------------------------------------------------------
# 2. django_celery_beat.tzcrontab.TzAwareCrontab.is_due
# --------------------------------------------------------------------------
def gen_crontab_due():
    rng = random.Random(2002)
    out = Out('crontab_due')
    app_for('UTC')
    for i in range(N(24000)):
        m, h, dow, dom, moy = rand_cron(rng)
        zone = rng.choice(ZONES)
        now = rand_now(rng, zone)
        last = rand_last(rng, now)
        rec = {'m': m, 'h': h, 'dow': dow, 'dom': dom, 'moy': moy,
               'tz': zone, 'last': iso(last), 'now': iso(now)}
        try:
            c = TzAwareCrontab(minute=m, hour=h, day_of_week=dow,
                               day_of_month=dom, month_of_year=moy,
                               tz=ZoneInfo(zone))
        except Exception as exc:  # noqa: BLE001
            rec['error'] = 'parse:' + type(exc).__name__
            out.add(rec)
            continue
        with travel(now):
            try:
                rem = c.remaining_estimate(last.astimezone(c.tz))
                due, nxt = c.is_due(last)
                rec['rem_us'] = rem // timedelta(microseconds=1)
                rec['due'] = bool(due)
                rec['next'] = nxt
            except Exception as exc:  # noqa: BLE001
                rec['error'] = type(exc).__name__
        out.add(rec)
    out.close()


# --------------------------------------------------------------------------
# 3. celery.schedules.crontab.is_due (DJANGO_CELERY_BEAT_TZ_AWARE=False):
#    evaluated in app.timezone, honours beat_cron_starting_deadline.
# --------------------------------------------------------------------------
def gen_crontab_plain():
    rng = random.Random(3003)
    out = Out('crontab_plain')
    for i in range(N(8000)):
        m, h, dow, dom, moy = rand_cron(rng)
        zone = rng.choice(ZONES)
        deadline = rng.choice([None, None, 30, 300, 3600, 86400])
        now = rand_now(rng, zone)
        last = rand_last(rng, now)
        rec = {'m': m, 'h': h, 'dow': dow, 'dom': dom, 'moy': moy,
               'tz': zone, 'deadline': deadline,
               'last': iso(last), 'now': iso(now)}
        app = app_for(zone, deadline)
        try:
            c = schedules.crontab(minute=m, hour=h, day_of_week=dow,
                                  day_of_month=dom, month_of_year=moy,
                                  app=app)
        except Exception as exc:  # noqa: BLE001
            rec['error'] = 'parse:' + type(exc).__name__
            out.add(rec)
            continue
        with travel(now):
            try:
                # ModelEntry.is_due hands over last_run_at in app.timezone
                due, nxt = c.is_due(last.astimezone(app.timezone))
                rec['due'] = bool(due)
                rec['next'] = nxt
            except Exception as exc:  # noqa: BLE001
                rec['error'] = type(exc).__name__
        out.add(rec)
    out.close()


# --------------------------------------------------------------------------
# 4. django_celery_beat.schedulers.ModelEntry (__init__ + is_due) for every
#    schedule type, including null last_run_at, start_time, expires, one_off.
# --------------------------------------------------------------------------
def db_dt(t):
    """What Django hands the scheduler for a DateTimeField value."""
    if t is None:
        return None
    if USE_TZ:
        return t
    return t.replace(tzinfo=None)   # naive, as stored in a timestamp column


def gen_entry_due():
    rng = random.Random(4004 + (0 if USE_TZ else 1))
    out = Out('entry_due' if USE_TZ else 'entry_due_naive')
    saves = []

    def fake_save(self, *a, **k):
        saves.append(1)

    PeriodicTask.save = fake_save

    for i in range(N(16000)):
        zone = rng.choice(ZONES)
        app = app_for(zone)
        now = rand_now(rng, zone)
        kind = rng.choice(['interval', 'interval', 'crontab', 'crontab',
                           'clocked'])
        sched = {}
        kw = {}
        if kind == 'interval':
            period = rng.choice(['seconds', 'seconds', 'minutes', 'hours',
                                 'days', 'microseconds'])
            every = {
                'seconds': lambda: rng.choice([1, 5, 10, 30, 45, 90, 3600,
                                               rng.randint(1, 100000)]),
                'minutes': lambda: rng.choice([1, 5, 15, 30, 90,
                                               rng.randint(1, 3000)]),
                'hours': lambda: rng.choice([1, 2, 6, 12, 24, 25,
                                             rng.randint(1, 200)]),
                'days': lambda: rng.choice([1, 2, 7, 30, rng.randint(1, 90)]),
                'microseconds': lambda: rng.choice(
                    [1, 500000, 1500000, rng.randint(1, 10 ** 8)]),
            }[period]()
            sched = {'every': every, 'period': period}
            kw['interval'] = IntervalSchedule(every=every, period=period)
        elif kind == 'crontab':
            m, h, dow, dom, moy = rand_cron(rng)
            czone = rng.choice(ZONES)
            sched = {'m': m, 'h': h, 'dow': dow, 'dom': dom, 'moy': moy,
                     'tz': czone}
            kw['crontab'] = CrontabSchedule(
                minute=m, hour=h, day_of_week=dow, day_of_month=dom,
                month_of_year=moy, timezone=ZoneInfo(czone))
        else:
            ct = now + timedelta(seconds=rng.choice([
                rng.uniform(-86400, 86400), rng.uniform(-5, 5), 0]))
            if rng.random() < 0.3:
                ct = ct.replace(microsecond=0)
            sched = {'clocked_time': iso(db_dt(ct))}
            kw['clocked'] = ClockedSchedule(clocked_time=db_dt(ct))

        changed = now - timedelta(seconds=math.exp(
            rng.uniform(math.log(0.5), math.log(90 * 86400))))
        last = None if rng.random() < 0.3 else rand_last(rng, now)
        start = None
        if rng.random() < 0.3:
            start = now + timedelta(seconds=rng.choice([
                rng.uniform(-86400 * 3, 86400 * 3), rng.uniform(-3, 3), 0]))
            if rng.random() < 0.4:
                start = start.replace(microsecond=0)
        expires = None
        if rng.random() < 0.2:
            expires = now + timedelta(seconds=rng.choice([
                rng.uniform(-86400, 86400 * 5), rng.uniform(-2, 2), 0]))
        one_off = (kind == 'clocked' and rng.random() < 0.9) or rng.random() < 0.15
        enabled = rng.random() < 0.93
        count = rng.choice([0, 0, 0, 1, 7])

        model = PeriodicTask(
            name='t%d' % i, task='x.y', enabled=enabled, one_off=one_off,
            start_time=db_dt(start), expires=db_dt(expires),
            last_run_at=db_dt(last), total_run_count=count, **kw)
        model.date_changed = db_dt(changed)

        rec = {'app_tz': zone, 'now': iso(now), 'kind': kind, 'sched': sched,
               'enabled': enabled, 'one_off': one_off,
               'start_time': iso(db_dt(start)), 'expires': iso(db_dt(expires)),
               'last_run_at': iso(db_dt(last)),
               'date_changed': iso(db_dt(changed)),
               'total_run_count': count}
        del saves[:]
        with travel(now):
            try:
                entry = ModelEntry(model, app=app)
            except Exception as exc:  # noqa: BLE001
                rec['error'] = 'init:' + type(exc).__name__
                out.add(rec)
                continue
            rec['eff_last'] = iso(entry.last_run_at)
            try:
                due, nxt = entry.is_due()
                rec['due'] = bool(due)
                rec['next'] = nxt
            except Exception as exc:  # noqa: BLE001
                rec['error'] = type(exc).__name__
        rec['saved'] = bool(saves)
        rec['after_enabled'] = bool(model.enabled)
        rec['after_count'] = model.total_run_count
        rec['after_no_changes'] = bool(model.no_changes)
        out.add(rec)
    out.close()


def write_versions():
    path = os.path.join(ARGS.out, 'versions.json')
    with open(path, 'w') as fh:
        json.dump({'celery': celery.__version__, 'kombu': kombu.__version__,
                   'django_celery_beat': django_celery_beat.__version__,
                   'tz_window': [iso(T_MIN), iso(T_MAX)]},
                  fh, indent=1, sort_keys=True)
        fh.write('\n')


def main():
    os.makedirs(ARGS.out, exist_ok=True)
    gens = [gen_entry_due]
    if USE_TZ:
        gens = [gen_crontab_parse, gen_crontab_due, gen_crontab_plain,
                gen_entry_due]
        try:
            import gen_messages
            gens += gen_messages.generators(sys.modules[__name__])
        except ImportError:
            pass
    for g in gens:
        if ARGS.only and ARGS.only not in g.__name__:
            continue
        g()
    if USE_TZ and not ARGS.only:
        write_versions()


if __name__ == '__main__':
    sys.modules.setdefault('gen_golden', sys.modules[__name__])
    main()
