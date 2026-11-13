"""Shared helpers for the docker-compose integration tests.

Run through `make integration` (which builds the images); the tests only
need Python's standard library, pytest and the docker CLI.
"""
import json
import os
import subprocess
import time
import urllib.request
import uuid

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
BROKER = os.environ.get('BROKER', 'redis')
BROKER_URLS = {
    'redis': 'redis://redis:6379/0',
    'amqp': 'amqp://guest:guest@rabbitmq:5672//',
}
ENV = dict(os.environ, BROKER_URL=BROKER_URLS[BROKER], COMPOSE_BAKE='false')
GOBEAT_PORT = os.environ.get('GOBEAT_PORT', '19099')
BROKER_SERVICE = 'redis' if BROKER == 'redis' else 'rabbitmq'


def sh(*cmd, check=True, env=None, timeout=180, input=None):
    p = subprocess.run(cmd, cwd=HERE, env=env or ENV, capture_output=True,
                       text=True, timeout=timeout, input=input)
    if check and p.returncode != 0:
        raise AssertionError('%s failed (%d):\n%s\n%s' % (
            ' '.join(cmd), p.returncode, p.stdout[-3000:], p.stderr[-3000:]))
    return p


def compose(*args, **kw):
    return sh('docker', 'compose', *args, **kw)


def psql(sql):
    """Run a query, return rows as dicts."""
    wrapped = 'select coalesce(json_agg(t), \'[]\') from (%s) t' % sql
    out = compose('exec', '-T', 'postgres', 'psql', '-U', 'app', '-d', 'app',
                  '-At', '-v', 'ON_ERROR_STOP=1', '-c', wrapped).stdout
    return json.loads(out)


def execute(sql):
    compose('exec', '-T', 'postgres', 'psql', '-U', 'app', '-d', 'app',
            '-v', 'ON_ERROR_STOP=1', '-c', sql)


def ctl(*ops):
    """Apply ORM operations the way the Django admin would."""
    out = compose('exec', '-T', 'ctl', 'python', 'manage.py', 'ctl',
                  json.dumps(list(ops))).stdout
    return json.loads(out.strip().splitlines()[-1])


def manage(*args):
    return compose('exec', '-T', 'ctl', 'python', 'manage.py', *args)


def runs(name):
    return psql("select *, extract(epoch from executed_at) as ts from "
                "runlog_run where periodic_task_name = '%s' order by id" % name)


def journal(name):
    return psql("select id, task_id, published_at, abandoned_at, attempts, "
                "extract(epoch from claimed_at) as claimed_ts, "
                "extract(epoch from scheduled_for) as scheduled_ts "
                "from django_gobeat_dispatch where name = '%s' order by id"
                % name)


def job_row(name):
    rows = psql("select * from django_gobeat_job where name = '%s'" % name)
    return rows[0] if rows else None


def task_row(name):
    rows = psql("select * from django_celery_beat_periodictask "
                "where name = '%s'" % name)
    return rows[0] if rows else None


def wait_for(fn, timeout=30, interval=0.5, what='condition'):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError('timed out after %ss waiting for %s (last: %r)' % (
        timeout, what, last))


def logs(service='gobeat', since=None):
    args = ['logs', '--no-log-prefix', service]
    if since:
        args[1:1] = ['--since', since]
    return compose(*args).stdout


def log_events(service='gobeat'):
    out = []
    for line in logs(service).splitlines():
        if line.startswith('{'):
            try:
                out.append(json.loads(line))
            except ValueError:
                pass
    return out


def container(service):
    return compose('ps', '-a', '-q', service).stdout.strip().splitlines()[0]


def inspect(service, fmt):
    return sh('docker', 'inspect', '-f', fmt, container(service)).stdout.strip()


def healthz():
    try:
        with urllib.request.urlopen(
                'http://127.0.0.1:%s/healthz' % GOBEAT_PORT, timeout=3) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())
    except OSError:
        return None, None


def metrics():
    with urllib.request.urlopen(
            'http://127.0.0.1:%s/metrics' % GOBEAT_PORT, timeout=3) as r:
        text = r.read().decode()
    out = {}
    for line in text.splitlines():
        if line and not line.startswith('#'):
            k, v = line.rsplit(' ', 1)
            out[k] = float(v)
    return out


def unique(prefix):
    return '%s-%s' % (prefix, uuid.uuid4().hex[:8])


def assert_exactly_once(name, settle=4):
    """Every journal row was published and executed exactly once."""
    def done():
        j = journal(name)
        return j and all(r['published_at'] for r in j)
    wait_for(done, timeout=60, what='journal of %s fully published' % name)
    time.sleep(settle)
    j = journal(name)
    r = runs(name)
    ids = [x['task_id'] for x in r]
    assert len(ids) == len(set(ids)), 'duplicate executions: %r' % sorted(
        i for i in set(ids) if ids.count(i) > 1)
    assert set(ids) == {x['task_id'] for x in j}, (
        'journal and executions differ: only journal %r, only runs %r' % (
            sorted({x['task_id'] for x in j} - set(ids)),
            sorted(set(ids) - {x['task_id'] for x in j})))
    return j, r


@pytest.fixture(scope='session', autouse=True)
def stack():
    compose('--profile', 'replica', '--profile', 'shadow', 'down', '-v',
            '--remove-orphans', check=False)
    compose('up', '-d', '--no-build', 'postgres', 'redis', 'rabbitmq', 'ctl',
            'worker', 'gobeat', timeout=600)
    wait_for(lambda: healthz()[0] == 200, timeout=60, what='gobeat healthy')
    yield
    if os.environ.get('KEEP_STACK') != '1':
        art = os.path.join(HERE, '.artifacts')
        os.makedirs(art, exist_ok=True)
        for svc in ('gobeat', 'worker', 'ctl'):
            with open(os.path.join(art, '%s-%s.log' % (BROKER, svc)), 'w') as fh:
                fh.write(compose('logs', svc, check=False).stdout)
        compose('--profile', 'replica', '--profile', 'shadow', 'down', '-v',
                '--remove-orphans', check=False)


@pytest.fixture(autouse=True)
def clean():
    """Start every test from an empty schedule with gobeat as sole leader."""
    ctl({'op': 'reset'})
    # a message dispatched just before the reset may still be executing
    time.sleep(1.5)
    ctl({'op': 'reset'})
    yield
    ctl({'op': 'reset'})
