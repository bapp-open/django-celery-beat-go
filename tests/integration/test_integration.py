"""Tasks scheduled by gobeat are executed by real Celery workers."""
import json
import time
from datetime import datetime

import pytest

from conftest import (BROKER, assert_exactly_once, compose, ctl, execute,
                      healthz, inspect, log_events, manage, metrics, psql,
                      runs, task_row, unique, wait_for)


def parse_iso(s):
    return datetime.fromisoformat(s)


def test_interval_task_args_queue_priority_expiry():
    name = unique('interval')
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [2, 'seconds'], 'args': [1, 'two', {'k': None}],
         'kwargs': {'a': 1.5, 'ünï': 'cødé'}, 'queue': 'high', 'priority': 5,
         'expire_seconds': 30, 'headers': {'x-custom': 'hv'}})
    got = wait_for(lambda: len(runs(name)) >= 3 and runs(name), timeout=30,
                   what='3 runs')
    for r in got:
        assert r['task'] == 'runlog.tasks.record'
        assert json.loads(r['args']) == [1, 'two', {'k': None}]
        assert json.loads(r['kwargs']) == {'a': 1.5, 'ünï': 'cødé'}
        assert r['queue'] == 'high'
        assert r['exchange'] == ''
        assert r['priority'] == 5
        assert json.loads(r['headers']) == {'x-custom': 'hv'}
        # expires header is ~30 s after the dispatch
        delta = parse_iso(r['expires']).timestamp() - r['ts']
        assert 25 < delta <= 30.5, delta
    gaps = [b['ts'] - a['ts'] for a, b in zip(got, got[1:])]
    assert all(1.0 < g < 3.5 for g in gaps), gaps
    j, r = assert_exactly_once(name)
    row = task_row(name)
    assert row['last_run_at'] is not None
    assert row['total_run_count'] == len(psql(
        "select 1 from django_gobeat_dispatch where name = '%s'" % name))
    # deterministic ids: uuid5 of (periodic task id, scheduled time)
    assert all(x['task_id'][14] == '5' for x in j)


def test_decorator_options_and_routes_resolved_by_celery():
    names = {t: unique(t) for t in ('decorated', 'routed', 'topic')}
    ctl(*[{'op': 'create', 'name': n, 'task': 'runlog.tasks.' + t,
           'interval': [2, 'seconds']} for t, n in names.items()])
    for n in names.values():
        wait_for(lambda n=n: runs(n), timeout=30, what='run of ' + n)
    d = runs(names['decorated'])[0]
    assert (d['queue'], d['priority']) == ('high', 7)
    delta = parse_iso(d['expires']).timestamp() - d['ts']
    assert 110 < delta <= 120.5, delta
    r = runs(names['routed'])[0]
    assert r['queue'] == 'high'
    t = runs(names['topic'])[0]
    assert (t['exchange'], t['queue']) == ('events', 'ev.created')
    assert t['expires'] is None


def test_crontab_fires_on_the_minute():
    name = unique('cron')
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'crontab': {'minute': '*', 'hour': '*', 'day_of_week': '*',
                     'day_of_month': '*', 'month_of_year': '*',
                     'timezone': 'America/New_York'}})
    got = wait_for(lambda: runs(name), timeout=75, what='crontab run')
    second = datetime.fromtimestamp(got[0]['ts']).second
    assert second < 4, second
    time.sleep(3)
    assert len(runs(name)) == 1


def test_admin_edits_take_effect():
    name = unique('edit')
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [2, 'seconds'], 'args': ['v1']})
    wait_for(lambda: len(runs(name)) >= 2, timeout=30, what='v1 runs')

    ctl({'op': 'update', 'name': name, 'args': ['v2'], 'queue': 'high'})
    edited = time.time()
    wait_for(lambda: any(json.loads(r['args']) == ['v2'] for r in runs(name)),
             timeout=15, what='v2 run')
    late_v1 = [r for r in runs(name)
               if json.loads(r['args']) == ['v1'] and r['ts'] > edited + 1.5]
    assert not late_v1, 'old arguments still used after the edit'
    assert all(r['queue'] == 'high' for r in runs(name)
               if json.loads(r['args']) == ['v2'])

    ctl({'op': 'update', 'name': name, 'enabled': False})
    time.sleep(3)
    n = len(runs(name))
    time.sleep(6)
    assert len(runs(name)) == n, 'disabled task kept running'

    ctl({'op': 'update', 'name': name, 'enabled': True})
    wait_for(lambda: len(runs(name)) > n, timeout=15, what='run after enable')

    ctl({'op': 'delete', 'name': name})
    time.sleep(3)
    n = len(runs(name))
    time.sleep(6)
    assert len(runs(name)) == n, 'deleted task kept running'


def test_one_off_runs_exactly_once():
    name = unique('oneoff')
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [2, 'seconds'], 'one_off': True})
    wait_for(lambda: runs(name), timeout=20, what='one-off run')
    time.sleep(9)
    assert len(runs(name)) == 1
    row = task_row(name)
    # ModelEntry.is_due "ONE OFF TASK": disabled and count reset
    assert row['enabled'] is False and row['total_run_count'] == 0
    assert_exactly_once(name, settle=0)


def test_clocked_runs_exactly_once_at_its_time():
    name = unique('clocked')
    created = time.time()
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'clocked_in': 6, 'one_off': True, 'args': ['at-the-time']})
    time.sleep(3)
    assert runs(name) == [], 'clocked task ran early'
    got = wait_for(lambda: runs(name), timeout=20, what='clocked run')
    assert 4.0 < got[0]['ts'] - created < 9.5, got[0]['ts'] - created
    time.sleep(8)
    assert len(runs(name)) == 1
    assert task_row(name)['enabled'] is False


def test_start_time_and_expires():
    later, expiring = unique('start'), unique('expires')
    created = time.time()
    ctl({'op': 'create', 'name': later, 'task': 'runlog.tasks.record',
         'interval': [1, 'seconds'], 'start_in': 6},
        {'op': 'create', 'name': expiring, 'task': 'runlog.tasks.record',
         'interval': [1, 'seconds'], 'expires_in': 6})
    wait_for(lambda: len(runs(later)) >= 2, timeout=25, what='runs after start_time')
    assert min(r['ts'] for r in runs(later)) - created > 5.0
    wait_for(lambda: task_row(expiring)['enabled'] is False, timeout=20,
             what='expired task disabled')
    got = runs(expiring)
    assert got, 'task never ran before it expired'
    assert max(r['ts'] for r in got) - created < 8.5
    n = len(got)
    time.sleep(4)
    assert len(runs(expiring)) == n


def test_unsupported_tasks_are_named_and_skipped():
    solar, pickled, badcron = unique('solar'), unique('pickle'), unique('badcron')
    ok = unique('fine')
    ctl({'op': 'create', 'name': solar, 'task': 'runlog.tasks.record',
         'solar': {'event': 'sunrise', 'latitude': 44.4, 'longitude': 26.1}},
        {'op': 'create', 'name': pickled, 'task': 'runlog.tasks.pickled',
         'interval': [1, 'seconds']},
        {'op': 'create', 'name': badcron, 'task': 'runlog.tasks.record',
         'crontab': {'minute': '61', 'hour': '*', 'day_of_week': '*',
                     'day_of_month': '*', 'month_of_year': '*'}},
        {'op': 'create', 'name': ok, 'task': 'runlog.tasks.record',
         'interval': [1, 'seconds']})
    wait_for(lambda: len(runs(ok)) >= 3, timeout=20, what='healthy task runs')
    for n in (solar, pickled, badcron):
        assert runs(n) == []
        refused = [e for e in log_events() if e.get('name') == n and
                   e.get('level') == 'ERROR' and 'refused' in e.get('msg', '')]
        assert refused, 'no error naming %s in the gobeat log' % n
    errs = {e['name']: e['error'] for e in log_events()
            if e.get('level') == 'ERROR' and 'error' in e and 'name' in e}
    assert 'solar' in errs[solar]
    assert 'pickle' in errs[pickled]
    assert 'crontab' in errs[badcron]
    assert healthz()[0] == 200


def test_code_defined_beat_schedule_is_synced_and_runs():
    out = manage('gobeat_sync').stdout
    assert 'code-defined entries synced' in out
    got = wait_for(lambda: runs('code-defined-every-3s'), timeout=20,
                   what='code-defined task run')
    assert json.loads(got[0]['args']) == ['from-code']
    assert got[0]['queue'] == 'high'
    assert task_row('code-defined-crontab') is not None
    assert task_row('celery.backend_cleanup') is not None


def test_healthz_metrics_and_memory():
    name = unique('metrics')
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [1, 'seconds']})
    wait_for(lambda: len(runs(name)) >= 2, timeout=20, what='runs')
    status, body = healthz()
    assert status == 200 and body['ok'] and body['state'] == 'leader'
    m = metrics()
    assert m['gobeat_dispatch_total{result="published"}'] >= 2
    assert m['gobeat_leader'] == 1
    assert m['gobeat_dispatch_lateness_seconds_count'] >= 1
    assert any(k.startswith('gobeat_reloads_total') for k in m)
    pid = inspect('gobeat', '{{.State.Pid}}')
    with open('/proc/%s/status' % pid) as fh:
        rss_kb = int([l for l in fh if l.startswith('VmHWM')][0].split()[1])
    assert rss_kb < 20 * 1024, 'peak RSS %d kB is over the 20 MB target' % rss_kb


def test_refuses_to_start_without_settings_row_or_on_unknown_schema():
    p = compose('run', '--rm', '--no-deps', '-e', 'GOBEAT_SETTINGS_KEY=absent',
                '-e', 'GOBEAT_HTTP_ADDR=off', 'gobeat', check=False)
    assert p.returncode == 1
    assert 'settings row not found' in p.stderr
    assert 'gobeat_sync' in p.stderr

    execute("insert into django_migrations (app, name, applied) values "
            "('django_celery_beat', '0099_from_the_future', now())")
    try:
        p = compose('run', '--rm', '--no-deps', '-e', 'GOBEAT_HTTP_ADDR=off',
                    '-e', 'GOBEAT_DRY_RUN=1', 'gobeat', check=False)
        assert p.returncode == 1
        assert 'unknown database schema' in p.stderr
        assert '0099_from_the_future' in p.stderr
    finally:
        execute("delete from django_migrations where name = '0099_from_the_future'")

    p = compose('run', '--rm', '--no-deps', '-e', 'GOBEAT_HTTP_ADDR=off',
                '-e', 'GOBEAT_DRY_RUN=1',
                '-e', 'GOBEAT_SETTINGS_OVERRIDE={"versions":{"celery":"4.4.7","django_celery_beat":"2.8.1"}}',
                'gobeat', check=False)
    assert p.returncode == 1
    assert 'celery 4.4.7 is not a supported version' in p.stderr


def test_settings_change_is_picked_up():
    name = unique('settings')
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [1, 'seconds']})
    wait_for(lambda: runs(name), timeout=20, what='run')
    execute("update django_gobeat_settings set version = 'changed-by-test'")
    wait_for(lambda: any(e.get('msg') == 'settings changed, reloading'
                         for e in log_events()), timeout=15,
             what='settings reload')
    n = len(runs(name))
    wait_for(lambda: len(runs(name)) > n + 1, timeout=15,
             what='runs after settings reload')
    assert 'written' in manage('gobeat_sync', '--no-beat-schedule').stdout
    assert_exactly_once(name)
