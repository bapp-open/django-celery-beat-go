"""cron.py jobs: discovered like models, stored in django_gobeat's own
table, scheduled by gobeat - with or without django-celery-beat."""
import json
import time
from datetime import datetime

from conftest import (assert_exactly_once, ctl, execute, healthz, job_row,
                      manage, psql, runs, unique, wait_for)

EVERY_2S = 'runlog.cron.EveryTwoSeconds'


def test_cron_py_job_is_discovered_synced_and_executed():
    row = job_row(EVERY_2S)
    assert row is not None, 'gobeat_sync did not write the cron.py job'
    assert (row['source'], row['kind'], row['every_microseconds']) == ('code', 'interval', 2000000)
    assert row['task'] == EVERY_2S          # run() became a Celery task of that name
    assert row['queue'] == 'high'           # inherited from the project's base class

    ctl({'op': 'job_update', 'name': EVERY_2S, 'paused': False})
    got = wait_for(lambda: len(runs(EVERY_2S)) >= 3 and runs(EVERY_2S), timeout=30, what='cron.py runs')
    for r in got:
        assert r['task'] == EVERY_2S
        assert json.loads(r['args']) == ['from-cron-py']
        assert (r['queue'], r['priority']) == ('high', 4)
        delta = datetime.fromisoformat(r['expires']).timestamp() - r['ts']
        assert 40 < delta <= 45.5, delta
    gaps = [b['ts'] - a['ts'] for a, b in zip(got, got[1:])]
    assert all(1.0 < g < 3.5 for g in gaps), gaps
    row = job_row(EVERY_2S)
    assert row['last_run_at'] is not None and row['total_run_count'] >= 3

    # the admin's pause switch
    ctl({'op': 'job_update', 'name': EVERY_2S, 'paused': True})
    time.sleep(3)
    n = len(runs(EVERY_2S))
    time.sleep(5)
    assert len(runs(EVERY_2S)) == n, 'paused job kept running'
    j, _ = assert_exactly_once(EVERY_2S, settle=0)
    assert all(x['task_id'][14] == '5' for x in j)


def test_cron_class_can_schedule_an_existing_task_and_use_a_crontab():
    ctl({'op': 'job_update', 'name': 'existing-task-job', 'paused': False},
        {'op': 'job_update', 'name': 'runlog.cron.EveryMinuteTokyo', 'paused': False})
    got = wait_for(lambda: runs('existing-task-job'), timeout=30, what='existing task run')
    assert got[0]['task'] == 'runlog.tasks.record'
    assert json.loads(got[0]['kwargs']) == {'via': 'cron.py'}

    cron = job_row('runlog.cron.EveryMinuteTokyo')
    assert (cron['kind'], cron['minute'], cron['hour'], cron['timezone']) == ('crontab', '*', '*', 'Asia/Tokyo')
    got = wait_for(lambda: runs('runlog.cron.EveryMinuteTokyo'), timeout=75, what='crontab cron.py run')
    assert datetime.fromtimestamp(got[0]['ts']).second < 4
    assert json.loads(got[0]['kwargs']) == {'zone': 'Asia/Tokyo'}   # get_kwargs() hook


def test_admin_created_job_and_sync_is_idempotent():
    name = unique('admin-job')
    ctl({'op': 'job_create', 'name': name, 'task': 'runlog.tasks.record', 'kind': 'interval',
         'every_microseconds': 1500000, 'args': '["admin"]', 'queue': 'celery'})
    wait_for(lambda: len(runs(name)) >= 2, timeout=30, what='admin job runs')
    assert json.loads(runs(name)[0]['args']) == ['admin']

    before = psql('select name, date_changed::text, total_run_count from django_gobeat_job '
                  "where source = 'code' order by name")
    out = manage('gobeat_sync').stdout
    assert 'cron jobs: 3 discovered, 0 created, 0 updated, 0 removed' in out, out
    after = psql('select name, date_changed::text, total_run_count from django_gobeat_job '
                 "where source = 'code' order by name")
    assert before == after, 're-running gobeat_sync must not touch unchanged jobs'
    assert job_row(name) is not None, 'gobeat_sync must leave admin-created jobs alone'

    ctl({'op': 'job_delete', 'name': name})
    time.sleep(3)
    n = len(runs(name))
    time.sleep(4)
    assert len(runs(name)) == n, 'deleted job kept running'
    assert healthz()[0] == 200
