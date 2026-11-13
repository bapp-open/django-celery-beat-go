"""The same stack without django-celery-beat installed at all
(WITH_DCB=0 DCB_VERSION=none): gobeat's own job table is the schedule.

Run in its own pass by `make integration`; it is not part of the default
test list because it needs a differently built stack."""
import json

from conftest import (assert_exactly_once, ctl, healthz, job_row, log_events,
                      manage, psql, runs, wait_for)

def test_no_django_celery_beat_anywhere():
    tables = [r['tablename'] for r in psql(
        "select tablename from pg_tables where tablename like 'django_celery_beat%'")]
    assert tables == []
    assert 'No module named' in manage('shell', '-c', (
        'import importlib\n'
        'try:\n    importlib.import_module("django_celery_beat")\n'
        'except ImportError as e:\n    print(e)')).stdout
    versions = psql("select data->'versions' as v from django_gobeat_settings")[0]['v']
    assert versions['django_celery_beat'] is None and versions['celery']
    assert healthz()[0] == 200


def test_cron_py_and_beat_schedule_run_without_django_celery_beat():
    # app.conf.beat_schedule entries became gobeat jobs
    row = job_row('code-defined-every-3s')
    assert row and (row['kind'], row['every_microseconds'], row['queue']) == ('interval', 3000000, 'high')
    assert job_row('code-defined-crontab')['kind'] == 'crontab'

    ctl({'op': 'job_update', 'name': 'runlog.cron.EveryTwoSeconds', 'paused': False},
        {'op': 'job_update', 'name': 'code-defined-every-3s', 'paused': False})
    wait_for(lambda: len(runs('runlog.cron.EveryTwoSeconds')) >= 3, timeout=30, what='cron.py runs')
    got = wait_for(lambda: runs('code-defined-every-3s'), timeout=30, what='beat_schedule job run')
    assert json.loads(got[0]['args']) == ['from-code'] and got[0]['queue'] == 'high'
    ctl({'op': 'job_update', 'name': 'runlog.cron.EveryTwoSeconds', 'paused': True},
        {'op': 'job_update', 'name': 'code-defined-every-3s', 'paused': True})
    assert_exactly_once('runlog.cron.EveryTwoSeconds')
    assert not [e for e in log_events() if e.get('level') == 'ERROR']
