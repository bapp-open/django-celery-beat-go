"""django_gobeat.cron: discovery, the CronInterface contract and sync."""
import json
from datetime import timedelta

import pytest
from celery import current_app
from celery.schedules import crontab

from django_gobeat import CronError, CronInterface, cron
from django_gobeat.models import Job


def test_cron_modules_are_discovered_like_admin_modules():
    # AppConfig.ready imported sampleapp/cron.py; abstract bases are skipped
    assert set(cron.registry) >= {'sampleapp.cron.Nightly', 'ping'}
    assert 'sampleapp.cron.TenantCron' not in cron.registry


def test_run_becomes_a_celery_task_named_after_the_job():
    from sampleapp import cron as sample
    task = current_app.tasks['sampleapp.cron.Nightly']
    assert task(tenant='acme') == 'ran'
    assert sample.CALLS[-1] == {'tenant': 'acme'}
    # a job that points at an existing task registers nothing
    assert 'ping' not in current_app.tasks


def test_fields_written_to_the_database():
    from sampleapp.cron import Nightly, Ping
    f = Nightly().as_fields()
    assert (f['kind'], f['minute'], f['hour'], f['day_of_week'], f['day_of_month'], f['month_of_year']) == \
        ('crontab', '0', '4', 'mon-fri', '*', '*')
    assert f['timezone'] == 'Europe/Bucharest' and f['queue'] == 'tenants'      # inherited
    assert json.loads(f['kwargs']) == {'tenant': 'acme'}                         # hook
    assert f['task'] == 'sampleapp.cron.Nightly'
    p = Ping().as_fields()
    assert (p['kind'], p['every_microseconds'], p['task'], p['expire_seconds'], p['priority']) == \
        ('interval', 30_000_000, 'sampleapp.tasks.ping', 20, 3)


@pytest.mark.parametrize('schedule', [
    object(),               # anything that is not a crontab or an interval (e.g. solar)
    None, 'daily', 0, -5, True, timedelta(0),
])
def test_unsupported_schedules_are_refused(schedule):
    with pytest.raises(CronError):
        cron.schedule_fields(schedule, 'x')


def test_definition_errors():
    with pytest.raises(CronError, match='two cron jobs are named'):
        class Ping2(CronInterface):
            name = 'ping'
            schedule = 5
            task = 't'
    class NoBody(CronInterface):
        name = 'no-body'
        schedule = 5
    try:
        with pytest.raises(CronError, match='define run\\(\\) or set task'):
            NoBody().as_fields()
        class BadArgs(CronInterface):
            name = 'bad-args'
            schedule = 5
            task = 't'
            kwargs = {'when': object()}
        with pytest.raises(CronError, match='not JSON serialisable'):
            BadArgs().as_fields()
    finally:
        cron.registry.pop('no-body', None)
        cron.registry.pop('bad-args', None)


def test_sync_is_idempotent_and_keeps_bookkeeping(db_ready):
    created, updated, deleted, errors = cron.sync_jobs()
    assert (created, updated, deleted, errors) == (2, 0, 0, [])
    job = Job.objects.get(name='ping')
    assert job.source == 'code'
    stamp = job.date_changed
    Job.objects.filter(pk=job.pk).update(total_run_count=7, paused=True)
    admin = Job.objects.create(name='by-hand', task='t', kind='interval', every_microseconds=10 ** 6)

    assert cron.sync_jobs() == (0, 0, 0, [])
    job.refresh_from_db()
    assert (job.total_run_count, job.paused, job.date_changed) == (7, True, stamp)

    # a changed definition updates the row; a removed class deletes it
    from sampleapp.cron import Ping
    Ping.priority = 9
    removed = cron.registry.pop('sampleapp.cron.Nightly')
    try:
        assert cron.sync_jobs() == (0, 1, 1, [])
        job.refresh_from_db()
        assert job.priority == 9 and job.total_run_count == 7 and job.date_changed > stamp
        assert not Job.objects.filter(name='sampleapp.cron.Nightly').exists()
        assert Job.objects.filter(pk=admin.pk).exists()      # admin rows are not ours to delete
    finally:
        Ping.priority = 3
        cron.registry['sampleapp.cron.Nightly'] = removed


def test_beat_schedule_entries_can_be_synced_as_jobs(db_ready):
    extra = {'from-settings': dict(cron.schedule_fields(crontab(minute='*/5'), 'from-settings'),
                                   task='t', args='[]', kwargs='{}', headers='{}')}
    created, _, _, errors = cron.sync_jobs(extra)
    assert errors == [] and Job.objects.get(name='from-settings').minute == '*/5'


def test_migrate_alone_syncs_settings_and_jobs(db_ready, capsys):
    """post_migrate hook: no separate gobeat_sync step is needed."""
    from django.core.management import call_command
    from django_gobeat.models import GobeatSettings

    GobeatSettings.objects.all().delete()
    call_command('migrate', verbosity=1)
    out = capsys.readouterr().out
    assert 'cron jobs: 2 discovered, 2 created' in out and 'gobeat settings written' in out
    row = GobeatSettings.objects.get(key='default')
    assert row.data['schema'] == 1 and 'sampleapp.cron.Nightly' in row.data['tasks']
    assert row.data['versions']['django_celery_beat'] is None      # not installed here
    assert Job.objects.filter(source='code').count() == 2

    # running it again changes nothing
    call_command('migrate', verbosity=1)
    assert 'gobeat settings unchanged' in capsys.readouterr().out


def test_auto_sync_can_be_disabled_and_never_breaks_migrate(db_ready, settings_override, capsys):
    from django.core.management import call_command
    from django_gobeat.models import GobeatSettings

    GobeatSettings.objects.all().delete()
    settings_override(GOBEAT_AUTO_SYNC=False)
    call_command('migrate', verbosity=0)
    assert not GobeatSettings.objects.exists() and not Job.objects.exists()

    settings_override(GOBEAT_AUTO_SYNC=True, GOBEAT_CELERY_APP='does.not:exist')
    call_command('migrate', verbosity=0)             # must not raise
    assert 'automatic sync after migrate failed' in capsys.readouterr().err
