"""Code-defined periodic jobs, discovered from each app's ``cron.py``.

Django finds models in ``models.py`` and admin classes in ``admin.py``;
django_gobeat finds periodic jobs in ``cron.py`` the same way. Put a
subclass of :class:`CronInterface` there::

    # myapp/cron.py
    from datetime import timedelta
    from django_gobeat import CronInterface, crontab

    class NightlyCleanup(CronInterface):
        schedule = crontab(minute=0, hour=4)        # or a timedelta / seconds
        timezone = 'Europe/Bucharest'               # crontab zone (default: CELERY_TIMEZONE)
        queue = 'maintenance'

        def run(self):
            ...                                     # executed by a Celery worker

    class Heartbeat(CronInterface):
        schedule = timedelta(seconds=30)
        task = 'myapp.tasks.ping'                   # schedule an existing task instead

``manage.py gobeat_sync`` writes the discovered jobs to the database, where
gobeat picks them up. No django-celery-beat is needed for this.

Project-specific behaviour goes into an abstract base of your own::

    class TenantCron(CronInterface):
        abstract = True
        queue = 'tenants'

        def get_kwargs(self):
            return {'tenant': self.tenant}
"""
import json
from datetime import timedelta

from celery import schedules

#: name -> CronInterface subclass, filled as ``cron.py`` modules are imported
registry = {}

crontab = schedules.crontab


class CronError(Exception):
    """A job definition gobeat cannot schedule."""


class CronInterface:
    """Base class of a code-defined periodic job."""

    #: Set on intermediate base classes that are not jobs themselves.
    abstract = True

    #: Unique job name; defaults to "<module>.<ClassName>".
    name = None
    #: ``celery.schedules.crontab``, ``timedelta`` or seconds.
    schedule = None
    #: IANA zone the crontab is evaluated in (default: the Celery timezone).
    timezone = None
    #: Name of an existing Celery task to send. When unset, a task that
    #: calls :meth:`run` is registered under the job's name.
    task = None
    args = ()
    kwargs = None
    headers = None
    queue = None
    exchange = None
    routing_key = None
    priority = None
    #: Seconds after which an undelivered run is dropped by the worker.
    expires = None
    enabled = True
    #: Run a single time only.
    one_off = False

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # `abstract` is not inherited: a subclass is a job unless it says so
        cls.abstract = cls.__dict__.get('abstract', False)
        if cls.abstract:
            return
        name = cls.get_name()
        other = registry.get(name)
        if other is not None and (other.__module__, other.__qualname__) != (
                cls.__module__, cls.__qualname__):
            raise CronError(
                f'two cron jobs are named {name!r}: {other.__module__}.'
                f'{other.__qualname__} and {cls.__module__}.{cls.__qualname__}')
        registry[name] = cls

    # -- hooks subclasses may override ------------------------------------

    @classmethod
    def get_name(cls):
        return cls.name or f'{cls.__module__}.{cls.__qualname__}'

    @classmethod
    def get_task_name(cls):
        """Celery task that a worker executes for this job."""
        return cls.task or cls.get_name()

    def get_schedule(self):
        return self.schedule

    def get_args(self):
        return list(self.args or ())

    def get_kwargs(self):
        return dict(self.kwargs or {})

    def get_headers(self):
        return dict(self.headers or {})

    def is_enabled(self):
        return bool(self.enabled)

    def run(self, *args, **kwargs):
        """The job body; executed by a Celery worker."""
        raise NotImplementedError(
            f'{type(self).__name__} defines neither run() nor task')

    # -- conversion --------------------------------------------------------

    @classmethod
    def has_run(cls):
        return cls.run is not CronInterface.run

    def as_fields(self):
        """Database representation (``django_gobeat.models.Job`` fields)."""
        cls = type(self)
        name = cls.get_name()
        if not cls.task and not cls.has_run():
            raise CronError(f'{name}: define run() or set task')
        fields = schedule_fields(self.get_schedule(), name)
        if self.timezone:
            fields['timezone'] = str(self.timezone)
        fields.update(
            task=cls.get_task_name(),
            args=_json(self.get_args(), list, name, 'args'),
            kwargs=_json(self.get_kwargs(), dict, name, 'kwargs'),
            headers=_json(self.get_headers(), dict, name, 'headers'),
            queue=self.queue or None, exchange=self.exchange or None,
            routing_key=self.routing_key or None, priority=self.priority,
            expire_seconds=_seconds(self.expires, name),
            enabled=self.is_enabled(), one_off=bool(self.one_off),
        )
        return fields


def _json(value, kind, name, what):
    if not isinstance(value, kind):
        raise CronError(f'{name}: {what} must be a {kind.__name__}')
    try:
        return json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise CronError(f'{name}: {what} is not JSON serialisable: {exc}')


def _seconds(value, name):
    if value is None:
        return None
    if isinstance(value, timedelta):
        value = value.total_seconds()
    if isinstance(value, bool) or not isinstance(value, (int, float)) \
            or value != int(value) or value <= 0:
        raise CronError(f'{name}: expires must be whole positive seconds')
    return int(value)


def schedule_fields(schedule, name):
    """Turn a schedule into Job columns; raises CronError for anything
    gobeat does not run (solar schedules, relative intervals, ...)."""
    empty = dict(minute='', hour='', day_of_week='', day_of_month='',
                 month_of_year='', timezone='', every_microseconds=None)
    if isinstance(schedule, schedules.crontab):
        return dict(
            empty, kind='crontab',
            minute=_cronfield(schedule._orig_minute),
            hour=_cronfield(schedule._orig_hour),
            day_of_week=_cronfield(schedule._orig_day_of_week),
            day_of_month=_cronfield(schedule._orig_day_of_month),
            month_of_year=_cronfield(schedule._orig_month_of_year))
    if isinstance(schedule, schedules.schedule):
        if schedule.relative:
            raise CronError(f'{name}: relative intervals are not supported')
        schedule = schedule.run_every
    if isinstance(schedule, bool):
        raise CronError(f'{name}: schedule must be a crontab or an interval')
    if isinstance(schedule, (int, float)):
        schedule = timedelta(seconds=schedule)
    if isinstance(schedule, timedelta):
        micros = schedule // timedelta(microseconds=1)
        if micros <= 0:
            raise CronError(f'{name}: the interval must be positive')
        return dict(empty, kind='interval', every_microseconds=micros)
    raise CronError(
        f'{name}: unsupported schedule {schedule!r} '
        f'(use celery.schedules.crontab, a timedelta or seconds)')


def _cronfield(value):
    if isinstance(value, (set, frozenset, list, tuple)):
        return ','.join(str(v) for v in sorted(value))
    return str(value)


def autodiscover():
    """Import ``cron`` from every installed app (like ``admin.autodiscover``)."""
    from django.utils.module_loading import autodiscover_modules
    autodiscover_modules('cron')


def register_tasks():
    """Register a Celery task for every job that defines ``run()``.

    Called from ``AppConfig.ready`` so that workers (which run
    ``django.setup()``) know the tasks.
    """
    from celery import shared_task

    for name, cls in list(registry.items()):
        if cls.task or not cls.has_run() or '_gobeat_task' in cls.__dict__:
            continue

        def runner(*args, _cls=cls, **kwargs):
            return _cls().run(*args, **kwargs)
        runner.__name__ = cls.__name__
        runner.__qualname__ = cls.__qualname__
        runner.__module__ = cls.__module__
        runner.__doc__ = cls.__doc__
        cls._gobeat_task = shared_task(name=cls.get_task_name())(runner)


def sync_jobs(extra=None):
    """Write the registry to ``Job`` rows (source="code").

    Rows of jobs that no longer exist in code are deleted; run bookkeeping
    (``last_run_at``, ``total_run_count``) and the admin's ``paused`` flag
    are kept. ``extra`` maps names to field dicts from other code sources
    (``app.conf.beat_schedule`` when django-celery-beat is not installed).
    Returns (created, updated, deleted, errors).
    """
    from .models import Job

    wanted, errors = {}, []
    for name, cls in sorted(registry.items()):
        try:
            wanted[name] = cls().as_fields()
        except CronError as exc:
            errors.append(str(exc))
    for name, fields in (extra or {}).items():
        wanted.setdefault(name, fields)

    created = updated = 0
    existing = {j.name: j for j in Job.objects.filter(source=Job.SOURCE_CODE)}
    for name, fields in wanted.items():
        job = existing.pop(name, None)
        if job is None:
            clash = Job.objects.filter(name=name).first()
            if clash is not None:
                errors.append(
                    f'{name}: a job with this name was created in the admin; '
                    f'the code definition is ignored')
                continue
            Job.objects.create(name=name, source=Job.SOURCE_CODE, **fields)
            created += 1
            continue
        changed = [k for k, v in fields.items() if getattr(job, k) != v]
        if changed:
            for k in changed:
                setattr(job, k, fields[k])
            job.save()
            updated += 1
    deleted = len(existing)
    for job in existing.values():
        job.delete()
    return created, updated, deleted, errors
