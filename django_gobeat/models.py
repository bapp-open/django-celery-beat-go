from django.db import models


class GobeatSettings(models.Model):
    """One row (``key='default'``) with everything gobeat needs to know
    about the Celery app. Written by ``manage.py gobeat_sync``."""

    key = models.CharField(max_length=64, primary_key=True, default='default')
    #: sha256 of the canonical JSON; gobeat reloads when it changes.
    version = models.CharField(max_length=64)
    data = models.JSONField()
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'django_gobeat_settings'
        verbose_name = 'gobeat settings'
        verbose_name_plural = 'gobeat settings'

    def __str__(self):
        return f'{self.key} ({self.version[:12]})'


class Dispatch(models.Model):
    """Dispatch journal, written by gobeat.

    A row is inserted in the same transaction that claims a run
    (compare-and-swap on ``PeriodicTask.last_run_at``) and holds the fully
    rendered message. ``published_at`` is set once the broker accepted it.
    Rows with ``published_at IS NULL`` are runs that were claimed but not
    yet delivered; gobeat publishes them before doing anything else.
    """

    id = models.BigAutoField(primary_key=True)
    #: Which table the run belongs to: "dcb" (django-celery-beat's
    #: PeriodicTask) or "job" (django_gobeat.Job).
    source = models.CharField(max_length=8, default='dcb')
    #: Primary key in that table (no FK: the journal must survive deletes).
    periodic_task_id = models.BigIntegerField(db_index=True)
    name = models.CharField(max_length=200)
    task = models.CharField(max_length=200)
    #: Deterministic Celery task id (UUIDv5 of task pk + scheduled time).
    task_id = models.CharField(max_length=64, unique=True)
    #: The instant the run became due.
    scheduled_for = models.DateTimeField()
    claimed_at = models.DateTimeField()
    published_at = models.DateTimeField(null=True, blank=True, db_index=True)
    #: Set when gobeat gave up (GOBEAT_PUBLISH_MAX_AGE exceeded or the
    #: message expired); never silently: it is logged and counted.
    abandoned_at = models.DateTimeField(null=True, blank=True)
    attempts = models.IntegerField(default=0)
    last_error = models.TextField(blank=True, default='')
    #: Rendered message (destination, headers, properties, body).
    message = models.JSONField()

    class Meta:
        db_table = 'django_gobeat_dispatch'
        verbose_name = 'gobeat dispatch'
        verbose_name_plural = 'gobeat dispatches'
        ordering = ['-id']

    def __str__(self):
        return f'{self.name} @ {self.scheduled_for:%Y-%m-%d %H:%M:%S}'


class Job(models.Model):
    """A periodic job owned by django_gobeat (no django-celery-beat needed).

    Rows with ``source="code"`` mirror the ``CronInterface`` classes found
    in the apps' ``cron.py`` modules and are rewritten by ``gobeat_sync``;
    rows with ``source="admin"`` are created by hand. gobeat schedules
    both, next to django-celery-beat's ``PeriodicTask`` rows when that app
    is installed.
    """

    SOURCE_CODE = 'code'
    SOURCE_ADMIN = 'admin'
    KIND_CRONTAB = 'crontab'
    KIND_INTERVAL = 'interval'

    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=200, unique=True)
    source = models.CharField(max_length=16, default=SOURCE_ADMIN, editable=False)
    task = models.CharField(max_length=200, help_text='Celery task name')

    kind = models.CharField(max_length=16, choices=[
        (KIND_CRONTAB, 'crontab'), (KIND_INTERVAL, 'interval')])
    minute = models.CharField(max_length=240, blank=True, default='')
    hour = models.CharField(max_length=96, blank=True, default='')
    day_of_week = models.CharField(max_length=64, blank=True, default='')
    day_of_month = models.CharField(max_length=124, blank=True, default='')
    month_of_year = models.CharField(max_length=64, blank=True, default='')
    timezone = models.CharField(
        max_length=64, blank=True, default='',
        help_text='IANA zone of the crontab; empty = the Celery timezone')
    every_microseconds = models.BigIntegerField(null=True, blank=True)

    args = models.TextField(blank=True, default='[]')
    kwargs = models.TextField(blank=True, default='{}')
    headers = models.TextField(blank=True, default='{}')
    queue = models.CharField(max_length=200, null=True, blank=True)
    exchange = models.CharField(max_length=200, null=True, blank=True)
    routing_key = models.CharField(max_length=200, null=True, blank=True)
    priority = models.PositiveIntegerField(null=True, blank=True)
    expire_seconds = models.PositiveIntegerField(null=True, blank=True)

    enabled = models.BooleanField(default=True)
    #: Admin switch that survives gobeat_sync (which owns ``enabled`` of
    #: code-defined jobs).
    paused = models.BooleanField(default=False)
    one_off = models.BooleanField(default=False)
    start_time = models.DateTimeField(null=True, blank=True)
    expires = models.DateTimeField(null=True, blank=True)

    last_run_at = models.DateTimeField(null=True, blank=True, editable=False)
    total_run_count = models.PositiveIntegerField(default=0, editable=False)
    #: When the definition last changed; a never-run job counts from here.
    date_changed = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'django_gobeat_job'
        ordering = ['name']

    def __str__(self):
        return self.name
