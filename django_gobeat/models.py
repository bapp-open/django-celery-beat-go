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
    #: PeriodicTask primary key (no FK: the journal must survive deletes).
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
