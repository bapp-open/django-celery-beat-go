import os

from celery import Celery
from celery.schedules import crontab

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proj.settings')

app = Celery('proj')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

# A code-defined entry: Python beat writes it to the database on start-up,
# gobeat_sync must do the same.
app.conf.beat_schedule = {
    'code-defined-every-3s': {
        'task': 'runlog.tasks.record',
        'schedule': 3.0,
        'args': ('from-code',),
        'options': {'queue': 'high'},
    },
    'code-defined-crontab': {
        'task': 'runlog.tasks.record',
        'schedule': crontab(minute='*'),
        'kwargs': {'source': 'crontab-from-code'},
    },
}
