"""Code-defined periodic jobs, discovered by django_gobeat (like models.py)."""
import json
from datetime import timedelta

from celery import current_task

from django_gobeat import CronInterface, crontab


class RecordingCron(CronInterface):
    """Project base class: everything below records its executions."""
    abstract = True
    queue = 'high'

    def run(self, *args, **kwargs):
        from .models import Run
        req = current_task.request
        info = req.delivery_info or {}
        Run.objects.create(
            task=current_task.name, task_id=req.id,
            periodic_task_name=getattr(req, 'periodic_task_name', None),
            args=json.dumps(list(args)), kwargs=json.dumps(kwargs, sort_keys=True),
            queue=info.get('routing_key'), exchange=info.get('exchange'),
            priority=info.get('priority'),
            expires=None if req.expires is None else str(req.expires))


class EveryTwoSeconds(RecordingCron):
    schedule = timedelta(seconds=2)
    args = ('from-cron-py',)
    priority = 4
    expires = 45


class EveryMinuteTokyo(RecordingCron):
    schedule = crontab(minute='*')
    timezone = 'Asia/Tokyo'

    def get_kwargs(self):
        return {'zone': self.timezone}


class ExistingTask(CronInterface):
    """Schedules a task that already exists instead of defining run()."""
    name = 'existing-task-job'
    schedule = 3
    task = 'runlog.tasks.record'
    kwargs = {'via': 'cron.py'}
