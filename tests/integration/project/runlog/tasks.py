import json

from celery import shared_task


def _record(self, args, kwargs):
    from .models import Run
    req = self.request
    info = req.delivery_info or {}
    expires = req.expires
    Run.objects.create(
        task=self.name, task_id=req.id,
        periodic_task_name=getattr(req, 'periodic_task_name', None),
        args=json.dumps(list(args)), kwargs=json.dumps(kwargs, sort_keys=True),
        queue=info.get('routing_key'), exchange=info.get('exchange'),
        priority=info.get('priority'),
        expires=None if expires is None else str(expires),
        headers=json.dumps({'x-custom': getattr(req, 'x-custom', None)}),
    )


@shared_task(bind=True, name='runlog.tasks.record')
def record(self, *args, **kwargs):
    _record(self, args, kwargs)


@shared_task(bind=True, name='runlog.tasks.routed')
def routed(self, *args, **kwargs):
    _record(self, args, kwargs)


@shared_task(bind=True, name='runlog.tasks.topic')
def topic(self, *args, **kwargs):
    _record(self, args, kwargs)


@shared_task(bind=True, name='runlog.tasks.decorated', queue='high',
             priority=7, expires=120)
def decorated(self, *args, **kwargs):
    _record(self, args, kwargs)


@shared_task(bind=True, name='runlog.tasks.pickled', serializer='pickle')
def pickled(self, *args, **kwargs):
    _record(self, args, kwargs)
