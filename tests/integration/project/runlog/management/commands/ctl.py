"""Test driver: do through the ORM what a person does in the Django admin.

    manage.py ctl '<json>'

The JSON is a list of operations; the result is printed as JSON.
"""
import json
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('ops')

    def handle(self, *args, **opts):
        from django_celery_beat.models import (ClockedSchedule,
                                               CrontabSchedule,
                                               IntervalSchedule, PeriodicTask,
                                               SolarSchedule)
        out = []
        for op in json.loads(opts['ops']):
            kind = op.pop('op')
            if kind == 'create':
                fields = dict(op)
                if 'interval' in fields:
                    every, period = fields.pop('interval')
                    fields['interval'], _ = IntervalSchedule.objects.get_or_create(
                        every=every, period=period)
                if 'crontab' in fields:
                    fields['crontab'], _ = CrontabSchedule.objects.get_or_create(
                        **fields.pop('crontab'))
                if 'clocked_in' in fields:
                    fields['clocked'] = ClockedSchedule.objects.create(
                        clocked_time=timezone.now() + timedelta(
                            seconds=fields.pop('clocked_in')))
                if 'solar' in fields:
                    fields['solar'], _ = SolarSchedule.objects.get_or_create(
                        **fields.pop('solar'))
                for key in ('start_in', 'expires_in'):
                    if key in fields:
                        target = 'start_time' if key == 'start_in' else 'expires'
                        fields[target] = timezone.now() + timedelta(
                            seconds=fields.pop(key))
                for key in ('args', 'kwargs', 'headers'):
                    if key in fields and not isinstance(fields[key], str):
                        fields[key] = json.dumps(fields[key])
                out.append(PeriodicTask.objects.create(**fields).pk)
            elif kind == 'update':
                pt = PeriodicTask.objects.get(name=op.pop('name'))
                for k, v in op.items():
                    if k in ('args', 'kwargs', 'headers') and not isinstance(v, str):
                        v = json.dumps(v)
                    setattr(pt, k, v)
                pt.save()
                out.append(pt.pk)
            elif kind == 'delete':
                PeriodicTask.objects.get(name=op['name']).delete()
                out.append(None)
            elif kind == 'reset':
                # leave only what the test creates
                PeriodicTask.objects.all().delete()
                from django_celery_beat.models import PeriodicTasks
                from django_gobeat.models import Dispatch
                from runlog.models import Run
                Run.objects.all().delete()
                Dispatch.objects.all().delete()
                PeriodicTasks.update_changed()
                out.append(None)
            else:
                raise SystemExit('unknown op %r' % kind)
        self.stdout.write(json.dumps(out))
