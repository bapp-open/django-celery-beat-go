import json

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction


class Command(BaseCommand):
    help = ('Write the gobeat settings row and sync cron.py jobs and '
            'code-defined beat_schedule entries. Runs automatically after '
            '`migrate` (post_migrate) unless GOBEAT_AUTO_SYNC = False.')

    def add_arguments(self, parser):
        parser.add_argument(
            '--app', default=None,
            help='Celery app as "module:attr" (default: settings.'
                 'GOBEAT_CELERY_APP, else celery.current_app).')
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Print the settings document; write nothing.')
        parser.add_argument(
            '--no-beat-schedule', action='store_true',
            help='Do not sync app.conf.beat_schedule into PeriodicTask.')
        parser.add_argument(
            '--no-broker-url', action='store_true',
            help='Leave broker_url out of the row (set GOBEAT_BROKER_URL '
                 'on the gobeat container instead).')
        parser.add_argument(
            '--strict', action='store_true',
            help='Fail if anything could not be resolved exactly.')

    def handle(self, *args, **opts):
        from django.conf import settings

        from django_gobeat.models import GobeatSettings
        from django_gobeat.resolve import (build_settings, canonical_json,
                                           version_hash)

        app = self._load_app(opts['app'] or getattr(
            settings, 'GOBEAT_CELERY_APP', None))
        app.loader.import_default_modules()
        app.finalize(auto=True)

        from django.apps import apps
        from django.db import connection

        from django_gobeat import cron, triggers
        from django_gobeat.models import Job

        has_dcb = apps.is_installed('django_celery_beat')
        referenced = set()
        if not opts['dry_run']:
            extra = {}
            if not opts['no_beat_schedule']:
                if has_dcb:
                    self._sync_beat_schedule(app)
                else:
                    extra = self._beat_schedule_as_jobs(app)
            created, updated, deleted, errors = cron.sync_jobs(extra)
            for error in errors:
                self.stderr.write(self.style.ERROR('gobeat_sync: ' + error))
            if errors and opts['strict']:
                raise CommandError('invalid cron job definitions')
            self.stdout.write(
                'cron jobs: %d discovered, %d created, %d updated, %d removed'
                % (len(cron.registry), created, updated, deleted))
            triggers.install(connection)
        referenced.update(Job.objects.values_list('task', flat=True))
        if has_dcb:
            from django_celery_beat.models import PeriodicTask
            referenced.update(
                PeriodicTask.objects.values_list('task', flat=True))
        referenced = sorted(referenced)
        data = build_settings(app, extra_task_names=referenced,
                              include_broker_url=not (
                                  opts['no_broker_url'] or not getattr(
                                      settings, 'GOBEAT_STORE_BROKER_URL', True)))
        version = version_hash(data)

        for warning in data['warnings']:
            self.stderr.write(self.style.WARNING('gobeat_sync: ' + warning))
        if opts['strict'] and data['warnings']:
            raise CommandError('unresolved items (see warnings above)')

        if opts['dry_run']:
            self.stdout.write(json.dumps(data, indent=2, sort_keys=True))
            return

        with transaction.atomic():
            row, created = GobeatSettings.objects.select_for_update() \
                .get_or_create(key='default',
                               defaults={'version': version, 'data': data})
            changed = created or row.version != version
            if changed and not created:
                row.version = version
                row.data = json.loads(canonical_json(data))
                row.save()
        self.stdout.write(self.style.SUCCESS(
            'gobeat settings %s (version %s, %d tasks, %d queues)' % (
                'written' if changed else 'unchanged', version[:12],
                len(data['tasks']), len(data['queues']))))

    def _load_app(self, path):
        if path:
            from kombu.utils.imports import symbol_by_name
            return symbol_by_name(path)
        from celery import current_app
        app = current_app._get_current_object()
        if app.main in (None, 'default') and not app.configured:
            raise CommandError(
                'No Celery app found. Import your app in the project '
                '__init__ or set GOBEAT_CELERY_APP = "proj.celery:app".')
        return app

    def _beat_schedule_as_jobs(self, app):
        """Without django-celery-beat, ``app.conf.beat_schedule`` entries
        become gobeat jobs."""
        from django_gobeat import cron

        out = {}
        for name, entry in (app.conf.beat_schedule or {}).items():
            options = dict(entry.get('options') or {})
            try:
                fields = cron.schedule_fields(entry['schedule'], name)
                fields.update(
                    task=entry['task'],
                    args=json.dumps(list(entry.get('args') or ())),
                    kwargs=json.dumps(dict(entry.get('kwargs') or {})),
                    headers=json.dumps(dict(options.get('headers') or {})),
                    queue=options.get('queue'),
                    exchange=options.get('exchange'),
                    routing_key=options.get('routing_key'),
                    priority=options.get('priority'),
                    expire_seconds=cron._seconds(
                        options.get('expire_seconds', options.get('expires')),
                        name),
                    enabled=True, one_off=False)
            except (cron.CronError, TypeError, ValueError, KeyError) as exc:
                self.stderr.write(self.style.ERROR(
                    'gobeat_sync: beat_schedule entry %r skipped: %s' % (name, exc)))
                continue
            out[name] = fields
        self.stdout.write('beat_schedule: %d code-defined entries synced '
                          'to gobeat jobs' % len(out))
        return out

    def _sync_beat_schedule(self, app):
        """Exactly what Python beat does on start-up:
        django_celery_beat/schedulers.py: DatabaseScheduler.setup_schedule
        (install_default_entries + update_from_dict(app.conf.beat_schedule)).
        """
        from django_celery_beat.schedulers import DatabaseScheduler

        scheduler = DatabaseScheduler(app=app, lazy=True)
        scheduler.setup_schedule()
        self.stdout.write('beat_schedule: %d code-defined entries synced' % (
            len(app.conf.beat_schedule or {})))
