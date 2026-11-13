from django.apps import AppConfig


class GobeatConfig(AppConfig):
    name = 'django_gobeat'
    verbose_name = 'gobeat'
    default_auto_field = 'django.db.models.BigAutoField'

    def ready(self):
        # Discover <app>/cron.py in every installed app, the way Django
        # discovers admin.py, and make the jobs' tasks known to Celery.
        from . import cron
        cron.autodiscover()
        cron.register_tasks()

        # `manage.py migrate` is all a deploy needs: the sync runs when it
        # finishes (post_migrate fires even if there was nothing to
        # migrate). GOBEAT_AUTO_SYNC = False turns this off.
        from django.db.models.signals import post_migrate
        post_migrate.connect(sync_after_migrate, sender=self,
                             dispatch_uid='django_gobeat.sync_after_migrate')


def sync_after_migrate(sender, using='default', verbosity=1, **kwargs):
    """post_migrate receiver: do what ``manage.py gobeat_sync`` does.

    A failure never breaks ``migrate``; it is reported and the previous
    settings row (if any) stays in place.
    """
    import sys

    from django.conf import settings
    from django.core.management import call_command
    from django.db import DEFAULT_DB_ALIAS, connections

    if not getattr(settings, 'GOBEAT_AUTO_SYNC', True) or using != DEFAULT_DB_ALIAS:
        return
    tables = connections[using].introspection.table_names()
    if 'django_gobeat_settings' not in tables or 'django_gobeat_job' not in tables:
        return        # e.g. `migrate django_gobeat zero`
    out = sys.stdout if verbosity > 0 else open(__import__('os').devnull, 'w')
    try:
        call_command('gobeat_sync', stdout=out)
    except Exception as exc:  # noqa: BLE001 - never fail the migration
        sys.stderr.write(
            'gobeat: automatic sync after migrate failed (%s: %s); fix the '
            'problem and run `manage.py gobeat_sync`\n'
            % (type(exc).__name__, exc))
    finally:
        if out is not sys.stdout:
            out.close()
