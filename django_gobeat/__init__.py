"""Companion Django app for gobeat, the Go replacement for ``celery beat``.

It owns two tables (the settings row gobeat reads at start-up and the
dispatch journal gobeat writes) and the ``gobeat_sync`` management command.
"""
__version__ = '0.1.0'

#: Version of the JSON document stored in ``GobeatSettings.data``.
SETTINGS_SCHEMA = 1
#: Postgres NOTIFY channel gobeat LISTENs on.
NOTIFY_CHANNEL = 'gobeat'


def __getattr__(name):
    # Lazy so that importing django_gobeat never imports Celery before the
    # project is ready for it.
    if name in ('CronInterface', 'CronError', 'crontab'):
        from . import cron
        return getattr(cron, name)
    raise AttributeError(name)
