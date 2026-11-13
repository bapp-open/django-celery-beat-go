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
