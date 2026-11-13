import os

from kombu import Exchange, Queue

SECRET_KEY = 'integration-tests-only'
DEBUG = False
ALLOWED_HOSTS = ['*']
INSTALLED_APPS = [
    'django.contrib.contenttypes',
    'django.contrib.auth',
    'django_celery_beat',
    'django_gobeat',
    'runlog',
]
DATABASES = {'default': {
    'ENGINE': 'django.db.backends.postgresql',
    'HOST': os.environ.get('PGHOST', 'postgres'),
    'PORT': os.environ.get('PGPORT', '5432'),
    'NAME': os.environ.get('PGDATABASE', 'app'),
    'USER': os.environ.get('PGUSER', 'app'),
    'PASSWORD': os.environ.get('PGPASSWORD', 'app'),
}}
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
USE_TZ = os.environ.get('USE_TZ', '1') == '1'
TIME_ZONE = os.environ.get('TIME_ZONE', 'Europe/Bucharest')
DJANGO_CELERY_BEAT_TZ_AWARE = USE_TZ

CELERY_BROKER_URL = os.environ['BROKER_URL']
CELERY_TIMEZONE = TIME_ZONE
CELERY_BEAT_SCHEDULER = 'django_celery_beat.schedulers:DatabaseScheduler'
CELERY_RESULT_BACKEND = None
CELERY_TASK_IGNORE_RESULT = True
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True
CELERY_TASK_QUEUES = [
    Queue('celery', Exchange('celery'), routing_key='celery'),
    Queue('high', Exchange('high'), routing_key='high',
          queue_arguments={'x-max-priority': 10}),
    Queue('topic_q', Exchange('events', type='topic'), routing_key='ev.#'),
]
CELERY_TASK_ROUTES = {
    'runlog.tasks.routed': {'queue': 'high'},
    'runlog.tasks.topic': {'queue': 'topic_q', 'routing_key': 'ev.created'},
}
CELERY_WORKER_PREFETCH_MULTIPLIER = 1
