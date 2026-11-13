import os
import sys

import django
from django.conf import settings

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.dirname(__file__))


def pytest_configure():
    settings.configure(
        USE_TZ=True, TIME_ZONE='UTC', SECRET_KEY='x',
        INSTALLED_APPS=['django.contrib.contenttypes', 'django_gobeat', 'sampleapp'],
        DATABASES={'default': {'ENGINE': 'django.db.backends.sqlite3', 'NAME': ':memory:'}},
        DEFAULT_AUTO_FIELD='django.db.models.BigAutoField',
    )
    django.setup()
    from django.core.management import call_command
    call_command('migrate', verbosity=0)


import pytest  # noqa: E402


@pytest.fixture
def db_ready():
    from django_gobeat.models import Job
    Job.objects.all().delete()
    yield
    Job.objects.all().delete()
