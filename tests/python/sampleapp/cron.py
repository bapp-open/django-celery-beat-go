from datetime import timedelta

from django_gobeat import CronInterface, crontab

CALLS = []


class TenantCron(CronInterface):
    """A project-specific base class; not a job itself."""
    abstract = True
    queue = 'tenants'
    tenant = 'acme'

    def get_kwargs(self):
        return {'tenant': self.tenant}


class Nightly(TenantCron):
    schedule = crontab(minute=0, hour='4', day_of_week='mon-fri')
    timezone = 'Europe/Bucharest'

    def run(self, **kwargs):
        CALLS.append(kwargs)
        return 'ran'


class Ping(CronInterface):
    name = 'ping'
    schedule = timedelta(seconds=30)
    task = 'sampleapp.tasks.ping'
    expires = 20
    priority = 3
