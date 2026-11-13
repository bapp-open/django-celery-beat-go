from django.db import migrations, models

from django.apps import apps as global_apps

from django_gobeat import triggers


def install_triggers(apps, schema_editor):
    # LISTEN/NOTIFY is a Postgres feature; gobeat falls back to polling
    # when the triggers are absent.
    triggers.install(schema_editor.connection)


def remove_triggers(apps, schema_editor):
    triggers.remove(schema_editor.connection)


class Migration(migrations.Migration):
    initial = True

    # django-celery-beat is optional; when it is installed its tables
    # must exist first so that the change trigger can be attached.
    dependencies = (
        [('django_celery_beat', '0001_initial')]
        if global_apps.is_installed('django_celery_beat') else []
    )

    operations = [
        migrations.CreateModel(
            name='GobeatSettings',
            fields=[
                ('key', models.CharField(default='default', max_length=64, primary_key=True, serialize=False)),
                ('version', models.CharField(max_length=64)),
                ('data', models.JSONField()),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'verbose_name': 'gobeat settings',
                'verbose_name_plural': 'gobeat settings',
                'db_table': 'django_gobeat_settings',
            },
        ),
        migrations.CreateModel(
            name='Dispatch',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('source', models.CharField(default='dcb', max_length=8)),
                ('periodic_task_id', models.BigIntegerField(db_index=True)),
                ('name', models.CharField(max_length=200)),
                ('task', models.CharField(max_length=200)),
                ('task_id', models.CharField(max_length=64, unique=True)),
                ('scheduled_for', models.DateTimeField()),
                ('claimed_at', models.DateTimeField()),
                ('published_at', models.DateTimeField(blank=True, db_index=True, null=True)),
                ('abandoned_at', models.DateTimeField(blank=True, null=True)),
                ('attempts', models.IntegerField(default=0)),
                ('last_error', models.TextField(blank=True, default='')),
                ('message', models.JSONField()),
            ],
            options={
                'verbose_name': 'gobeat dispatch',
                'verbose_name_plural': 'gobeat dispatches',
                'db_table': 'django_gobeat_dispatch',
                'ordering': ['-id'],
            },
        ),
        migrations.CreateModel(
            name='Job',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=200, unique=True)),
                ('source', models.CharField(default='admin', editable=False, max_length=16)),
                ('task', models.CharField(help_text='Celery task name', max_length=200)),
                ('kind', models.CharField(choices=[('crontab', 'crontab'), ('interval', 'interval')], max_length=16)),
                ('minute', models.CharField(blank=True, default='', max_length=240)),
                ('hour', models.CharField(blank=True, default='', max_length=96)),
                ('day_of_week', models.CharField(blank=True, default='', max_length=64)),
                ('day_of_month', models.CharField(blank=True, default='', max_length=124)),
                ('month_of_year', models.CharField(blank=True, default='', max_length=64)),
                ('timezone', models.CharField(blank=True, default='', help_text='IANA zone of the crontab; empty = the Celery timezone', max_length=64)),
                ('every_microseconds', models.BigIntegerField(blank=True, null=True)),
                ('args', models.TextField(blank=True, default='[]')),
                ('kwargs', models.TextField(blank=True, default='{}')),
                ('headers', models.TextField(blank=True, default='{}')),
                ('queue', models.CharField(blank=True, max_length=200, null=True)),
                ('exchange', models.CharField(blank=True, max_length=200, null=True)),
                ('routing_key', models.CharField(blank=True, max_length=200, null=True)),
                ('priority', models.PositiveIntegerField(blank=True, null=True)),
                ('expire_seconds', models.PositiveIntegerField(blank=True, null=True)),
                ('enabled', models.BooleanField(default=True)),
                ('paused', models.BooleanField(default=False)),
                ('one_off', models.BooleanField(default=False)),
                ('start_time', models.DateTimeField(blank=True, null=True)),
                ('expires', models.DateTimeField(blank=True, null=True)),
                ('last_run_at', models.DateTimeField(blank=True, editable=False, null=True)),
                ('total_run_count', models.PositiveIntegerField(default=0, editable=False)),
                ('date_changed', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'django_gobeat_job',
                'ordering': ['name'],
            },
        ),
        migrations.RunPython(install_triggers, remove_triggers),
    ]
