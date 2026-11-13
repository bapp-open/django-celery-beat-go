from django.db import migrations, models

NOTIFY_SQL = """
CREATE OR REPLACE FUNCTION django_gobeat_notify() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('gobeat', TG_ARGV[0]);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS django_gobeat_schedule_changed ON django_celery_beat_periodictasks;
CREATE TRIGGER django_gobeat_schedule_changed
    AFTER INSERT OR UPDATE OR DELETE ON django_celery_beat_periodictasks
    FOR EACH STATEMENT EXECUTE FUNCTION django_gobeat_notify('schedule');

DROP TRIGGER IF EXISTS django_gobeat_settings_changed ON django_gobeat_settings;
CREATE TRIGGER django_gobeat_settings_changed
    AFTER INSERT OR UPDATE OR DELETE ON django_gobeat_settings
    FOR EACH STATEMENT EXECUTE FUNCTION django_gobeat_notify('settings');
"""

NOTIFY_SQL_REVERSE = """
DROP TRIGGER IF EXISTS django_gobeat_schedule_changed ON django_celery_beat_periodictasks;
DROP TRIGGER IF EXISTS django_gobeat_settings_changed ON django_gobeat_settings;
DROP FUNCTION IF EXISTS django_gobeat_notify();
"""


def install_triggers(apps, schema_editor):
    # LISTEN/NOTIFY is a Postgres feature; gobeat falls back to polling
    # the change marker when the triggers are absent.
    if schema_editor.connection.vendor == 'postgresql':
        schema_editor.execute(NOTIFY_SQL)


def remove_triggers(apps, schema_editor):
    if schema_editor.connection.vendor == 'postgresql':
        schema_editor.execute(NOTIFY_SQL_REVERSE)


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        ('django_celery_beat', '0001_initial'),
    ]

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
                ('periodic_task_id', models.BigIntegerField(db_index=True)),
                ('name', models.CharField(max_length=200)),
                ('task', models.CharField(max_length=200)),
                ('task_id', models.CharField(max_length=64, unique=True)),
                ('scheduled_for', models.DateTimeField()),
                ('claimed_at', models.DateTimeField()),
                ('published_at', models.DateTimeField(blank=True, db_index=True, null=True)),
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
        migrations.RunPython(install_triggers, remove_triggers),
    ]
