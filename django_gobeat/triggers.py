"""Postgres NOTIFY triggers that wake gobeat when a schedule changes.

Installed by the migration and re-checked by every ``gobeat_sync`` (so
that django-celery-beat's table is covered even if that app is added
later). gobeat polls as a fallback, so the triggers are an optimisation.
"""

JOB_COLUMNS = (
    'name', 'task', 'kind', 'minute', 'hour', 'day_of_week', 'day_of_month',
    'month_of_year', 'timezone', 'every_microseconds', 'args', 'kwargs',
    'headers', 'queue', 'exchange', 'routing_key', 'priority',
    'expire_seconds', 'enabled', 'paused', 'one_off', 'start_time', 'expires',
    'date_changed',
)

FUNCTION = """
CREATE OR REPLACE FUNCTION django_gobeat_notify() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('gobeat', TG_ARGV[0]);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""

# (trigger name, table, events, payload)
TRIGGERS = (
    ('django_gobeat_settings_changed', 'django_gobeat_settings',
     'INSERT OR UPDATE OR DELETE', 'settings'),
    # not on last_run_at/total_run_count: gobeat's own claims must not
    # wake gobeat
    ('django_gobeat_job_changed', 'django_gobeat_job',
     'INSERT OR DELETE OR UPDATE OF ' + ', '.join(JOB_COLUMNS), 'schedule'),
    ('django_gobeat_schedule_changed', 'django_celery_beat_periodictasks',
     'INSERT OR UPDATE OR DELETE', 'schedule'),
)


def install(connection):
    """Create the triggers on every table that exists. Idempotent."""
    if connection.vendor != 'postgresql':
        return []
    installed = []
    tables = set(connection.introspection.table_names())
    with connection.cursor() as cursor:
        cursor.execute(FUNCTION)
        for name, table, events, payload in TRIGGERS:
            if table not in tables:
                continue
            cursor.execute(f'DROP TRIGGER IF EXISTS {name} ON {table}')
            cursor.execute(
                f'CREATE TRIGGER {name} AFTER {events} ON {table} '
                f"FOR EACH STATEMENT EXECUTE FUNCTION django_gobeat_notify('{payload}')")
            installed.append(table)
    return installed


def remove(connection):
    if connection.vendor != 'postgresql':
        return
    tables = set(connection.introspection.table_names())
    with connection.cursor() as cursor:
        for name, table, _, _ in TRIGGERS:
            if table in tables:
                cursor.execute(f'DROP TRIGGER IF EXISTS {name} ON {table}')
        cursor.execute('DROP FUNCTION IF EXISTS django_gobeat_notify()')
