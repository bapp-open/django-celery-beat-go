from celery import current_app
from django.test import override_settings

from django_gobeat.resolve import build_settings


def test_db_time_zone_is_unset_with_use_tz():
    assert build_settings(current_app)['db_time_zone'] is None


@override_settings(USE_TZ=False, TIME_ZONE='Europe/Bucharest')
def test_db_time_zone_is_the_connection_zone_without_use_tz():
    # On PostgreSQL Django keeps `timestamp with time zone` columns with
    # USE_TZ=False and exchanges naive values in this zone; gobeat's session
    # must use the same one.
    assert build_settings(current_app)['db_time_zone'] == 'Europe/Bucharest'
