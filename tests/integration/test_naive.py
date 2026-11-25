"""USE_TZ = False on PostgreSQL (run with USE_TZ=0, TIME_ZONE=Europe/Bucharest).

Django keeps `timestamp with time zone` columns and exchanges naive values in
TIME_ZONE; Python beat stores and reads last_run_at as naive UTC. gobeat must
read the value Python beat wrote, claim the row (compare-and-swap on that same
value) and write back a value Django reads as naive UTC.
"""
import datetime
import os

import pytest

from conftest import ctl, runs, unique, wait_for

pytestmark = pytest.mark.skipif(os.environ.get('USE_TZ', '1') != '0',
                                reason='USE_TZ=0 stack only')


def naive(iso):
    return datetime.datetime.fromisoformat(iso)


def test_naive_last_run_at_round_trips_through_django():
    name = unique('naive')
    # last run 60 s ago in Python beat's convention: due immediately, and
    # read two hours off (in the future) by a session that takes the stored
    # instant as UTC
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [2, 'seconds'], 'last_run_ago': 60})
    got = wait_for(lambda: len(runs(name)) >= 3 and runs(name), timeout=20,
                   what='3 runs')
    gaps = [b['ts'] - a['ts'] for a, b in zip(got, got[1:])]
    assert all(1.0 < g < 3.5 for g in gaps), gaps

    row = ctl({'op': 'get', 'name': name})[0]
    assert row['total_run_count'] >= 3
    last, now = naive(row['last_run_at']), naive(row['beat_now'])
    assert last.tzinfo is None and now.tzinfo is None
    # gobeat's last claim, as Django reads it, is naive UTC "now"
    assert datetime.timedelta(0) <= now - last < datetime.timedelta(seconds=5), (last, now)
